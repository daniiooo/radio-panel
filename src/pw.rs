//! Native PipeWire layer: one thread owns the connection, the `radio` sink and the
//! in-process loopback. Commands go in over a pipewire channel, events come out
//! through a callback that runs on that thread. See docs/pipewire-notes.md.
#![allow(dead_code)] // until the UI uses it

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ffi::CString;
use std::io::Cursor;
use std::rc::Rc;
use std::thread::JoinHandle;

use pipewire as pw;
use pw::spa;
use spa::param::ParamType;
use spa::pod::serialize::PodSerializer;
use spa::pod::{Object, Pod, Property, Value, ValueArray, deserialize::PodDeserializer};

pub enum Command {
    /// Volume of the sink, pactl percent. Its monitor feeds ffmpeg, so listeners hear this.
    StreamVolume(u32),
    /// Volume of the loopback playback stream, pactl percent. Ignored while the monitor is off.
    LocalVolume(u32),
    /// Local monitoring on or off (loads or unloads the loopback).
    Monitor(bool),
    /// Output device (sink node name) for local monitoring. WirePlumber remembers it.
    Output(String),
    /// Tests only: fallback links go to this sink, never to the real default device.
    #[cfg(test)]
    FallbackSink(String),
    Quit,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// The sink exists; players can be routed to it.
    Ready,
    StreamVolume(u32),
    LocalVolume(u32),
    /// Sinks the monitor can play to, as (node name, description), and the one it plays to now.
    Outputs { sinks: Vec<(String, String)>, current: Option<String> },
    /// Sink monitor audio for the spectrum: mono, `CAPTURE_RATE`, -1.0..=1.0.
    Samples(Vec<f32>),
    /// The connection could not be set up or was lost. The thread has ended.
    Failed(String),
}

pub struct Handle {
    tx: pw::channel::Sender<Command>,
    thread: Option<JoinHandle<()>>,
}

impl Handle {
    pub fn send(&self, command: Command) {
        let _ = self.tx.send(command);
    }
}

impl Drop for Handle {
    /// Ends the thread; the server then removes the sink and the loopback. Players linked
    /// into the sink are first linked to the default sink, so they stay audible.
    fn drop(&mut self) {
        self.send(Command::Quit);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Sample rate of `Event::Samples`.
pub const CAPTURE_RATE: u32 = 44100;

/// Sink node name: `RADIO_PANEL_SINK` for dev runs, else `radio`.
pub fn sink_name() -> String {
    std::env::var("RADIO_PANEL_SINK").ok().filter(|s| !s.is_empty()).unwrap_or_else(|| "radio".into())
}

/// pactl percent is cubic: 79% is linear 0.5.
pub fn percent_to_linear(percent: u32) -> f32 {
    (percent as f32 / 100.0).powi(3)
}

pub fn linear_to_percent(linear: f32) -> u32 {
    (linear.max(0.0).cbrt() * 100.0).round() as u32
}

pub fn spawn(sink: &str, on_event: impl Fn(Event) + Send + 'static) -> Handle {
    assert!(!(cfg!(test) && sink == "radio"), "tests must not create the real sink");
    let (tx, rx) = pw::channel::channel();
    let sink = sink.to_string();
    let thread = std::thread::spawn(move || {
        let on_event = Rc::new(on_event);
        if let Err(error) = run(&sink, rx, on_event.clone()) {
            on_event(Event::Failed(error.to_string()));
        }
    });
    Handle { tx, thread: Some(thread) }
}

fn set_volume(node: &pw::node::Node, percent: u32) {
    let volume = percent_to_linear(percent);
    let object = Value::Object(Object {
        type_: spa::utils::SpaTypes::ObjectParamProps.as_raw(),
        id: ParamType::Props.as_raw(),
        properties: vec![Property::new(
            spa::sys::SPA_PROP_channelVolumes,
            Value::ValueArray(ValueArray::Float(vec![volume, volume])),
        )],
    });
    let bytes = PodSerializer::serialize(Cursor::new(Vec::new()), &object).expect("volume pod").0.into_inner();
    node.set_param(ParamType::Props, 0, Pod::from_bytes(&bytes).expect("volume pod"));
}

/// Loudest channel of a Props param, as pactl percent.
fn volume_percent(pod: &Pod) -> Option<u32> {
    let (_, Value::Object(object)) = PodDeserializer::deserialize_any_from(pod.as_bytes()).ok()? else {
        return None;
    };
    object.properties.into_iter().find_map(|p| match p.value {
        Value::ValueArray(ValueArray::Float(v)) if p.key == spa::sys::SPA_PROP_channelVolumes => {
            v.into_iter().reduce(f32::max).map(linear_to_percent)
        }
        _ => None,
    })
}

fn watch_volume(node: &pw::node::Node, report: impl Fn(u32) + 'static) -> pw::node::NodeListener {
    let listener = node
        .add_listener_local()
        .param(move |_, _, _, _, pod| {
            if let Some(percent) = pod.and_then(volume_percent) {
                report(percent);
            }
        })
        .register();
    node.subscribe_params(&[ParamType::Props]);
    listener
}

/// The loopback module (sink monitor -> ears) and, once the server announces it,
/// its playback node, which carries the local volume.
struct Loopback {
    context: pw::context::ContextRc,
    sink: String,
    /// Target named in the module arguments; without one WirePlumber restores the saved target.
    output: Option<String>,
    module: *mut pw::sys::pw_impl_module,
    node: Option<(u32, pw::node::Node, pw::node::NodeListener)>,
}

impl Loopback {
    fn playback_name(&self) -> String {
        format!("output.{}-local", self.sink)
    }

    fn load(&mut self) {
        if !self.module.is_null() {
            return;
        }
        let target = self.output.as_ref().map(|o| format!(" target.object=\"{o}\"")).unwrap_or_default();
        let args = format!(
            "{{ node.description=\"{sink}-local\" capture.props={{ node.name=\"input.{sink}-local\" target.object=\"{sink}\" stream.capture.sink=true node.passive=true }} playback.props={{ node.name=\"{playback}\"{target} }} }}",
            sink = self.sink,
            playback = self.playback_name(),
        );
        let Ok(args) = CString::new(args) else { return };
        // SAFETY: the context is alive (we hold a ContextRc) and both strings are valid C strings.
        self.module = unsafe {
            pw::sys::pw_context_load_module(
                self.context.as_raw_ptr(),
                c"libpipewire-module-loopback".as_ptr(),
                args.as_ptr(),
                std::ptr::null_mut(),
            )
        };
    }

    fn unload(&mut self) {
        self.node = None;
        if !self.module.is_null() {
            // SAFETY: the pointer came from pw_context_load_module and is destroyed once.
            unsafe { pw::sys::pw_impl_module_destroy(self.module) };
            self.module = std::ptr::null_mut();
        }
    }
}

struct Sink {
    name: String,
    description: String,
    serial: String,
}

struct Port {
    node: u32,
    /// `audio.channel`, such as `FL`; empty when the port has none.
    channel: String,
    input: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Link {
    out_node: u32,
    out_port: u32,
    in_node: u32,
    in_port: u32,
}

const EPIPE: i32 = 32;

/// Link property marking a link we made at shutdown (same name as the Python panel).
const FALLBACK_KEY: &str = "radio.panel.fallback";

/// What the registry has announced, as far as the panel needs it.
#[derive(Default)]
struct Graph {
    /// Audio sinks by node id.
    sinks: HashMap<u32, Sink>,
    ports: HashMap<u32, Port>,
    links: HashMap<u32, Link>,
    /// Fallback links left by an earlier run, by link id.
    leftover: HashMap<u32, Link>,
    default_sink: Option<String>,
    reported: Option<Event>,
}

impl Graph {
    /// Send `Event::Outputs` if the list or the monitor's current sink changed.
    fn report(&mut self, own_sink: &str, loopback_node: Option<u32>, on_event: &dyn Fn(Event)) {
        let mut sinks: Vec<_> = self
            .sinks
            .values()
            // Looping the monitor back into our own sink would feed back.
            .filter(|s| s.name != own_sink)
            .map(|s| (s.name.clone(), s.description.clone()))
            .collect();
        sinks.sort_by(|a, b| a.1.cmp(&b.1));
        let current = loopback_node
            .and_then(|node| self.links.values().find(|l| l.out_node == node))
            .and_then(|l| self.sinks.get(&l.in_node))
            .map(|s| s.name.clone());
        let event = Event::Outputs { sinks, current };
        if self.reported.as_ref() != Some(&event) {
            self.reported = Some(event.clone());
            on_event(event);
        }
    }
}

impl Graph {
    fn sink_id(&self, name: &str) -> Option<u32> {
        self.sinks.iter().find(|(_, s)| s.name == name).map(|(id, _)| *id)
    }

    fn input_port(&self, node: u32, channel: &str) -> Option<u32> {
        self.ports.iter().find(|(_, p)| p.node == node && p.input && p.channel == channel).map(|(id, _)| *id)
    }

    fn linked(&self, out_port: u32, in_port: u32) -> bool {
        self.links.values().any(|l| l.out_port == out_port && l.in_port == in_port)
    }

    /// Links to make at shutdown: every output port playing into `own` gets a link to the
    /// same channel of `target`, unless it has one.
    fn fallback_links(&self, own: &str, target: &str) -> Vec<Link> {
        let (Some(own), Some(target)) = (self.sink_id(own), self.sink_id(target)) else { return Vec::new() };
        if own == target {
            return Vec::new();
        }
        let mut wanted: Vec<Link> = self
            .links
            .values()
            .filter(|l| l.in_node == own)
            .filter_map(|l| {
                let channel = &self.ports.get(&l.in_port)?.channel;
                // A link that crosses channels (a mono player upmixed by hand) is not copied.
                if self.ports.get(&l.out_port).is_some_and(|p| !p.channel.is_empty() && p.channel != *channel) {
                    return None;
                }
                let in_port = self.input_port(target, channel)?;
                (!self.linked(l.out_port, in_port)).then_some(Link { in_node: target, in_port, ..*l })
            })
            .collect();
        wanted.sort();
        wanted.dedup();
        wanted
    }

    /// Leftover fallback links that can be undone now: (link id, link into `own` to make
    /// first, if the player is not linked there yet). Waits until our ports are known.
    fn take_leftovers(&mut self, own: &str) -> Vec<(u32, Option<Link>)> {
        let Some(own) = self.sink_id(own) else { return Vec::new() };
        let ready: Vec<(u32, Option<Link>)> = self
            .leftover
            .iter()
            .filter_map(|(id, l)| {
                let in_port = self.input_port(own, &self.ports.get(&l.in_port)?.channel)?;
                let back = (!self.linked(l.out_port, in_port)).then_some(Link { in_node: own, in_port, ..*l });
                Some((*id, back))
            })
            .collect();
        for (id, _) in &ready {
            self.leftover.remove(id);
        }
        ready
    }
}

fn make_link(core: &pw::core::CoreRc, link: Link, fallback: bool) -> Option<pw::link::Link> {
    let ids = [link.out_node, link.out_port, link.in_node, link.in_port].map(|id| id.to_string());
    let mut props = vec![
        ("link.output.node", ids[0].as_str()),
        ("link.output.port", ids[1].as_str()),
        ("link.input.node", ids[2].as_str()),
        ("link.input.port", ids[3].as_str()),
    ];
    if fallback {
        // Lingering: the link must outlive our connection.
        props.extend([("object.linger", "true"), (FALLBACK_KEY, "true")]);
    }
    let props: pw::properties::PropertiesBox = props.into_iter().collect();
    core.create_object::<pw::link::Link>("link-factory", &props).ok()
}

/// The `name` member of a metadata value such as `{"name":"alsa_output.x"}`.
fn json_name(value: &str) -> Option<String> {
    let rest = &value[value.find("\"name\"")? + 6..];
    let rest = &rest[rest.find(':')? + 1..];
    let rest = &rest[rest.find('"')? + 1..];
    Some(rest[..rest.find('"')?].to_string())
}

/// Capture stream on the sink monitor; every buffer goes out as `Event::Samples`.
fn capture<'c>(
    core: &'c pw::core::CoreRc, sink_name: &str, on_event: Rc<dyn Fn(Event)>,
) -> Result<(pw::stream::StreamBox<'c>, pw::stream::StreamListener<()>), pw::Error> {
    let stream = pw::stream::StreamBox::new(
        core,
        &format!("{sink_name}-meter"),
        pw::properties::properties! {
            *pw::keys::MEDIA_TYPE => "Audio",
            *pw::keys::MEDIA_CATEGORY => "Capture",
            *pw::keys::MEDIA_ROLE => "Music",
            "target.object" => sink_name,
            *pw::keys::STREAM_CAPTURE_SINK => "true",
        },
    )?;
    // No RT_PROCESS: process() runs on the loop thread, like every other callback here.
    let listener = stream
        .add_local_listener_with_user_data(())
        .process(move |stream, _| {
            let Some(mut buffer) = stream.dequeue_buffer() else { return };
            let Some(data) = buffer.datas_mut().first_mut() else { return };
            let size = data.chunk().size() as usize;
            if let Some(bytes) = data.data() {
                let samples: Vec<f32> =
                    bytes[..size.min(bytes.len())].as_chunks::<4>().0.iter().map(|b| f32::from_le_bytes(*b)).collect();
                if !samples.is_empty() {
                    on_event(Event::Samples(samples));
                }
            }
        })
        .register()?;
    let mut info = spa::param::audio::AudioInfoRaw::new();
    info.set_format(spa::param::audio::AudioFormat::F32LE);
    info.set_rate(CAPTURE_RATE);
    info.set_channels(1);
    let mut position = [0; spa::sys::SPA_AUDIO_MAX_CHANNELS as usize];
    position[0] = spa::sys::SPA_AUDIO_CHANNEL_MONO;
    info.set_position(position);
    let format = PodSerializer::serialize(
        Cursor::new(Vec::new()),
        &Value::Object(Object {
            type_: spa::utils::SpaTypes::ObjectParamFormat.as_raw(),
            id: ParamType::EnumFormat.as_raw(),
            properties: info.into(),
        }),
    )
    .expect("format pod")
    .0
    .into_inner();
    stream.connect(
        spa::utils::Direction::Input,
        None,
        pw::stream::StreamFlags::AUTOCONNECT | pw::stream::StreamFlags::MAP_BUFFERS,
        &mut [Pod::from_bytes(&format).expect("format pod")],
    )?;
    Ok((stream, listener))
}

fn run(sink_name: &str, rx: pw::channel::Receiver<Command>, on_event: Rc<dyn Fn(Event)>) -> Result<(), pw::Error> {
    pw::init();
    let mainloop = pw::main_loop::MainLoopRc::new(None)?;
    let context = pw::context::ContextRc::new(&mainloop, None)?;
    let core = context.connect_rc(None)?;
    let registry = core.get_registry_rc()?;

    let description = if sink_name == "radio" { "Radio Stream".to_string() } else { format!("Radio Stream ({sink_name})") };
    let sink = core.create_object::<pw::node::Node>(
        "adapter",
        &pw::properties::properties! {
            "factory.name" => "support.null-audio-sink",
            "node.name" => sink_name,
            "node.description" => description.as_str(),
            "media.class" => "Audio/Sink",
            "audio.position" => "FL,FR",
            // The sink volume must scale the monitor, which is what ffmpeg captures.
            "monitor.channel-volumes" => "true",
        },
    )?;
    let _sink_listener = watch_volume(&sink, {
        let on_event = on_event.clone();
        move |percent| on_event(Event::StreamVolume(percent))
    });

    let loopback = Rc::new(RefCell::new(Loopback {
        context: context.clone(),
        sink: sink_name.to_string(),
        output: None,
        module: std::ptr::null_mut(),
        node: None,
    }));
    let graph = Rc::new(RefCell::new(Graph::default()));
    // The session manager's `default` metadata: default sink out, stream moves in.
    let metadata: Rc<RefCell<Option<(pw::metadata::Metadata, pw::metadata::MetadataListener)>>> = Rc::default();
    let report = {
        let (graph, loopback, on_event) = (graph.clone(), loopback.clone(), on_event.clone());
        let sink_name = sink_name.to_string();
        move || {
            let node = loopback.borrow().node.as_ref().map(|(id, ..)| *id);
            graph.borrow_mut().report(&sink_name, node, &*on_event);
        }
    };

    // Links we made: fallback undo links must live as long as the connection.
    let made_links: Rc<RefCell<Vec<pw::link::Link>>> = Rc::default();
    // Proxies of the links that existed at startup, for their info. Kept to the end:
    // destroying a proxy after the server removed its link is a protocol error.
    let link_proxies: Rc<RefCell<Vec<(pw::link::Link, pw::link::LinkListener)>>> = Rc::default();
    let scanning = Rc::new(Cell::new(true));
    // Undo fallback links of an earlier run: put the player back on our sink, then
    // remove the link to the default sink, or it would be heard twice.
    let undo_leftovers: Rc<dyn Fn()> = Rc::new({
        let (core, registry, graph, made_links) = (core.clone(), registry.clone(), graph.clone(), made_links.clone());
        let sink_name = sink_name.to_string();
        move || {
            let ready = graph.borrow_mut().take_leftovers(&sink_name);
            for (leftover, back) in ready {
                // ponytail: the link back is not confirmed before the leftover goes; if it
                // fails the port is gone or WirePlumber relinks the player to its target.
                made_links.borrow_mut().extend(back.and_then(|link| make_link(&core, link, false)));
                registry.destroy_global(leftover);
            }
        }
    });

    let _registry_listener = registry
        .add_listener_local()
        .global({
            let (registry, loopback, on_event) = (registry.clone(), loopback.clone(), on_event.clone());
            let (graph, metadata, report) = (graph.clone(), metadata.clone(), report.clone());
            let (link_proxies, scanning, undo_leftovers) = (link_proxies.clone(), scanning.clone(), undo_leftovers.clone());
            let sink_name = sink_name.to_string();
            move |global| {
                let Some(props) = global.props else { return };
                match global.type_ {
                    pw::types::ObjectType::Node => {
                        let name = props.get("node.name").unwrap_or_default();
                        if props.get("media.class") == Some("Audio/Sink") {
                            let sink = Sink {
                                name: name.to_string(),
                                description: props.get("node.description").unwrap_or(name).to_string(),
                                serial: props.get("object.serial").unwrap_or_default().to_string(),
                            };
                            graph.borrow_mut().sinks.insert(global.id, sink);
                        }
                        if name == sink_name {
                            on_event(Event::Ready);
                        } else if name == loopback.borrow().playback_name()
                            && let Ok(node) = registry.bind::<pw::node::Node, _>(global)
                        {
                            // WirePlumber restores this stream's saved volume itself; we only report it.
                            let on_event = on_event.clone();
                            let listener = watch_volume(&node, move |percent| on_event(Event::LocalVolume(percent)));
                            loopback.borrow_mut().node = Some((global.id, node, listener));
                        }
                    }
                    pw::types::ObjectType::Port => {
                        let Some(node) = props.get("node.id").and_then(|v| v.parse().ok()) else { return };
                        let port = Port {
                            node,
                            channel: props.get("audio.channel").unwrap_or_default().to_string(),
                            input: props.get("port.direction") == Some("in"),
                        };
                        graph.borrow_mut().ports.insert(global.id, port);
                        undo_leftovers();
                        return;
                    }
                    pw::types::ObjectType::Link => {
                        let id = |key| props.get(key).and_then(|v| v.parse().ok());
                        let (Some(out_node), Some(out_port), Some(in_node), Some(in_port)) =
                            (id("link.output.node"), id("link.output.port"), id("link.input.node"), id("link.input.port"))
                        else {
                            return;
                        };
                        let link = Link { out_node, out_port, in_node, in_port };
                        graph.borrow_mut().links.insert(global.id, link);
                        // The fallback mark is not among the registry properties, only in the
                        // link's own info. Only links from before our start can carry it.
                        if scanning.get()
                            && let Ok(proxy) = registry.bind::<pw::link::Link, _>(global)
                        {
                            let listener = proxy
                                .add_listener_local()
                                .info({
                                    let (graph, undo_leftovers) = (graph.clone(), undo_leftovers.clone());
                                    move |info| {
                                        if info.props().is_some_and(|p| p.get(FALLBACK_KEY).is_some()) {
                                            graph.borrow_mut().leftover.insert(info.id(), link);
                                            undo_leftovers();
                                        }
                                    }
                                })
                                .register();
                            link_proxies.borrow_mut().push((proxy, listener));
                        }
                    }
                    pw::types::ObjectType::Metadata if props.get("metadata.name") == Some("default") => {
                        let Ok(proxy) = registry.bind::<pw::metadata::Metadata, _>(global) else { return };
                        let listener = proxy
                            .add_listener_local()
                            .property({
                                let graph = graph.clone();
                                move |subject, key, _type, value| {
                                    if subject == 0 && key == Some("default.audio.sink") {
                                        graph.borrow_mut().default_sink = value.and_then(json_name);
                                    }
                                    0
                                }
                            })
                            .register();
                        *metadata.borrow_mut() = Some((proxy, listener));
                    }
                    _ => return,
                }
                report();
            }
        })
        .global_remove({
            let (loopback, graph, report) = (loopback.clone(), graph.clone(), report.clone());
            move |id| {
                {
                    let mut loopback = loopback.borrow_mut();
                    if loopback.node.as_ref().is_some_and(|(node_id, ..)| *node_id == id) {
                        loopback.node = None;
                    }
                    let mut graph = graph.borrow_mut();
                    graph.sinks.remove(&id);
                    graph.ports.remove(&id);
                    graph.links.remove(&id);
                    graph.leftover.remove(&id);
                }
                report();
            }
        })
        .register();

    let _capture = capture(&core, sink_name, on_event.clone())?;

    // Everything that existed before us has been announced once this comes back.
    let scan_seq = core.sync(0)?;
    let quit_seq = Rc::new(Cell::new(None));
    let fallback_sink: Rc<RefCell<Option<String>>> = Rc::default();
    let _core_listener = core
        .add_listener_local()
        .done({
            let (mainloop, scanning, quit_seq) = (mainloop.clone(), scanning.clone(), quit_seq.clone());
            move |id, seq| {
                if id != pw::core::PW_ID_CORE {
                    return;
                }
                if seq == scan_seq {
                    scanning.set(false);
                }
                if quit_seq.get() == Some(seq) {
                    mainloop.quit();
                }
            }
        })
        .error({
            let (mainloop, on_event) = (mainloop.clone(), on_event.clone());
            move |id, _seq, res, message| {
                // EPIPE on the core itself: the connection is gone. Other core errors answer
                // a single request (such as one for an object that just went away).
                if id == pw::core::PW_ID_CORE && res == -EPIPE {
                    on_event(Event::Failed(message.to_string()));
                    mainloop.quit();
                }
            }
        })
        .register();

    let _commands = rx.attach(mainloop.loop_(), {
        let (mainloop, loopback) = (mainloop.clone(), loopback.clone());
        let (graph, metadata) = (graph.clone(), metadata.clone());
        let (core, quit_seq, fallback_sink) = (core.clone(), quit_seq.clone(), fallback_sink.clone());
        let sink_name = sink_name.to_string();
        move |command| match command {
            Command::StreamVolume(percent) => set_volume(&sink, percent),
            Command::LocalVolume(percent) => {
                if let Some((_, node, _)) = &loopback.borrow().node {
                    set_volume(node, percent);
                }
            }
            Command::Monitor(true) => loopback.borrow_mut().load(),
            Command::Monitor(false) => loopback.borrow_mut().unload(),
            Command::Output(output) => {
                let mut loopback = loopback.borrow_mut();
                // Moving a live stream is a `target.object` entry in the default metadata,
                // which is what `pactl move-sink-input` writes; WirePlumber saves it.
                let serial = graph.borrow().sinks.values().find(|s| s.name == output).map(|s| s.serial.clone());
                if let (Some((node, ..)), Some((metadata, _)), Some(serial)) =
                    (&loopback.node, &*metadata.borrow(), serial)
                {
                    metadata.set_property(*node, "target.object", Some("Spa:Id"), Some(&serial));
                }
                loopback.output = Some(output);
            }
            #[cfg(test)]
            Command::FallbackSink(name) => *fallback_sink.borrow_mut() = Some(name),
            Command::Quit => {
                // Our sink goes away with the connection. Give every player on it a link to
                // the default sink first: WirePlumber moves them there anyway, but only
                // after a gap. In tests only a sink the test named is used.
                let target = fallback_sink.borrow().clone().or_else(|| {
                    if cfg!(test) { None } else { graph.borrow().default_sink.clone() }
                });
                let wanted = target.map(|t| graph.borrow().fallback_links(&sink_name, &t)).unwrap_or_default();
                for link in &wanted {
                    // Not dropped: destroying the proxy would take the link with it.
                    std::mem::forget(make_link(&core, *link, true));
                }
                // The server must have the links before it sees us leave.
                match core.sync(0) {
                    Ok(seq) if !wanted.is_empty() => quit_seq.set(Some(seq)),
                    _ => mainloop.quit(),
                }
            }
        }
    });

    mainloop.run();
    loopback.borrow_mut().unload();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{Receiver, channel};
    use std::time::{Duration, Instant};

    const SINK: &str = "radio-dev";
    const OUT: &str = "radio-dev-out";
    const OUT2: &str = "radio-dev-out2";

    fn spawn_with_events(sink: &str) -> (Handle, Receiver<Event>) {
        let (tx, rx) = channel();
        (spawn(sink, move |event| drop(tx.send(event))), rx)
    }

    /// Wait for the first event `wanted` accepts, ignoring the others.
    fn wait_event(rx: &Receiver<Event>, what: &str, wanted: impl Fn(&Event) -> bool) -> Event {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match rx.recv_timeout(left) {
                Ok(Event::Failed(error)) => panic!("pipewire thread failed: {error}"),
                Ok(event) if wanted(&event) => return event,
                Ok(_) => {}
                Err(_) => panic!("timed out waiting for {what}"),
            }
        }
    }

    fn expect(rx: &Receiver<Event>, wanted: Event) {
        wait_event(rx, &format!("{wanted:?}"), |event| *event == wanted);
    }

    /// Poll until `check` holds; the pulse view lags behind our own events.
    fn wait_for(what: &str, check: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !check() {
            assert!(Instant::now() < deadline, "timed out waiting for: {what}");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn pactl(args: &[&str]) -> String {
        let out = std::process::Command::new("pactl").args(args).output().expect("pactl");
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    fn sink_exists(name: &str) -> bool {
        pactl(&["list", "short", "sinks"]).lines().any(|line| line.split('\t').nth(1) == Some(name))
    }

    /// Input ports the output port `port` is linked to, from `pw-link -l`.
    fn links_of(port: &str) -> Vec<String> {
        let out = std::process::Command::new("pw-link").arg("-l").output().expect("pw-link");
        let (mut found, mut inside) = (Vec::new(), false);
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            if !line.starts_with(' ') {
                inside = line == port;
            } else if inside && let Some(peer) = line.trim().strip_prefix("|-> ") {
                found.push(peer.to_string());
            }
        }
        found.sort();
        found
    }

    /// A silent player that WirePlumber leaves alone (`--target 0`: no autoconnect), so it
    /// can never be moved to the real output device. The test links it by hand.
    struct Player(std::process::Child);

    impl Player {
        fn start() -> Player {
            let child = std::process::Command::new("pw-cat")
                .args(["-p", "-a", "--rate", "48000", "--channels", "2", "--format", "s16", "--target", "0"])
                .args(["-P", r#"{ node.name = "radio-dev-player" }"#, "/dev/zero"])
                .spawn()
                .expect("pw-cat");
            Player(child)
        }
    }

    impl Drop for Player {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn graph_with_player() -> Graph {
        let mut graph = Graph::default();
        for (id, name) in [(10, "radio-dev"), (20, "out")] {
            graph.sinks.insert(id, Sink { name: name.into(), description: name.into(), serial: id.to_string() });
        }
        // (port, node, channel, input): both sinks, the sink monitor, a stereo and a mono player.
        for (id, node, channel, input) in [
            (11, 10, "FL", true), (12, 10, "FR", true), (13, 10, "FL", false),
            (21, 20, "FL", true), (22, 20, "FR", true),
            (31, 30, "FL", false), (32, 30, "FR", false), (41, 40, "MONO", false),
        ] {
            graph.ports.insert(id, Port { node, channel: channel.into(), input });
        }
        graph
    }

    fn link(out_node: u32, out_port: u32, in_node: u32, in_port: u32) -> Link {
        Link { out_node, out_port, in_node, in_port }
    }

    #[test]
    fn fallback_links_copy_each_player_port_once() {
        let mut graph = graph_with_player();
        graph.links.insert(100, link(30, 31, 10, 11));
        graph.links.insert(101, link(30, 32, 10, 12));
        graph.links.insert(102, link(30, 31, 20, 21)); // FL already plays to the target
        graph.links.insert(103, link(40, 41, 10, 11)); // crosses channels: not copied
        graph.links.insert(104, link(10, 13, 50, 51)); // our monitor feeding a capture
        assert_eq!(graph.fallback_links("radio-dev", "out"), [link(30, 32, 20, 22)]);
        assert_eq!(graph.fallback_links("radio-dev", "radio-dev"), []);
        assert_eq!(graph.fallback_links("radio-dev", "missing"), []);
    }

    #[test]
    fn leftovers_wait_for_our_ports_and_skip_existing_links() {
        let mut graph = graph_with_player();
        graph.links.insert(100, link(30, 31, 10, 11)); // FL is already back on our sink
        graph.leftover.insert(200, link(30, 31, 20, 21));
        graph.leftover.insert(201, link(30, 32, 20, 22));
        let own_ports: Vec<_> = [11, 12].into_iter().map(|id| (id, graph.ports.remove(&id).unwrap())).collect();
        assert_eq!(graph.take_leftovers("radio-dev"), []);
        assert_eq!(graph.leftover.len(), 2);
        graph.ports.extend(own_ports);
        let mut ready = graph.take_leftovers("radio-dev");
        ready.sort();
        assert_eq!(ready, [(200, None), (201, Some(link(30, 32, 10, 12)))]);
        assert!(graph.leftover.is_empty());
    }

    #[test]
    fn json_name_reads_the_default_sink_value() {
        assert_eq!(json_name(r#"{"name":"alsa_output.usb-x.analog-stereo"}"#).as_deref(), Some("alsa_output.usb-x.analog-stereo"));
        assert_eq!(json_name(r#"{ "name": "radio-dev" }"#).as_deref(), Some("radio-dev"));
        assert_eq!(json_name("{}"), None);
        assert_eq!(json_name(r#"{"name":"#), None);
    }

    #[test]
    fn percent_mapping_matches_pactl() {
        assert_eq!(linear_to_percent(0.5), 79);
        assert_eq!(linear_to_percent(1.0), 100);
        assert_eq!(linear_to_percent(0.0), 0);
        for percent in [0, 1, 25, 50, 79, 100, 150] {
            assert_eq!(linear_to_percent(percent_to_linear(percent)), percent);
        }
    }

    /// One test for the live server: parallel tests would fight over the dev sink names.
    /// Silent: the loopback plays into a second null sink, never a real device.
    #[test]
    fn sink_loopback_and_volumes_on_live_pipewire() {
        let (out, out_events) = spawn_with_events(OUT);
        expect(&out_events, Event::Ready);
        let (out2, out2_events) = spawn_with_events(OUT2);
        expect(&out2_events, Event::Ready);
        // Outputs events are deduplicated by the thread, so the test keeps the latest one
        // instead of fishing for it in the queue.
        let outputs = std::sync::Arc::new(std::sync::Mutex::new(None));
        let (tx, events) = channel();
        let panel = spawn(SINK, {
            let outputs = outputs.clone();
            move |event| match event {
                Event::Outputs { .. } => *outputs.lock().unwrap() = Some(event),
                other => drop(tx.send(other)),
            }
        });
        expect(&events, Event::Ready);
        wait_for("sink visible", || sink_exists(SINK));

        // Stream volume: set natively, confirmed by our event and by pactl. Wait first:
        // WirePlumber restores the saved volume just after the node appears and would
        // override an earlier set. The app only sets volumes on user action.
        std::thread::sleep(Duration::from_millis(500));
        panel.send(Command::StreamVolume(50));
        expect(&events, Event::StreamVolume(50));
        wait_for("pactl sink volume 50%", || pactl(&["get-sink-volume", SINK]).contains("50%"));

        // Monitor on: loopback appears, playing into the dev output sink.
        let loopback_listed = || pactl(&["list", "sink-inputs"]).contains("output.radio-dev-local");
        panel.send(Command::Output(OUT.into()));
        panel.send(Command::Monitor(true));
        // WirePlumber restores whatever volume this stream had last time.
        let Event::LocalVolume(restored) = wait_event(&events, "a local volume", |e| matches!(e, Event::LocalVolume(_)))
        else {
            unreachable!()
        };
        wait_for("loopback stream listed", loopback_listed);

        // Local volume set after the session manager settled must stick.
        let target = if restored == 40 { 60 } else { 40 };
        std::thread::sleep(Duration::from_millis(500));
        panel.send(Command::LocalVolume(target));
        expect(&events, Event::LocalVolume(target));
        std::thread::sleep(Duration::from_millis(500));
        assert!(pactl(&["list", "sink-inputs"]).contains(&format!("{target}%")), "local volume did not stick");

        // The device list has both dev outputs and never our own sink. Send the target
        // again now that the stream exists: a saved target from an aborted run may differ.
        let playing_to = |target: &'static str| {
            let outputs = outputs.clone();
            move || matches!(&*outputs.lock().unwrap(), Some(Event::Outputs { sinks, current })
                if current.as_deref() == Some(target)
                    && [OUT, OUT2].iter().all(|o| sinks.iter().any(|(name, _)| name == o))
                    && !sinks.iter().any(|(name, _)| name == SINK))
        };
        panel.send(Command::Output(OUT.into()));
        wait_for("monitor on the first dev output", playing_to(OUT));
        // Move the live stream and back, so the saved target stays the first output.
        panel.send(Command::Output(OUT2.into()));
        wait_for("monitor moved to the second dev output", playing_to(OUT2));
        panel.send(Command::Output(OUT.into()));
        wait_for("monitor moved back", playing_to(OUT));

        // The capture stream delivers the (silent) sink monitor.
        wait_event(&events, "captured samples", |e| matches!(e, Event::Samples(s) if !s.is_empty()));

        // Leave the saved WirePlumber state at 100% for both.
        panel.send(Command::LocalVolume(100));
        expect(&events, Event::LocalVolume(100));
        panel.send(Command::StreamVolume(100));
        expect(&events, Event::StreamVolume(100));

        // Monitor off removes the loopback; the sink stays.
        panel.send(Command::Monitor(false));
        wait_for("loopback stream gone", || !loopback_listed());
        assert!(sink_exists(SINK));

        // A player on the sink stays linked when the panel quits: to the fallback sink.
        let player = Player::start();
        let (left, right) = ("radio-dev-player:output_FL", "radio-dev-player:output_FR");
        let on = |sink: &str| (vec![format!("{sink}:playback_FL")], vec![format!("{sink}:playback_FR")]);
        wait_for("player linked into the sink", || {
            for (output, input) in [(left, "radio-dev:playback_FL"), (right, "radio-dev:playback_FR")] {
                let _ = std::process::Command::new("pw-link").args([output, input]).output();
            }
            (links_of(left), links_of(right)) == on(SINK)
        });
        panel.send(Command::FallbackSink(OUT.into()));
        drop(panel);
        wait_for("sink gone", || !sink_exists(SINK));
        wait_for("player on the fallback sink", || (links_of(left), links_of(right)) == on(OUT));

        // The next start takes the player back and removes the fallback links.
        let (panel, events) = spawn_with_events(SINK);
        expect(&events, Event::Ready);
        wait_for("player back on the sink only", || (links_of(left), links_of(right)) == on(SINK));
        drop(player);

        // Dropping the handle ends the thread and the server removes the sink.
        drop(panel);
        drop(out);
        drop(out2);
        wait_for("sinks gone", || ![SINK, OUT, OUT2].iter().any(|s| sink_exists(s)));
    }
}
