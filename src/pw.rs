//! Native PipeWire layer: one thread owns the connection, the `radio` sink and the
//! in-process loopback. Commands go in over a pipewire channel, events come out
//! through a callback that runs on that thread. See docs/pipewire-notes.md.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
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
    /// Ends the thread; the server then removes the sink and the loopback. WirePlumber
    /// moves the players that were on it to the default sink (and pauses them, see
    /// `linking.pause-playback`).
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

/// What players show in their output picker.
pub fn sink_description(sink: &str) -> String {
    if sink == "radio" { "Radio Stream".to_string() } else { format!("Radio Stream ({sink})") }
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
    /// Target named in the module arguments: an output picked while the monitor was
    /// off. Without one WirePlumber restores the saved target.
    output: Option<String>,
    /// `output` still has to be written as a move, so that WirePlumber saves it.
    save_output: bool,
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
        self.save_output = self.output.is_some();
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

#[derive(Clone, Copy)]
struct Link {
    out_node: u32,
    in_node: u32,
}

const EPIPE: i32 = 32;

/// What the registry has announced, as far as the panel needs it.
#[derive(Default)]
struct Graph {
    /// Audio sinks by node id.
    sinks: HashMap<u32, Sink>,
    links: HashMap<u32, Link>,
    /// Stream nodes to move onto our sink once its serial and the metadata are known.
    adopt: Vec<u32>,
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

/// The string member `key` of a flat JSON object such as `{"name":"alsa_output.x"}`.
fn json_string(value: &str, key: &str) -> Option<String> {
    let key = format!("\"{key}\"");
    let rest = &value[value.find(&key)? + key.len()..];
    let rest = &rest[rest.find(':')? + 1..];
    let rest = &rest[rest.find('"')? + 1..];
    Some(rest[..rest.find('"')?].to_string())
}

/// WirePlumber's name for a stream in its `stream-properties` state file: `formKey`
/// in its `node/state-stream.lua` (read in 0.5.19).
fn stream_key<'a>(prop: impl Fn(&str) -> Option<&'a str>) -> Option<String> {
    let class = prop("media.class")?;
    let class = class.strip_prefix("Stream/").unwrap_or(class);
    if prop("media.role") == Some("Notification") {
        return Some(format!("{class}:media.role:Notification"));
    }
    ["application.id", "application.name", "media.name", "node.name"]
        .into_iter()
        .find_map(|key| prop(key).map(|value| format!("{class}:{key}:{value}")))
}

/// Keys of the streams whose saved target is `sink`, from the text of that state file.
/// Its keys escape space, `[` and `]` as `\s`, `\o`, `\c` (seen in the file); `\e` for
/// `=` and `\\` are from WirePlumber's source as remembered, not seen.
fn streams_saved_to(state: &str, sink: &str) -> HashSet<String> {
    let unescape = |key: &str| {
        let (mut plain, mut chars) = (String::new(), key.chars());
        while let Some(character) = chars.next() {
            let escaped = if character == '\\' { chars.next() } else { None };
            plain.push(match escaped {
                Some('s') => ' ',
                Some('e') => '=',
                Some('o') => '[',
                Some('c') => ']',
                Some(other) => other,
                None => character,
            });
        }
        plain
    };
    state
        .lines()
        .filter_map(|line| line.split_once('='))
        .filter(|(_, value)| json_string(value, "target").as_deref() == Some(sink))
        .map(|(key, _)| unescape(key))
        .collect()
}

/// The state file text. Tests name the saved streams themselves.
fn saved_streams_state() -> String {
    #[cfg(test)]
    if let Some(state) = tests::SAVED_STATE.lock().unwrap().clone() {
        return state;
    }
    let dir = std::env::var("XDG_STATE_HOME").ok().filter(|dir| !dir.is_empty());
    let dir = dir.unwrap_or_else(|| format!("{}/.local/state", std::env::var("HOME").unwrap_or_default()));
    std::fs::read_to_string(format!("{dir}/wireplumber/stream-properties")).unwrap_or_default()
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

    let description = sink_description(sink_name);
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
        save_output: false,
        module: std::ptr::null_mut(),
        node: None,
    }));
    let graph = Rc::new(RefCell::new(Graph::default()));
    // The session manager's `default` metadata, for stream moves.
    let metadata: Rc<RefCell<Option<pw::metadata::Metadata>>> = Rc::default();
    let report = {
        let (graph, loopback, on_event) = (graph.clone(), loopback.clone(), on_event.clone());
        let sink_name = sink_name.to_string();
        move || {
            let node = loopback.borrow().node.as_ref().map(|(id, ..)| *id);
            graph.borrow_mut().report(&sink_name, node, &*on_event);
        }
    };

    let scanning = Rc::new(Cell::new(true));
    // A player started while our sink did not exist stays on the default device:
    // WirePlumber applies a saved target only when a stream is created. Move the
    // streams that are saved to our sink the way pavucontrol would.
    let saved = Rc::new(streams_saved_to(&saved_streams_state(), sink_name));
    // Proxies of the playback streams that existed at startup, for their info; kept
    // to the end like the link proxies.
    let stream_proxies: Rc<RefCell<Vec<(pw::node::Node, pw::node::NodeListener)>>> = Rc::default();
    let adopt: Rc<dyn Fn()> = Rc::new({
        let (graph, metadata) = (graph.clone(), metadata.clone());
        let sink_name = sink_name.to_string();
        move || {
            let mut graph = graph.borrow_mut();
            let sink = graph.sinks.iter().find(|(_, s)| s.name == sink_name && !s.serial.is_empty());
            let serial = sink.map(|(id, s)| (*id, s.serial.clone()));
            // WirePlumber saves a target by looking the serial up among the nodes it
            // knows; a sink it has not seen yet is saved as no target, which wipes the
            // route set up in pavucontrol. It links our spectrum capture to the sink
            // once it knows it, so wait for a link out of the sink.
            let known = serial.as_ref().is_some_and(|(id, _)| graph.links.values().any(|link| link.out_node == *id));
            if let (true, Some((_, serial)), Some(metadata)) = (known, serial, &*metadata.borrow()) {
                for node in graph.adopt.drain(..) {
                    metadata.set_property(node, "target.object", Some("Spa:Id"), Some(&serial));
                }
            }
        }
    });
    // A target in the loopback's own properties is not saved by WirePlumber. Write it
    // as a move too, once WirePlumber has linked the loopback (it saves a move only
    // for a node it knows); from then on WirePlumber remembers it.
    let save_output: Rc<dyn Fn()> = Rc::new({
        let (loopback, graph, metadata) = (loopback.clone(), graph.clone(), metadata.clone());
        move || {
            let mut loopback = loopback.borrow_mut();
            let Some(node) = loopback.node.as_ref().map(|(id, ..)| *id).filter(|_| loopback.save_output) else { return };
            let graph = graph.borrow();
            if !graph.links.values().any(|link| link.out_node == node) {
                return;
            }
            let serial = graph.sinks.values().find(|s| Some(&s.name) == loopback.output.as_ref()).map(|s| s.serial.clone());
            if let (Some(serial), Some(metadata)) = (serial, &*metadata.borrow()) {
                metadata.set_property(node, "target.object", Some("Spa:Id"), Some(&serial));
                (loopback.save_output, loopback.output) = (false, None);
            }
        }
    });

    let _registry_listener = registry
        .add_listener_local()
        .global({
            let (registry, loopback, on_event) = (registry.clone(), loopback.clone(), on_event.clone());
            let (graph, metadata, report) = (graph.clone(), metadata.clone(), report.clone());
            let scanning = scanning.clone();
            let (saved, stream_proxies, adopt, save_output) = (saved.clone(), stream_proxies.clone(), adopt.clone(), save_output.clone());
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
                        } else if scanning.get()
                            && !saved.is_empty()
                            && props.get("media.class") == Some("Stream/Output/Audio")
                            && let Ok(node) = registry.bind::<pw::node::Node, _>(global)
                        {
                            // The key needs properties the registry does not carry.
                            let (graph, saved, adopt, first) = (graph.clone(), saved.clone(), adopt.clone(), Cell::new(true));
                            let listener = node
                                .add_listener_local()
                                .info(move |info| {
                                    // Once: a later move by the user must stand. Like WirePlumber's
                                    // own restore, leave a stream that names its target alone.
                                    let Some(props) = info.props().filter(|_| first.replace(false)) else { return };
                                    if props.get("target.object").is_none()
                                        && props.get("node.target").is_none()
                                        && stream_key(|key| props.get(key)).is_some_and(|key| saved.contains(&key))
                                    {
                                        graph.borrow_mut().adopt.push(info.id());
                                        adopt();
                                    }
                                })
                                .register();
                            stream_proxies.borrow_mut().push((node, listener));
                        }
                    }
                    pw::types::ObjectType::Link => {
                        let id = |key| props.get(key).and_then(|v| v.parse().ok());
                        let (Some(out_node), Some(in_node)) = (id("link.output.node"), id("link.input.node")) else { return };
                        graph.borrow_mut().links.insert(global.id, Link { out_node, in_node });
                    }
                    pw::types::ObjectType::Metadata if props.get("metadata.name") == Some("default") => {
                        *metadata.borrow_mut() = registry.bind::<pw::metadata::Metadata, _>(global).ok();
                    }
                    _ => return,
                }
                adopt();
                save_output();
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
                    graph.links.remove(&id);
                }
                report();
            }
        })
        .register();

    let _capture = capture(&core, sink_name, on_event.clone())?;

    // Everything that existed before us has been announced once this comes back.
    let scan_seq = core.sync(0)?;
    let _core_listener = core
        .add_listener_local()
        .done({
            let scanning = scanning.clone();
            move |id, seq| {
                if id == pw::core::PW_ID_CORE && seq == scan_seq {
                    scanning.set(false);
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
                if let (Some((node, ..)), Some(metadata), Some(serial)) =
                    (&loopback.node, &*metadata.borrow(), serial)
                {
                    metadata.set_property(*node, "target.object", Some("Spa:Id"), Some(&serial));
                } else {
                    // The monitor is off: the pick applies when it is turned on.
                    loopback.output = Some(output);
                }
            }
            Command::Quit => mainloop.quit(),
        }
    });

    mainloop.run();
    loopback.borrow_mut().unload();
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::mpsc::{Receiver, channel};
    use std::time::{Duration, Instant};

    const SINK: &str = "radio-dev";
    const OUT: &str = "radio-dev-out";
    const OUT2: &str = "radio-dev-out2";

    /// Stands in for WirePlumber's state file while a test runs.
    pub(crate) static SAVED_STATE: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

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
    pub(crate) fn wait_for(what: &str, check: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !check() {
            assert!(Instant::now() < deadline, "timed out waiting for: {what}");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub(crate) fn pactl(args: &[&str]) -> String {
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
                .args(["-P", r#"{ node.name = "radio-dev-player" application.name = "radio-dev-player" }"#, "/dev/zero"])
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

    /// (id, object.serial) of the node called `name`, from `pw-cli ls Node`.
    fn node_ids(name: &str) -> Option<(String, String)> {
        let out = std::process::Command::new("pw-cli").args(["ls", "Node"]).output().expect("pw-cli");
        let (mut id, mut serial) = (String::new(), String::new());
        for line in String::from_utf8_lossy(&out.stdout).lines().map(str::trim) {
            if let Some(rest) = line.strip_prefix("id ") {
                id = rest.split(',').next().unwrap_or_default().to_string();
            } else if let Some(value) = line.strip_prefix("object.serial = ") {
                serial = value.trim_matches('"').to_string();
            } else if line.strip_prefix("node.name = ").is_some_and(|value| value.trim_matches('"') == name) {
                return Some((id, serial));
            }
        }
        None
    }

    #[test]
    fn json_string_reads_the_default_sink_value() {
        let name = |value| json_string(value, "name");
        assert_eq!(name(r#"{"name":"alsa_output.usb-x.analog-stereo"}"#).as_deref(), Some("alsa_output.usb-x.analog-stereo"));
        assert_eq!(name(r#"{ "name": "radio-dev" }"#).as_deref(), Some("radio-dev"));
        assert_eq!(name("{}"), None);
        assert_eq!(name(r#"{"name":"#), None);
    }

    #[test]
    fn stream_key_follows_wireplumber() {
        let key = |props: &[(&'static str, &'static str)]| {
            let props = props.to_vec();
            stream_key(move |wanted| props.iter().find(|(name, _)| *name == wanted).map(|(_, value)| *value))
        };
        let class = ("media.class", "Stream/Output/Audio");
        let aimp = [class, ("application.name", "PipeWire ALSA [AIMP]"), ("media.name", "ALSA Playback"), ("node.name", "alsa_playback.AIMP")];
        assert_eq!(key(&aimp).as_deref(), Some("Output/Audio:application.name:PipeWire ALSA [AIMP]"));
        // application.id comes first; only the Notification role replaces the application.
        assert_eq!(key(&[class, ("application.name", "n"), ("application.id", "i")]).as_deref(), Some("Output/Audio:application.id:i"));
        assert_eq!(key(&[class, ("media.role", "Music"), ("media.name", "m"), ("node.name", "x")]).as_deref(), Some("Output/Audio:media.name:m"));
        assert_eq!(key(&[class, ("media.role", "Notification"), ("application.name", "n")]).as_deref(), Some("Output/Audio:media.role:Notification"));
        assert_eq!(key(&[class, ("node.name", "x")]).as_deref(), Some("Output/Audio:node.name:x"));
        assert_eq!(key(&[("media.class", "Audio/Sink"), ("node.name", "x")]).as_deref(), Some("Audio/Sink:node.name:x"));
        assert_eq!(key(&[class]), None);
        assert_eq!(key(&[("node.name", "x")]), None);
    }

    #[test]
    fn saved_streams_are_read_from_the_state_file() {
        // The first three lines are copied from a real file.
        let state = r#"[stream-properties]
Output/Audio:application.name:PipeWire\sALSA\s\oAIMP\c={"volume":1.000000, "channelVolumes":[1.000000, 1.000000], "mute":false, "target":"radio", "channelMap":["FL", "FR"]}
Audio/Sink:node.name:radio={"volume":1.000000, "mute":false, "channelMap":["FL", "FR"], "channelVolumes":[1.000000, 1.000000]}
Output/Audio:media.name:radio-dev-local\soutput={"volume":1.000000, "target":"radio-dev-out", "mute":false}
Output/Audio:application.name:a\eb\\c={"target":"radio"}
Output/Audio:application.name:other={"target":"radio2"}
"#;
        let keys = |sink| {
            let mut keys: Vec<_> = streams_saved_to(state, sink).into_iter().collect();
            keys.sort();
            keys
        };
        assert_eq!(keys("radio"), ["Output/Audio:application.name:PipeWire ALSA [AIMP]", "Output/Audio:application.name:a=b\\c"]);
        assert_eq!(keys("radio-dev-out"), ["Output/Audio:media.name:radio-dev-local output"]);
        assert!(keys("radio-dev").is_empty());
        assert!(streams_saved_to("", "radio").is_empty());
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

        // An output picked while the monitor is off is used when it is turned on, and
        // WirePlumber remembers it like a move.
        let local_key = "Output/Audio:media.name:radio-dev-local output";
        let saved_to = |sink: &str| streams_saved_to(&saved_streams_state(), sink).contains(local_key);
        wait_for("first output remembered", || saved_to(OUT));
        panel.send(Command::Output(OUT2.into()));
        panel.send(Command::Monitor(true));
        wait_for("monitor on the second dev output", playing_to(OUT2));
        wait_for("second output remembered", || saved_to(OUT2));
        panel.send(Command::Output(OUT.into()));
        wait_for("monitor moved back", playing_to(OUT));
        wait_for("first output remembered again", || saved_to(OUT));
        panel.send(Command::Monitor(false));
        wait_for("loopback stream gone", || !loopback_listed());

        // A player on the sink at quit: the panel makes no links of its own. WirePlumber
        // ignores this player (no autoconnect), so nothing links it anywhere.
        let player = Player::start();
        let (left, right) = ("radio-dev-player:output_FL", "radio-dev-player:output_FR");
        let on = |sink: &str| (vec![format!("{sink}:playback_FL")], vec![format!("{sink}:playback_FR")]);
        wait_for("player linked into the sink", || {
            for (output, input) in [(left, "radio-dev:playback_FL"), (right, "radio-dev:playback_FR")] {
                let _ = std::process::Command::new("pw-link").args([output, input]).output();
            }
            (links_of(left), links_of(right)) == on(SINK)
        });
        drop(panel);
        wait_for("sink gone", || !sink_exists(SINK));
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!((links_of(left), links_of(right)), (vec![], vec![]));

        // The next start gives a running player saved to our sink our sink as its target,
        // as pavucontrol would; WirePlumber does the move (not for this player) and saves
        // it. Start from no saved target in WirePlumber's own file.
        let key = "Output/Audio:application.name:radio-dev-player";
        let saved_by_wireplumber = || streams_saved_to(&saved_streams_state(), SINK).contains(key);
        let (player_id, _) = node_ids("radio-dev-player").expect("player node");
        let _ = std::process::Command::new("pw-metadata").args(["-n", "default", "--", &player_id, "target.object", "-1", "Spa:Id"]).output();
        wait_for("no saved target", || !saved_by_wireplumber());
        let state = r#"Output/Audio:application.name:radio-dev-player={"target":"radio-dev"}"#;
        *SAVED_STATE.lock().unwrap() = Some(state.into());
        let (panel, events) = spawn_with_events(SINK);
        expect(&events, Event::Ready);
        *SAVED_STATE.lock().unwrap() = None;
        wait_for("player targeted at the sink", || {
            let (Some((player, _)), Some((_, sink))) = (node_ids("radio-dev-player"), node_ids(SINK)) else { return false };
            let out = std::process::Command::new("pw-metadata").args(["-n", "default"]).output().expect("pw-metadata");
            String::from_utf8_lossy(&out.stdout).contains(&format!("id:{player} key:'target.object' value:'{sink}'"))
        });
        // WirePlumber saved our sink as the player's target. Written before WirePlumber
        // knew the new sink, it saved none: the route was gone from then on.
        wait_for("target saved by WirePlumber", saved_by_wireplumber);
        assert_eq!((links_of(left), links_of(right)), (vec![], vec![]));
        drop(player);

        // Dropping the handle ends the thread and the server removes the sink.
        drop(panel);
        drop(out);
        drop(out2);
        wait_for("sinks gone", || ![SINK, OUT, OUT2].iter().any(|s| sink_exists(s)));
    }
}
