//! Native PipeWire layer: one thread owns the connection, the `radio` sink and the
//! in-process loopback. Commands go in over a pipewire channel, events come out
//! through a callback that runs on that thread. See docs/pipewire-notes.md.
#![allow(dead_code)] // until the UI uses it

use std::cell::RefCell;
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
    /// Output device for local monitoring; `None` follows the default device.
    Output(Option<String>),
    Quit,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// The sink exists; players can be routed to it.
    Ready,
    StreamVolume(u32),
    LocalVolume(u32),
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
    /// Ends the thread; the server then removes the sink and the loopback.
    fn drop(&mut self) {
        self.send(Command::Quit);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

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

    let _registry_listener = registry
        .add_listener_local()
        .global({
            let (registry, loopback, on_event) = (registry.clone(), loopback.clone(), on_event.clone());
            let sink_name = sink_name.to_string();
            move |global| {
                if global.type_ != pw::types::ObjectType::Node {
                    return;
                }
                let name = global.props.and_then(|p| p.get("node.name"));
                if name == Some(sink_name.as_str()) {
                    on_event(Event::Ready);
                } else if name == Some(loopback.borrow().playback_name().as_str())
                    && let Ok(node) = registry.bind::<pw::node::Node, _>(global)
                {
                    // WirePlumber restores this stream's saved volume itself; we only report it.
                    let on_event = on_event.clone();
                    let listener = watch_volume(&node, move |percent| on_event(Event::LocalVolume(percent)));
                    loopback.borrow_mut().node = Some((global.id, node, listener));
                }
            }
        })
        .global_remove({
            let loopback = loopback.clone();
            move |id| {
                let mut loopback = loopback.borrow_mut();
                if loopback.node.as_ref().is_some_and(|(node_id, ..)| *node_id == id) {
                    loopback.node = None;
                }
            }
        })
        .register();

    let _core_listener = core
        .add_listener_local()
        .error({
            let (mainloop, on_event) = (mainloop.clone(), on_event.clone());
            move |id, _seq, _res, message| {
                // id 0 is the core itself: the connection is gone.
                if id == 0 {
                    on_event(Event::Failed(message.to_string()));
                    mainloop.quit();
                }
            }
        })
        .register();

    let _commands = rx.attach(mainloop.loop_(), {
        let (mainloop, loopback) = (mainloop.clone(), loopback.clone());
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
                if loopback.output != output {
                    loopback.output = output;
                    // ponytail: reload moves the stream with a short gap in local audio only;
                    // set the `target.object` metadata instead if the gap is audible.
                    if !loopback.module.is_null() {
                        loopback.unload();
                        loopback.load();
                    }
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
mod tests {
    use super::*;
    use std::sync::mpsc::{Receiver, channel};
    use std::time::{Duration, Instant};

    const SINK: &str = "radio-dev";
    const OUT: &str = "radio-dev-out";

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
        let (panel, events) = spawn_with_events(SINK);
        expect(&events, Event::Ready);
        wait_for("sink visible", || pactl(&["list", "short", "sinks"]).contains(SINK));

        // Stream volume: set natively, confirmed by our event and by pactl. Wait first:
        // WirePlumber restores the saved volume just after the node appears and would
        // override an earlier set. The app only sets volumes on user action.
        std::thread::sleep(Duration::from_millis(500));
        panel.send(Command::StreamVolume(50));
        expect(&events, Event::StreamVolume(50));
        wait_for("pactl sink volume 50%", || pactl(&["get-sink-volume", SINK]).contains("50%"));

        // Monitor on: loopback appears, playing into the dev output sink.
        let loopback_listed = || pactl(&["list", "sink-inputs"]).contains("output.radio-dev-local");
        panel.send(Command::Output(Some(OUT.into())));
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

        // Leave the saved WirePlumber state at 100% for both.
        panel.send(Command::LocalVolume(100));
        expect(&events, Event::LocalVolume(100));
        panel.send(Command::StreamVolume(100));
        expect(&events, Event::StreamVolume(100));

        // Monitor off removes the loopback; the sink stays.
        panel.send(Command::Monitor(false));
        wait_for("loopback stream gone", || !loopback_listed());
        assert!(pactl(&["list", "short", "sinks"]).contains(SINK));

        // Dropping the handle ends the thread and the server removes the sink.
        drop(panel);
        drop(out);
        wait_for("sink gone", || !pactl(&["list", "short", "sinks"]).contains(SINK));
    }
}
