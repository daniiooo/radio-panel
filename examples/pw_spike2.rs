//! PipeWire spike 2: native volume set/readback on our sink and on the in-process
//! loopback's playback node, plus a capture stream on the sink monitor.
//!
//! Prints the sink monitor peak twice a second. Sink volume goes 1.0 -> 0.5 -> 1.0,
//! so with a tone playing into the sink the peak must halve and recover.
//! Usage: RADIO_PANEL_SINK=radio-dev cargo run --example pw_spike2

use std::cell::{Cell, RefCell};
use std::ffi::CString;
use std::io::Cursor;
use std::rc::Rc;
use std::time::Duration;

use pipewire as pw;
use pw::spa;
use spa::param::ParamType;
use spa::pod::serialize::PodSerializer;
use spa::pod::{Object, Pod, Property, Value, ValueArray, deserialize::PodDeserializer};

fn volume_pod(volume: f32) -> Vec<u8> {
    let obj = Value::Object(Object {
        type_: spa::utils::SpaTypes::ObjectParamProps.as_raw(),
        id: ParamType::Props.as_raw(),
        properties: vec![Property::new(
            spa::sys::SPA_PROP_channelVolumes,
            Value::ValueArray(ValueArray::Float(vec![volume, volume])),
        )],
    });
    PodSerializer::serialize(Cursor::new(Vec::new()), &obj).expect("serialize").0.into_inner()
}

fn set_volume(node: &pw::node::Node, volume: f32) {
    let bytes = volume_pod(volume);
    node.set_param(ParamType::Props, 0, Pod::from_bytes(&bytes).expect("pod"));
}

fn channel_volumes(pod: &Pod) -> Option<Vec<f32>> {
    let (_, Value::Object(obj)) = PodDeserializer::deserialize_any_from(pod.as_bytes()).ok()? else {
        return None;
    };
    obj.properties.into_iter().find_map(|p| match p.value {
        Value::ValueArray(ValueArray::Float(v)) if p.key == spa::sys::SPA_PROP_channelVolumes => Some(v),
        _ => None,
    })
}

/// Subscribe to Props and print every channelVolumes update under `label`.
fn watch_volume(node: &pw::node::Node, label: &'static str) -> pw::node::NodeListener {
    let listener = node
        .add_listener_local()
        .param(move |_, _, _, _, pod| {
            if let Some(v) = pod.and_then(channel_volumes) {
                println!("{label} {v:?}");
            }
        })
        .register();
    node.subscribe_params(&[ParamType::Props]);
    listener
}

fn main() {
    let sink_name = std::env::var("RADIO_PANEL_SINK").unwrap_or_else(|_| "radio-dev".to_string());
    if sink_name == "radio" {
        eprintln!("error: RADIO_PANEL_SINK=radio is the real sink; use 'radio-dev' for dev runs");
        std::process::exit(2);
    }
    let out_name = format!("{sink_name}-out");
    let lb_name = "output.radio-local-dev";

    pw::init();
    let mainloop = pw::main_loop::MainLoopRc::new(None).expect("mainloop");
    let context = pw::context::ContextRc::new(&mainloop, None).expect("context");
    let core = context.connect_rc(None).expect("connect");
    let registry = core.get_registry_rc().expect("registry");

    let sink = core
        .create_object::<pw::node::Node>(
            "adapter",
            &pw::properties::properties! {
                "factory.name" => "support.null-audio-sink",
                "node.name" => sink_name.as_str(),
                "node.description" => "Radio Stream (dev)",
                "media.class" => "Audio/Sink",
                "audio.position" => "FL,FR",
                "monitor.channel-volumes" => "true",
            },
        )
        .expect("create sink");
    let _sink_listener = watch_volume(&sink, "SINK_VOL");
    let _out = core
        .create_object::<pw::node::Node>(
            "adapter",
            &pw::properties::properties! {
                "factory.name" => "support.null-audio-sink",
                "node.name" => out_name.as_str(),
                "media.class" => "Audio/Sink",
                "audio.position" => "FL,FR",
            },
        )
        .expect("create out sink");

    let args = CString::new(format!(
        "{{ capture.props={{ node.name=\"input.radio-local-dev\" target.object=\"{sink_name}\" stream.capture.sink=true node.passive=true }} playback.props={{ node.name=\"{lb_name}\" target.object=\"{out_name}\" state.restore-props=false }} }}"
    ))
    .expect("args");
    // Loads libpipewire-module-loopback into this process; both strings are valid C strings.
    let module = unsafe {
        pw::sys::pw_context_load_module(
            context.as_raw_ptr(),
            c"libpipewire-module-loopback".as_ptr(),
            args.as_ptr(),
            std::ptr::null_mut(),
        )
    };
    assert!(!module.is_null(), "loopback module failed to load");

    // Local volume: bind the loopback's playback node when it shows up in the registry.
    let loopback: Rc<RefCell<Option<(pw::node::Node, pw::node::NodeListener)>>> = Rc::default();
    let _registry_listener = registry
        .add_listener_local()
        .global({
            let (registry, loopback) = (registry.clone(), loopback.clone());
            move |global| {
                let is_loopback = global.type_ == pw::types::ObjectType::Node
                    && global.props.and_then(|p| p.get("node.name")) == Some(lb_name);
                if is_loopback && loopback.borrow().is_none() {
                    let node: pw::node::Node = registry.bind(global).expect("bind loopback node");
                    let listener = watch_volume(&node, "LOOPBACK_VOL");
                    set_volume(&node, 0.25);
                    *loopback.borrow_mut() = Some((node, listener));
                }
            }
        })
        .register();

    // Spectrum path: capture stream on the sink monitor, peak tracked per timer tick.
    let peak = Rc::new(Cell::new(0f32));
    let stream = pw::stream::StreamBox::new(
        &core,
        "radio-dev-capture",
        pw::properties::properties! {
            *pw::keys::MEDIA_TYPE => "Audio",
            *pw::keys::MEDIA_CATEGORY => "Capture",
            *pw::keys::MEDIA_ROLE => "Music",
            "target.object" => sink_name.as_str(),
            *pw::keys::STREAM_CAPTURE_SINK => "true",
        },
    )
    .expect("stream");
    let _stream_listener = stream
        .add_local_listener_with_user_data(peak.clone())
        .process(|stream, peak| {
            let Some(mut buffer) = stream.dequeue_buffer() else { return };
            let Some(data) = buffer.datas_mut().first_mut() else { return };
            let size = data.chunk().size() as usize;
            if let Some(bytes) = data.data() {
                let max = bytes[..size.min(bytes.len())]
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|b| f32::from_le_bytes(*b).abs())
                    .fold(peak.get(), f32::max);
                peak.set(max);
            }
        })
        .register()
        .expect("stream listener");
    let mut audio_info = spa::param::audio::AudioInfoRaw::new();
    audio_info.set_format(spa::param::audio::AudioFormat::F32LE);
    let format = PodSerializer::serialize(
        Cursor::new(Vec::new()),
        &Value::Object(Object {
            type_: spa::utils::SpaTypes::ObjectParamFormat.as_raw(),
            id: ParamType::EnumFormat.as_raw(),
            properties: audio_info.into(),
        }),
    )
    .expect("format")
    .0
    .into_inner();
    // No RT_PROCESS: process() runs on this loop, so the Rc<Cell> peak is safe.
    stream
        .connect(
            spa::utils::Direction::Input,
            None,
            pw::stream::StreamFlags::AUTOCONNECT | pw::stream::StreamFlags::MAP_BUFFERS,
            &mut [Pod::from_bytes(&format).expect("format pod")],
        )
        .expect("connect stream");

    let tick = Cell::new(0u32);
    let timer = mainloop.loop_().add_timer({
        let mainloop = mainloop.clone();
        move |_| {
            tick.set(tick.get() + 1);
            println!("t={:.1}s PEAK {:.3}", tick.get() as f32 / 2.0, peak.replace(0.0));
            match tick.get() {
                6 => set_volume(&sink, 0.5),
                12 => set_volume(&sink, 1.0),
                18 => mainloop.quit(),
                _ => {}
            }
        }
    });
    let half = Duration::from_millis(500);
    timer.update_timer(Some(half), Some(half)).into_sync_result().expect("arm timer");

    mainloop.run();
    unsafe { pw::sys::pw_impl_module_destroy(module) };
    // exit() skips destructors; the server drops our nodes when the connection closes.
    std::process::exit(0);
}
