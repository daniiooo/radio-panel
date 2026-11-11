//! Headless PipeWire spike: two null sinks + in-process loopback module,
//! all three confirmed through the registry, then clean exit.

use std::cell::Cell;
use std::ffi::CString;
use std::io::Write;
use std::rc::Rc;
use std::time::{Duration, Instant};

use pipewire as pw;

fn flush() {
    let _ = std::io::stdout().flush();
}

fn main() {
    let s = std::env::var("RADIO_PANEL_SINK").unwrap_or_else(|_| "radio-dev".to_string());
    if s == "radio" {
        eprintln!("error: RADIO_PANEL_SINK=radio is the real sink; use 'radio-dev' for dev runs");
        std::process::exit(2);
    }
    let stay: u64 = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(2);
    let s_out = format!("{s}-out");
    let lb_name = "output.radio-local-dev";

    pw::init();
    let mainloop = pw::main_loop::MainLoopRc::new(None).expect("mainloop");
    let context = pw::context::ContextRc::new(&mainloop, None).expect("context");
    let core = context.connect_rc(None).expect("connect");
    let registry = core.get_registry().expect("registry");

    let _sink_a = core
        .create_object::<pw::node::Node>(
            "adapter",
            &pw::properties::properties! {
                "factory.name" => "support.null-audio-sink",
                "node.name" => s.as_str(),
                "node.description" => "Radio Stream (dev)",
                "media.class" => "Audio/Sink",
                "audio.position" => "FL,FR",
                "monitor.channel-volumes" => "true",
            },
        )
        .expect("create sink A");
    let _sink_b = core
        .create_object::<pw::node::Node>(
            "adapter",
            &pw::properties::properties! {
                "factory.name" => "support.null-audio-sink",
                "node.name" => s_out.as_str(),
                "node.description" => "Radio dev loopback target",
            },
        )
        .expect("create sink B");

    let args = CString::new(format!(
        "{{ node.description=\"radio-local-dev\" capture.props={{ node.name=\"input.radio-local-dev\" target.object=\"{s}\" stream.capture.sink=true node.passive=true }} playback.props={{ node.name=\"{lb_name}\" target.object=\"{s_out}\" node.dont-reconnect=false }} }}"
    ))
    .expect("args cstring");
    // One FFI call: loads libpipewire-module-loopback in-process; both
    // string args are valid NUL-terminated c-strings, context is connected.
    let module = unsafe {
        pw::sys::pw_context_load_module(
            context.as_raw_ptr(),
            c"libpipewire-module-loopback".as_ptr(),
            args.as_ptr(),
            std::ptr::null_mut(),
        )
    };
    if module.is_null() {
        println!("LOOPBACK_FAIL");
        flush();
        std::process::exit(1);
    }

    let sink_ok = Rc::new(Cell::new(false));
    let out_ok = Rc::new(Cell::new(false));
    let lb_ok = Rc::new(Cell::new(false));
    let all_at = Rc::new(Cell::new(None));
    let done = Rc::new(Cell::new(false));

    // Listener must outlive run(): the spa_hook it registers points into it.
    let _listener = registry
        .add_listener_local()
        .global({
            let (sink_ok_c, out_ok_c, lb_ok_c) = (sink_ok.clone(), out_ok.clone(), lb_ok.clone());
            let (s_c, s_out_c) = (s.clone(), s_out.clone());
            move |global: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>| {
                if global.type_ != pw::types::ObjectType::Node {
                    return;
                }
                let Some(name) = global.props.and_then(|p| p.get("node.name")) else {
                    return;
                };
                if name == s_c && !sink_ok_c.get() {
                    sink_ok_c.set(true);
                    println!("SINK_OK id={}", global.id);
                    flush();
                } else if name == s_out_c && !out_ok_c.get() {
                    out_ok_c.set(true);
                    println!("OUT_OK id={}", global.id);
                    flush();
                } else if name == lb_name && !lb_ok_c.get() {
                    lb_ok_c.set(true);
                    println!("LOOPBACK_OK id={}", global.id);
                    flush();
                }
            }
        })
        .register();

    let timer = mainloop
        .loop_()
        .add_timer({
            let (sink_ok_c, out_ok_c, lb_ok_c) = (sink_ok.clone(), out_ok.clone(), lb_ok.clone());
            let (all_at_c, done_c) = (all_at.clone(), done.clone());
            let (s_c, s_out_c, lb_c) = (s.clone(), s_out.clone(), lb_name.to_string());
            let ml = mainloop.clone();
            let start = Instant::now();
            move |_expirations: u64| {
                let now = Instant::now();
                if sink_ok_c.get() && out_ok_c.get() && lb_ok_c.get() {
                    if all_at_c.get().is_none() {
                        all_at_c.set(Some(now));
                    }
                    if now.duration_since(all_at_c.get().unwrap())
                        >= Duration::from_secs(stay)
                    {
                        println!("DONE");
                        flush();
                        done_c.set(true);
                        ml.quit();
                    }
                } else if now.duration_since(start) >= Duration::from_secs(6) {
                    let mut missing = Vec::new();
                    if !sink_ok_c.get() {
                        missing.push(s_c.as_str());
                    }
                    if !out_ok_c.get() {
                        missing.push(s_out_c.as_str());
                    }
                    if !lb_ok_c.get() {
                        missing.push(lb_c.as_str());
                    }
                    println!("TIMEOUT missing={}", missing.join(" "));
                    flush();
                    ml.quit();
                }
            }
        });
    timer
        .update_timer(Some(Duration::from_millis(200)), Some(Duration::from_millis(200)))
        .into_sync_result()
        .expect("arm timer");

    mainloop.run();
    let code = if done.get() { 0 } else { 1 };
    // The load_module return value is a reference we own; destroy releases it.
    unsafe {
        pw::sys::pw_impl_module_destroy(module);
    }
    // exit() skips destructors and pw::deinit(); the server drops our nodes
    // (no object.linger) when the connection closes.
    std::process::exit(code);
}
