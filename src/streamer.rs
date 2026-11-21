//! The stream to Icecast: one thread supervises ffmpeg. Commands go in over a channel,
//! events come out through a callback that runs on that thread.

use std::io::Read;
use std::process::{Child, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::ffmpeg;

#[derive(Default)]
pub struct Progress {
    linebuf: String,
    /// Scrubbed log lines, last 2000 chars.
    pub last_output: String,
    bytes: Option<u64>,
    time: f64,
}

#[derive(Debug, PartialEq)]
pub enum ProgressEvent {
    /// Upload rate in kbit/s.
    Rate(f64),
    /// ffmpeg completed a telemetry record: output is flowing.
    Record,
}

impl Progress {
    pub fn feed(&mut self, chunk: &str, now: f64, scrub: impl Fn(&str) -> String) -> Vec<ProgressEvent> {
        self.linebuf.push_str(chunk);
        let mut events = Vec::new();
        while let Some(pos) = self.linebuf.find('\n') {
            let tail = self.linebuf.split_off(pos + 1);
            let line = std::mem::take(&mut self.linebuf);
            self.linebuf = tail;
            let line = line[..pos].trim();
            if line.is_empty() {
                continue;
            }
            let (key, value) = match line.find('=') {
                Some(eq) => (&line[..eq], &line[eq + 1..]),
                None => ("", ""),
            };
            let is_telemetry = line.contains('=') && !key.contains(' ');
            if is_telemetry {
                if key == "total_size" {
                    if !value.is_empty()
                        && value.chars().all(|c| c.is_ascii_digit())
                        && let Ok(size) = value.parse::<u64>()
                    {
                        if let Some(prev) = self.bytes
                            && now > self.time
                        {
                            let kbps = (size as f64 - prev as f64) * 8.0 / (now - self.time) / 1000.0;
                            events.push(ProgressEvent::Rate(kbps.max(0.0)));
                        }
                        self.bytes = Some(size);
                        self.time = now;
                    }
                } else if key == "progress" && (value == "continue" || value == "end") {
                    events.push(ProgressEvent::Record);
                }
            } else {
                self.last_output.push_str(&scrub(line));
                self.last_output.push('\n');
                let count = self.last_output.chars().count();
                if count > 2000 {
                    let keep = count - 2000;
                    let byte_idx = self.last_output.char_indices().nth(keep).unwrap().0;
                    self.last_output = self.last_output.split_off(byte_idx);
                }
            }
        }
        events
    }

    pub fn stalled(&self, now: f64) -> bool {
        self.bytes.is_some() && now - self.time > 1.5
    }
}

pub enum Command {
    /// Start streaming with this configuration. A running ffmpeg is kept: send `Stop`
    /// first to apply changed settings.
    GoLive(Box<Config>),
    Stop,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// The status line. `wanted` is false while off air and after giving up.
    State { text: String, live: bool, error: bool, wanted: bool },
    /// Upload rate in kbit/s; 0.0 while the upload is stalled.
    Rate(f64),
    /// Icecast is receiving the stream, or no longer is. A started ffmpeg is not proof;
    /// its first complete progress record is.
    Connected(bool),
    /// Scrubbed detail of the last failure; empty after a new go-live.
    Diagnostic(String),
}

enum Msg {
    Command(Command),
    Quit,
    /// Output of the ffmpeg run with this number.
    Output(u64, String),
    Exited(u64),
}

pub struct Handle {
    tx: Sender<Msg>,
    thread: Option<JoinHandle<()>>,
}

impl Handle {
    pub fn send(&self, command: Command) {
        let _ = self.tx.send(Msg::Command(command));
    }
}

impl Drop for Handle {
    /// Ends ffmpeg and the thread.
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::Quit);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// `sink` is the sink whose monitor ffmpeg encodes.
pub fn spawn(sink: &str, on_event: impl Fn(Event) + Send + 'static) -> Handle {
    let (tx, rx) = channel();
    let supervisor = Supervisor {
        sink: sink.to_string(),
        on_event,
        tx: tx.clone(),
        clock: Instant::now(),
        config: None,
        want_live: false,
        connected: false,
        connected_at: Instant::now(),
        restart_delay: FIRST_RETRY,
        retry_at: None,
        child: None,
        run: 0,
        started: Instant::now(),
        progress: Progress::default(),
    };
    Handle { tx, thread: Some(std::thread::spawn(move || supervisor.run(rx))) }
}

const FIRST_RETRY: Duration = Duration::from_secs(2);
const LAST_RETRY: Duration = Duration::from_secs(30);
const STALL_PERIOD: Duration = Duration::from_secs(1);
/// A run that was connected this long had a working encoder, password and mount, so
/// its end is a lost connection and is retried whatever ffmpeg printed. A refused
/// password also counts as connected for a moment: ffmpeg sends audio without
/// waiting for the server's answer (measured: under 0.5 s against a local Icecast
/// and the ingest proxy). Raise this if a server takes longer to refuse.
const STABLE_RUN: Duration = Duration::from_secs(10);

struct Supervisor<F> {
    sink: String,
    on_event: F,
    tx: Sender<Msg>,
    /// Zero of the time `progress` is fed.
    clock: Instant,
    config: Option<Box<Config>>,
    want_live: bool,
    connected: bool,
    /// When `connected` last became true.
    connected_at: Instant,
    restart_delay: Duration,
    retry_at: Option<Instant>,
    child: Option<Child>,
    /// Number of the current ffmpeg run; output of earlier runs is dropped.
    run: u64,
    started: Instant,
    progress: Progress,
}

impl<F: Fn(Event)> Supervisor<F> {
    fn run(mut self, rx: Receiver<Msg>) {
        let mut stall_check = Instant::now() + STALL_PERIOD;
        loop {
            let deadline = self.retry_at.map_or(stall_check, |retry| retry.min(stall_check));
            match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(Msg::Command(Command::GoLive(config))) => self.go_live(config),
                Ok(Msg::Command(Command::Stop)) => self.stop(),
                Ok(Msg::Output(run, chunk)) if run == self.run => self.output(&chunk),
                Ok(Msg::Exited(run)) if run == self.run => self.exited(),
                Ok(Msg::Output(..) | Msg::Exited(..)) | Err(RecvTimeoutError::Timeout) => {}
                Ok(Msg::Quit) | Err(RecvTimeoutError::Disconnected) => break,
            }
            let now = Instant::now();
            if self.retry_at.is_some_and(|at| now >= at) {
                self.retry_at = None;
                if self.want_live {
                    self.start();
                }
            }
            if now >= stall_check {
                stall_check = now + STALL_PERIOD;
                // ffmpeg blocks on socket backpressure, so progress going quiet means
                // the upload has stalled.
                if self.want_live && self.child.is_some() && self.progress.stalled(self.clock.elapsed().as_secs_f64()) {
                    (self.on_event)(Event::Rate(0.0));
                }
            }
        }
        self.end_process();
    }

    fn state(&self, text: impl Into<String>, live: bool, error: bool) {
        (self.on_event)(Event::State { text: text.into(), live, error, wanted: self.want_live });
    }

    fn set_connected(&mut self, connected: bool) {
        if self.connected != connected {
            self.connected = connected;
            if connected {
                self.connected_at = Instant::now();
            }
            (self.on_event)(Event::Connected(connected));
        }
    }

    fn go_live(&mut self, config: Box<Config>) {
        self.retry_at = None;
        let missing = if config.get("server", "host").trim().is_empty() {
            Some("No server host configured")
        } else if config.get("server", "mount").trim().trim_start_matches('/').is_empty() {
            Some("No stream mount configured")
        } else {
            None
        };
        if let Some(text) = missing {
            self.want_live = false;
            self.state(text, false, true);
            return;
        }
        self.config = Some(config);
        self.want_live = true;
        self.restart_delay = FIRST_RETRY;
        (self.on_event)(Event::Diagnostic(String::new()));
        self.start();
    }

    fn stop(&mut self) {
        self.want_live = false;
        self.retry_at = None;
        self.set_connected(false);
        self.end_process();
        self.state("Off air", false, false);
    }

    fn start(&mut self) {
        let Some(config) = &self.config else { return };
        if self.child.is_some() {
            return;
        }
        let args = ffmpeg::stream_command(config, &self.sink);
        self.set_connected(false);
        self.progress = Progress::default();
        self.state("Connecting...", false, false);
        self.run += 1;
        match spawn_ffmpeg(&args, self.run, self.tx.clone()) {
            Ok(child) => {
                self.child = Some(child);
                self.started = Instant::now();
            }
            Err(error) => self.schedule_retry(&format!("ffmpeg error: could not start: {error}"), false),
        }
    }

    fn output(&mut self, chunk: &str) {
        let (Some(config), Some(_)) = (&self.config, &self.child) else { return };
        let now = self.clock.elapsed().as_secs_f64();
        for event in self.progress.feed(chunk, now, |line| ffmpeg::scrub_diagnostic(config, line)) {
            match event {
                ProgressEvent::Rate(kbps) => (self.on_event)(Event::Rate(kbps)),
                ProgressEvent::Record if self.want_live && !self.connected => {
                    self.set_connected(true);
                    self.state("LIVE", true, false);
                }
                ProgressEvent::Record => {}
            }
        }
    }

    fn exited(&mut self) {
        let Some(mut child) = self.child.take() else { return };
        let code = child.wait().ok().and_then(|status| status.code()).unwrap_or(-1);
        let lost = self.connected && self.connected_at.elapsed() >= STABLE_RUN;
        self.set_connected(false);
        if !self.want_live {
            return;
        }
        // A long stable run earns a fresh, quick retry schedule.
        if self.started.elapsed() > LAST_RETRY {
            self.restart_delay = FIRST_RETRY;
        }
        let reason = match self.progress.last_output.trim() {
            "" => format!("ffmpeg exited (code {code})"),
            output => output.to_string(),
        };
        self.schedule_retry(&reason, lost);
    }

    /// `lost`: the run had been connected for `STABLE_RUN`.
    fn schedule_retry(&mut self, reason: &str, lost: bool) {
        let Some(config) = &self.config else { return };
        if !self.want_live || self.retry_at.is_some() {
            return;
        }
        let (summary, retry) = if lost { ("connection lost", true) } else { ffmpeg::classify_diagnostic(reason, &self.sink) };
        let detail = ffmpeg::scrub_diagnostic(config, reason);
        self.set_connected(false);
        if !retry {
            self.want_live = false;
            self.state(format!("Streaming stopped: {summary}"), false, true);
        } else if summary == "stream process exited" {
            self.state(format!("Reconnecting... ({})", ffmpeg::final_diagnostic_line(&detail)), false, true);
        } else {
            self.state(format!("Reconnecting... ({summary})"), false, true);
        }
        (self.on_event)(Event::Diagnostic(detail));
        if retry {
            self.retry_at = Some(Instant::now() + self.restart_delay);
            self.restart_delay = (self.restart_delay * 2).min(LAST_RETRY);
        }
    }

    fn end_process(&mut self) {
        if let Some(mut child) = self.child.take() {
            ffmpeg::terminate(&mut child, Duration::from_secs(2));
        }
    }
}

/// Start ffmpeg with stdout and stderr merged into one pipe. A thread forwards the
/// output, then reports the end of the pipe: the process has exited.
fn spawn_ffmpeg(args: &[String], run: u64, tx: Sender<Msg>) -> std::io::Result<Child> {
    let (mut reader, writer) = std::io::pipe()?;
    let mut command = std::process::Command::new(&args[0]);
    command.args(&args[1..]).stdin(Stdio::null()).stdout(writer.try_clone()?).stderr(writer);
    ffmpeg::die_with_parent(&mut command);
    ffmpeg::no_fallback(&mut command);
    let child = command.spawn()?;
    drop(command); // closes our copies of the pipe's write end, so the reader sees its end
    std::thread::spawn(move || {
        let mut buffer = [0u8; 4096];
        while let Ok(count) = reader.read(&mut buffer)
            && count > 0
        {
            let _ = tx.send(Msg::Output(run, String::from_utf8_lossy(&buffer[..count]).into_owned()));
        }
        let _ = tx.send(Msg::Exited(run));
    });
    Ok(child)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::pw::tests::{pactl, wait_for};

    fn id(s: &str) -> String {
        s.to_string()
    }

    pub(crate) const SINK: &str = "radio-dev-stream";
    pub(crate) const PORT: u16 = 18010;
    /// The ingest harness: its fake master and the proxy.
    pub(crate) const MASTER_PORT: u16 = 18011;
    pub(crate) const PROXY_PORT: u16 = 18012;
    pub(crate) const PASSWORD: &str = "test-source-pw";
    pub(crate) const DJ_PASSWORD: &str = "test-dj-pw";

    /// The live tests share the sink and count this process's ffmpeg children.
    pub(crate) static LIVE: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// A test server; killed on drop.
    pub(crate) struct Server(Child);

    fn listening(port: u16) -> bool {
        std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
    }

    /// A local Icecast on `port`.
    pub(crate) fn start_icecast(dir: &std::path::Path, port: u16) -> Server {
        let xml = format!(
            "<icecast><location>test</location><admin>test@localhost</admin><hostname>localhost</hostname>
            <limits><sources>2</sources></limits>
            <authentication><source-password>{PASSWORD}</source-password>
            <admin-user>admin</admin-user><admin-password>{PASSWORD}</admin-password></authentication>
            <listen-socket><port>{port}</port><bind-address>127.0.0.1</bind-address></listen-socket>
            <paths><basedir>/usr/share/icecast</basedir><logdir>{}</logdir>
            <webroot>/usr/share/icecast/web</webroot><adminroot>/usr/share/icecast/admin</adminroot></paths>
            <logging><accesslog>access.log</accesslog><errorlog>error.log</errorlog></logging></icecast>",
            dir.display()
        );
        let file = dir.join(format!("icecast-{port}.xml"));
        std::fs::write(&file, xml).unwrap();
        let child = std::process::Command::new("icecast").arg("-c").arg(file).stdout(Stdio::null()).spawn().expect("icecast");
        wait_for("icecast listening", || listening(port));
        Server(child)
    }

    /// The real ingest proxy code (`tests/ingest`, built here) on `PROXY_PORT`,
    /// forwarding to its fake master on `MASTER_PORT`. Its one DJ is `dj`.
    pub(crate) fn start_ingest(dir: &std::path::Path) -> Server {
        let binary = dir.join("ingest-proxy-harness");
        let built = std::process::Command::new("go")
            .args(["build", "-o"])
            .arg(&binary)
            .arg(".")
            .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/ingest"))
            .status()
            .expect("go");
        assert!(built.success(), "go build of tests/ingest failed");
        let child = std::process::Command::new(binary)
            .args([&PROXY_PORT.to_string(), &MASTER_PORT.to_string(), DJ_PASSWORD])
            .stderr(Stdio::null())
            .spawn()
            .expect("ingest harness");
        wait_for("ingest listening", || listening(PROXY_PORT));
        Server(child)
    }

    impl Drop for Server {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    /// Response to a GET on `port`; empty if it cannot be reached.
    pub(crate) fn http_get(port: u16, path: &str) -> String {
        use std::io::Write;
        let mut body = String::new();
        if let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", port)) {
            let _ = stream.write_all(format!("GET {path} HTTP/1.0\r\n\r\n").as_bytes());
            let _ = stream.read_to_string(&mut body);
        }
        body
    }

    /// Mounts the Icecast on `PORT` lists.
    pub(crate) fn icecast_status() -> String {
        http_get(PORT, "/status-json.xsl")
    }

    /// Whether the ingest harness is forwarding a source to its master.
    fn master_live() -> bool {
        http_get(MASTER_PORT, "/status").contains("live=1")
    }

    /// Pids of the ffmpeg processes this test process started.
    pub(crate) fn ffmpeg_pids() -> Vec<String> {
        let own = std::process::id().to_string();
        let out = std::process::Command::new("pgrep").args(["-P", &own, "-x", "ffmpeg"]).output().expect("pgrep");
        String::from_utf8_lossy(&out.stdout).lines().map(str::to_string).collect()
    }

    /// Wait up to `seconds` for the first event `wanted` accepts, ignoring the others.
    pub(crate) fn wait_event<E>(rx: &Receiver<E>, seconds: u64, what: &str, wanted: impl Fn(&E) -> bool) -> E {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        loop {
            match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(event) if wanted(&event) => return event,
                Ok(_) => {}
                Err(_) => panic!("timed out waiting for {what}"),
            }
        }
    }

    fn wait_state(rx: &Receiver<Event>, seconds: u64, start: &str, live: bool, error: bool, wanted: bool) -> String {
        let expected = |e: &Event| matches!(e, Event::State { text, live: l, error: r, wanted: w }
            if text.starts_with(start) && (*l, *r, *w) == (live, error, wanted));
        let Event::State { text, .. } = wait_event(rx, seconds, start, expected) else { unreachable!() };
        text
    }

    /// One test for the live servers: Icecast on `PORT` plus a silent sink monitor.
    /// Silent: nothing plays into the sink and it is not linked to an output.
    #[test]
    fn streams_to_local_icecast_and_reconnects() {
        let _live = LIVE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let dir = std::env::temp_dir().join(format!("radio-panel-rs-streamer-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sink = crate::pw::spawn(SINK, |_| {});
        wait_for("sink monitor visible to pulse", || pactl(&["list", "short", "sources"]).contains("radio-dev-stream.monitor"));

        let mut config = Config::load(&dir.join("none.conf")).unwrap();
        for (key, value) in [("host", "127.0.0.1"), ("port", "18010"), ("mount", "/main.mp3"), ("source_password", PASSWORD)] {
            config.set("server", key, value);
        }
        let mut wrongpw = config.clone();
        wrongpw.set("server", "source_password", "wrong-pw");
        let (tx, events) = channel();
        let streamer = spawn(SINK, move |event| drop(tx.send(event)));

        // Nothing to connect to: no server configured stops before ffmpeg starts.
        let mut no_host = config.clone();
        no_host.set("server", "host", " ");
        streamer.send(Command::GoLive(Box::new(no_host)));
        wait_state(&events, 15, "No server host configured", false, true, false);
        let mut no_mount = config.clone();
        no_mount.set("server", "mount", "/");
        streamer.send(Command::GoLive(Box::new(no_mount)));
        wait_state(&events, 15, "No stream mount configured", false, true, false);
        assert!(ffmpeg_pids().is_empty());

        // The server is down: refused, then retried with backoff, and it goes live
        // as soon as the server is back.
        streamer.send(Command::GoLive(Box::new(config.clone())));
        wait_state(&events, 15, "Connecting...", false, false, true);
        wait_state(&events, 15, "Reconnecting... (network connection failed)", false, true, true);
        let Event::Diagnostic(detail) = wait_event(&events, 15, "failure details", |e| matches!(e, Event::Diagnostic(d) if !d.is_empty()))
        else {
            unreachable!()
        };
        assert!(!detail.contains("wrong-pw"), "{detail}");
        let icecast = start_icecast(&dir, PORT);
        wait_event(&events, 15, "connected", |e| *e == Event::Connected(true));
        wait_state(&events, 15, "LIVE", true, false, true);
        wait_event(&events, 15, "an upload rate", |e| matches!(e, Event::Rate(kbps) if *kbps > 100.0));
        assert!(icecast_status().contains("/main.mp3"));

        // A second go-live keeps the running ffmpeg.
        let pids = ffmpeg_pids();
        assert_eq!(pids.len(), 1);
        streamer.send(Command::GoLive(Box::new(config.clone())));
        wait_event(&events, 15, "details cleared", |e| *e == Event::Diagnostic(String::new()));
        assert_eq!(ffmpeg_pids(), pids);

        // A frozen ffmpeg reports no progress: the rate drops to zero, then resumes.
        let signal = |name: &str| drop(std::process::Command::new("kill").args([name, &pids[0]]).status());
        signal("-STOP");
        wait_event(&events, 15, "a stalled upload", |e| *e == Event::Rate(0.0));
        signal("-CONT");
        wait_event(&events, 15, "the upload resuming", |e| matches!(e, Event::Rate(kbps) if *kbps > 100.0));

        // Off air ends ffmpeg and frees the mount; a wrong password is not retried.
        streamer.send(Command::Stop);
        wait_event(&events, 15, "disconnected", |e| *e == Event::Connected(false));
        wait_state(&events, 15, "Off air", false, false, false);
        assert!(ffmpeg_pids().is_empty());
        wait_for("mount is gone", || !icecast_status().contains("/main.mp3"));
        streamer.send(Command::GoLive(Box::new(wrongpw)));
        wait_event(&events, 15, "disconnected", |e| *e == Event::Connected(false));
        wait_state(&events, 15, "Streaming stopped: ffmpeg encoder unavailable", false, true, false);
        let Event::Diagnostic(detail) = wait_event(&events, 15, "failure details", |e| matches!(e, Event::Diagnostic(d) if !d.is_empty()))
        else {
            unreachable!()
        };
        assert!(!detail.contains("wrong-pw"), "{detail}");

        // The server dies before the run counts as stable: the broken pipe block names
        // the mp3 encoder, as after a wrong password, so it is not retried.
        streamer.send(Command::GoLive(Box::new(config.clone())));
        wait_event(&events, 15, "connected", |e| *e == Event::Connected(true));
        wait_state(&events, 15, "LIVE", true, false, true);
        drop(icecast);
        wait_state(&events, 15, "Streaming stopped: ffmpeg encoder unavailable", false, true, false);

        // The server dies under a stable run: retried until the server is back, with
        // no new go-live.
        let icecast = start_icecast(&dir, PORT);
        streamer.send(Command::GoLive(Box::new(config.clone())));
        wait_event(&events, 15, "connected", |e| *e == Event::Connected(true));
        std::thread::sleep(STABLE_RUN + Duration::from_secs(1));
        drop(icecast);
        wait_state(&events, 15, "Reconnecting... (connection lost)", false, true, true);
        wait_state(&events, 15, "Reconnecting... (network connection failed)", false, true, true);
        let icecast = start_icecast(&dir, PORT);
        wait_event(&events, 40, "reconnected", |e| *e == Event::Connected(true));
        wait_for("mount is back", || icecast_status().contains("/main.mp3"));

        // Dropping the handle while live ends ffmpeg too.
        streamer.send(Command::Stop);
        wait_state(&events, 15, "Off air", false, false, false);
        streamer.send(Command::GoLive(Box::new(config.clone())));
        wait_event(&events, 15, "connected", |e| *e == Event::Connected(true));
        drop(streamer);
        assert!(ffmpeg_pids().is_empty());

        // The sink goes away while live: ffmpeg ends instead of capturing the default
        // sink's monitor, and without the sink no attempt gets on air again.
        let (tx, events) = channel();
        let streamer = spawn(SINK, move |event| drop(tx.send(event)));
        streamer.send(Command::GoLive(Box::new(config)));
        wait_event(&events, 15, "connected", |e| *e == Event::Connected(true));
        drop(sink);
        wait_event(&events, 15, "disconnected", |e| *e == Event::Connected(false));
        let until = Instant::now() + Duration::from_secs(6);
        while let Ok(event) = events.recv_timeout(until.saturating_duration_since(Instant::now())) {
            assert_ne!(event, Event::Connected(true), "on air without the sink");
        }
        drop(streamer);
        assert!(ffmpeg_pids().is_empty());

        drop(icecast);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go-live as the station sees it: the real ingest proxy code in front of a fake master.
    #[test]
    fn streams_to_ingest_proxy() {
        let _live = LIVE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let dir = std::env::temp_dir().join(format!("radio-panel-rs-ingest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let proxy = start_ingest(&dir);
        let sink = crate::pw::spawn(SINK, |_| {});
        wait_for("sink monitor visible to pulse", || pactl(&["list", "short", "sources"]).contains("radio-dev-stream.monitor"));

        let mut config = Config::load(&dir.join("none.conf")).unwrap();
        for (key, value) in [("host", "127.0.0.1"), ("port", "18012"), ("mount", "/main.mp3"), ("user", "dj"), ("source_password", DJ_PASSWORD)] {
            config.set("server", key, value);
        }
        let (tx, events) = channel();
        let streamer = spawn(SINK, move |event| drop(tx.send(event)));

        // The DJ's own account name: audio passes the proxy and reaches the master.
        streamer.send(Command::GoLive(Box::new(config.clone())));
        wait_event(&events, 15, "connected", |e| *e == Event::Connected(true));
        wait_state(&events, 15, "LIVE", true, false, true);
        wait_for("audio reaching the master", master_live);

        // Off air: the proxy drops the master connection five seconds later.
        streamer.send(Command::Stop);
        wait_state(&events, 15, "Off air", false, false, false);
        wait_for("master connection closed", || !master_live());

        // User `source` with `name|password`; the `|` travels percent-encoded.
        let mut compat = config.clone();
        compat.set("server", "user", "source");
        compat.set("server", "source_password", &format!("dj|{DJ_PASSWORD}"));
        streamer.send(Command::GoLive(Box::new(compat)));
        wait_state(&events, 15, "LIVE", true, false, true);
        wait_for("audio reaching the master", master_live);

        // An upload stalled for 20 s is kicked by the proxy. The run was stable, so the
        // streamer connects again by itself.
        let pids = ffmpeg_pids();
        assert_eq!(pids.len(), 1);
        let signal = |name: &str| drop(std::process::Command::new("kill").args([name, &pids[0]]).status());
        signal("-STOP");
        std::thread::sleep(Duration::from_secs(21));
        signal("-CONT");
        wait_state(&events, 15, "Reconnecting... (connection lost)", false, true, true);
        wait_state(&events, 15, "LIVE", true, false, true);
        wait_for("audio reaching the master", master_live);
        streamer.send(Command::Stop);
        wait_state(&events, 15, "Off air", false, false, false);
        wait_for("master connection closed", || !master_live());

        // A wrong password: the proxy resets the connection, which is not retried.
        // ffmpeg does not read the 401, so the same encoder message is all there is.
        let mut wrongpw = config.clone();
        wrongpw.set("server", "source_password", "wrong-pw");
        streamer.send(Command::GoLive(Box::new(wrongpw)));
        wait_state(&events, 15, "Streaming stopped: ffmpeg encoder unavailable", false, true, false);
        let Event::Diagnostic(detail) = wait_event(&events, 15, "failure details", |e| matches!(e, Event::Diagnostic(d) if !d.is_empty()))
        else {
            unreachable!()
        };
        assert!(!detail.contains("wrong-pw"), "{detail}");
        assert!(!master_live());

        drop(streamer);
        assert!(ffmpeg_pids().is_empty());
        drop(sink);
        drop(proxy);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn p1_partial_lines() {
        let mut p = Progress::default();
        assert_eq!(p.feed("total_size=1000\ntotal_si", 0.0, id), vec![]);
        assert_eq!(p.feed("ze=6000\n", 0.5, id), vec![ProgressEvent::Rate(80.0)]);
    }

    #[test]
    fn p2_blank_lines_skipped() {
        let mut p = Progress::default();
        assert_eq!(p.feed("\n  \n\r\n", 0.0, id), vec![]);
        assert_eq!(p.last_output, "");
    }

    #[test]
    fn p3_telemetry_vs_log() {
        let mut p = Progress::default();
        let out = p.feed(
            "frame=  100\nsize=    1024kB time=00:00:04.20\n[pulse @ 0x1] latency=5 ms\nConnection refused\n",
            0.0,
            id,
        );
        assert_eq!(out, vec![]);
        assert_eq!(
            p.last_output,
            "[pulse @ 0x1] latency=5 ms\nConnection refused\n"
        );
    }

    #[test]
    fn p4_rate_samples() {
        let mut p = Progress::default();
        assert_eq!(p.feed("total_size=1000\n", 1.0, id), vec![]);
        assert_eq!(p.feed("total_size=6000\n", 1.5, id), vec![ProgressEvent::Rate(80.0)]);
        assert_eq!(p.feed("total_size=1000\n", 2.0, id), vec![ProgressEvent::Rate(0.0)]);
        assert_eq!(p.feed("total_size=9000\n", 2.0, id), vec![]);
        assert_eq!(p.feed("total_size=N/A\n", 3.0, id), vec![]);
        assert_eq!(p.feed("total_size=\n", 3.0, id), vec![]);
        assert_eq!(p.feed("total_size=10000\n", 2.5, id), vec![ProgressEvent::Rate(16.0)]);
    }

    #[test]
    fn p5_record() {
        let mut p = Progress::default();
        assert_eq!(p.feed("progress=continue\n", 0.0, id), vec![ProgressEvent::Record]);
        assert_eq!(p.feed("progress=end\n", 0.0, id), vec![ProgressEvent::Record]);
        assert_eq!(p.feed("progress=whatever\n", 0.0, id), vec![]);
        assert_eq!(p.feed("total_size=0\n", 1.0, id), vec![]);
        assert_eq!(
            p.feed("total_size=5000\nprogress=continue\n", 1.5, id),
            vec![ProgressEvent::Rate(80.0), ProgressEvent::Record]
        );
    }

    #[test]
    fn p6_log_window() {
        let scrub = |l: &str| l.replace("secret", "***");
        let mut p = Progress::default();
        p.feed("pw secret\n", 0.0, scrub);
        assert_eq!(p.last_output, "pw ***\n");
        let chunk = "ąąąąąąąąą\n".repeat(300);
        p.feed(&chunk, 0.0, scrub);
        assert_eq!(p.last_output.chars().count(), 2000);
        assert!(p.last_output.ends_with("ą\n"));
        assert!(!p.last_output.contains("pw"));
    }

    #[test]
    fn p7_stalled() {
        let mut p = Progress::default();
        assert!(!p.stalled(100.0));
        p.feed("total_size=1\n", 10.0, id);
        assert!(!p.stalled(11.5));
        assert!(p.stalled(11.6));
    }
}
