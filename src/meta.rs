//! The track title: read from a watched file or a player (MPRIS), or set by hand, and pushed to Icecast
//! as stream metadata. One thread; commands go in over a channel, events come out
//! through a callback that runs on that thread.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use base64::Engine;
use notify::Watcher;

use crate::config::Config;
use crate::ffmpeg::quote;

pub fn read_track(path: &Path) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    // Some players start the file with a byte order mark; it is not whitespace.
    let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
    let first = text.split(['\n', '\r']).next().unwrap_or("").trim();
    if first.is_empty() {
        None
    } else {
        Some(first.to_string())
    }
}

pub fn watch_dir(path: &Path) -> Option<&Path> {
    path.ancestors()
        .skip(1)
        .map(|p| {
            if p.as_os_str().is_empty() {
                Path::new(".")
            } else {
                p
            }
        })
        .find(|p| p.is_dir())
}

pub enum Command {
    /// Settings were saved: use them and watch the configured track file.
    Configure(Box<Config>),
    /// The stream is connected, or no longer is. Metadata is sent only while it is.
    SetEnabled(bool),
    /// A title that overrides the track file. An empty one returns to the file.
    SetManualTitle(String),
    ReturnToAutomatic,
    /// Send the current title again, even if the server already has it.
    SendNow,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// The title in effect, `-` if there is none.
    Track(String),
    /// The status line.
    Status(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Outcome {
    Sent,
    /// Worth another attempt: no answer, or a 5xx.
    Retry,
    /// A 4xx: the same request would fail again.
    Failed,
}

enum Msg {
    Command(Command),
    Quit,
    FileChanged,
    /// A player's title changed; sent by the poller whose stop flag it carries.
    PlayerTitle(Arc<AtomicBool>, Option<String>),
    Done { request: u64, title: String, generation: u64, outcome: Outcome },
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
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::Quit);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub fn spawn(config: Box<Config>, on_event: impl Fn(Event) + Send + 'static) -> Handle {
    let (tx, rx) = channel();
    let pusher = Pusher {
        on_event,
        tx: tx.clone(),
        config,
        watcher: None,
        poller: None,
        enabled: false,
        automatic_title: None,
        manual_title: None,
        last_pushed: None,
        generation: 0,
        requests: 0,
        inflight: None,
        pending: false,
        pending_force: false,
        retry_index: 0,
        debounce_at: None,
        initial_at: None,
        retry_at: None,
        titles: None,
    };
    Handle { tx, thread: Some(std::thread::spawn(move || pusher.run(rx))) }
}

const RETRY_DELAYS: [Duration; 4] =
    [Duration::from_secs(2), Duration::from_secs(5), Duration::from_secs(10), Duration::from_secs(30)];
/// Editors and players write the track file in several steps.
const DEBOUNCE: Duration = Duration::from_millis(300);
/// Gives Icecast a moment to register the mount after the stream connected.
const INITIAL_PUSH: Duration = Duration::from_secs(2);
/// How often the player is asked for its title.
const POLL: Duration = Duration::from_secs(1);
/// The Python app had no limit; a request that hangs would block all later titles.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

struct Pusher<F> {
    on_event: F,
    tx: Sender<Msg>,
    config: Box<Config>,
    watcher: Option<notify::RecommendedWatcher>,
    /// Stop flag of the thread polling the player, when titles come from one.
    poller: Option<Arc<AtomicBool>>,
    enabled: bool,
    automatic_title: Option<String>,
    manual_title: Option<String>,
    last_pushed: Option<String>,
    /// Counts changes of the title and of `enabled`; the answer to a request from an
    /// earlier generation says nothing about the current title.
    generation: u64,
    requests: u64,
    /// Number of the request whose answer is awaited. One request at a time.
    inflight: Option<u64>,
    /// A push was asked for while a request was in flight.
    pending: bool,
    pending_force: bool,
    retry_index: usize,
    debounce_at: Option<Instant>,
    initial_at: Option<Instant>,
    retry_at: Option<Instant>,
    /// The sent-titles file of this connection, once a title was sent, and the last
    /// title written to it.
    titles: Option<(std::path::PathBuf, String)>,
}

/// Report the title of player `id` now and whenever it changes, until `stop` is set.
/// ponytail: polls once per `POLL` instead of following PropertiesChanged signals, so
/// a title arrives up to a second late; follow the signals if that is ever too slow.
fn poll_player(id: String, stop: Arc<AtomicBool>, tx: Sender<Msg>) {
    let mut connection = None;
    let mut last: Option<Option<String>> = None;
    while !stop.load(Ordering::Relaxed) {
        let title = player_title(&mut connection, &id);
        if last.as_ref() != Some(&title) {
            if tx.send(Msg::PlayerTitle(stop.clone(), title.clone())).is_err() {
                break;
            }
            last = Some(title);
        }
        std::thread::sleep(POLL);
    }
}

fn player_title(connection: &mut Option<zbus::blocking::Connection>, id: &str) -> Option<String> {
    if connection.is_none() {
        *connection = zbus::blocking::Connection::session().ok();
    }
    match connection.as_ref().map(|connection| crate::mpris::current_title(connection, id)) {
        Some(Ok(title)) => title,
        // The bus went away: connect again on the next round.
        Some(Err(_)) | None => {
            *connection = None;
            None
        }
    }
}

/// Whether `timer` is due; a due timer is cleared.
fn take_due(timer: &mut Option<Instant>, now: Instant) -> bool {
    let due = timer.is_some_and(|at| now >= at);
    if due {
        *timer = None;
    }
    due
}

impl<F: Fn(Event)> Pusher<F> {
    fn run(mut self, rx: Receiver<Msg>) {
        self.rewatch();
        self.status("Disabled");
        loop {
            let message = match [self.debounce_at, self.initial_at, self.retry_at].into_iter().flatten().min() {
                Some(at) => rx.recv_timeout(at.saturating_duration_since(Instant::now())),
                None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
            };
            match message {
                Ok(Msg::Command(Command::Configure(config))) => {
                    self.config = config;
                    self.rewatch();
                }
                Ok(Msg::Command(Command::SetEnabled(enabled))) => self.set_enabled(enabled),
                Ok(Msg::Command(Command::SetManualTitle(title))) => self.set_manual_title(title.trim()),
                Ok(Msg::Command(Command::ReturnToAutomatic)) => self.set_manual_title(""),
                Ok(Msg::Command(Command::SendNow)) => {
                    // Also outdates an in-flight request for the same title.
                    self.generation += 1;
                    self.retry_at = None;
                    self.retry_index = 0;
                    self.push_current(true);
                }
                Ok(Msg::FileChanged) => self.debounce_at = Some(Instant::now() + DEBOUNCE),
                // A poller stopped since may still report once.
                Ok(Msg::PlayerTitle(from, title)) => {
                    if self.poller.as_ref().is_some_and(|current| Arc::ptr_eq(current, &from)) {
                        self.set_automatic(title);
                    }
                }
                Ok(Msg::Done { request, title, generation, outcome }) => self.done(request, title, generation, outcome),
                Err(RecvTimeoutError::Timeout) => {}
                Ok(Msg::Quit) | Err(RecvTimeoutError::Disconnected) => break,
            }
            let now = Instant::now();
            if take_due(&mut self.debounce_at, now) {
                self.rewatch();
            }
            if take_due(&mut self.initial_at, now) {
                self.push_current(false);
            }
            if take_due(&mut self.retry_at, now) && self.enabled {
                self.push_current(true);
            }
        }
        self.stop_poller();
    }

    fn stop_poller(&mut self) {
        if let Some(stop) = self.poller.take() {
            stop.store(true, Ordering::Relaxed);
        }
    }

    fn status(&self, text: impl Into<String>) {
        (self.on_event)(Event::Status(text.into()));
    }

    fn effective_title(&self) -> Option<&String> {
        self.manual_title.as_ref().or(self.automatic_title.as_ref())
    }

    fn publish_track(&self) {
        (self.on_event)(Event::Track(self.effective_title().map_or("-", String::as_str).to_string()));
    }

    /// Watch the track file and read it. The nearest existing parent directory is
    /// watched too: the file may not exist yet, and replacing it ends its own watch.
    /// With a player configured, its poller reports the title instead.
    fn rewatch(&mut self) {
        self.watcher = None;
        self.stop_poller();
        let player = self.config.get("metadata", "player").trim();
        if !player.is_empty() {
            let stop = Arc::new(AtomicBool::new(false));
            let (id, flag, tx) = (player.to_string(), stop.clone(), self.tx.clone());
            std::thread::spawn(move || poll_player(id, flag, tx));
            self.poller = Some(stop);
            return;
        }
        // `~` as in the recording folder.
        let path = std::path::absolute(crate::recorder::expand_user(self.config.get("metadata", "track_file"))).unwrap_or_default();
        if let Some(dir) = watch_dir(&path) {
            let (tx, track) = (self.tx.clone(), path.clone());
            let watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                // Our own reads are reported as well; only changes count. The directory
                // watch reports every entry; only the file and its parents count.
                if let Ok(event) = event
                    && !event.kind.is_access()
                    && event.paths.iter().any(|changed| track.starts_with(changed))
                {
                    let _ = tx.send(Msg::FileChanged);
                }
            });
            if let Ok(mut watcher) = watcher {
                let _ = watcher.watch(dir, notify::RecursiveMode::NonRecursive);
                let _ = watcher.watch(&path, notify::RecursiveMode::NonRecursive);
                self.watcher = Some(watcher);
            }
        }
        self.set_automatic(read_track(&path));
    }

    fn set_automatic(&mut self, title: Option<String>) {
        if title == self.automatic_title {
            self.publish_track();
            return;
        }
        self.automatic_title = title;
        if self.manual_title.is_none() {
            self.effective_title_changed(false);
        }
    }

    fn set_enabled(&mut self, enabled: bool) {
        if self.enabled == enabled {
            return;
        }
        self.enabled = enabled;
        self.generation += 1;
        // Each connection gets its own file, like the recording.
        self.titles = None;
        self.initial_at = None;
        self.retry_at = None;
        self.pending = false;
        self.pending_force = false;
        if !enabled {
            self.inflight = None;
            self.status("Disabled");
            return;
        }
        self.last_pushed = None;
        self.retry_index = 0;
        self.status("Waiting for stream");
        self.initial_at = Some(Instant::now() + INITIAL_PUSH);
    }

    /// `title` is trimmed; empty means back to the track file.
    fn set_manual_title(&mut self, title: &str) {
        let manual = (!title.is_empty()).then(|| title.to_string());
        if manual == self.manual_title {
            return;
        }
        // Returning to the file is a sync point, even if its title equals one sent
        // before the override began.
        let force = manual.is_none();
        self.manual_title = manual;
        self.effective_title_changed(force);
    }

    fn effective_title_changed(&mut self, force: bool) {
        self.generation += 1;
        self.retry_at = None;
        self.retry_index = 0;
        self.publish_track();
        self.push_current(force);
    }

    fn push_current(&mut self, force: bool) {
        self.publish_track();
        if !self.enabled {
            self.status("Disabled");
            return;
        }
        let Some(title) = self.effective_title().cloned() else {
            self.status("Waiting for title");
            return;
        };
        if self.inflight.is_some() {
            self.pending = true;
            self.pending_force |= force;
            return;
        }
        if force {
            self.retry_at = None;
        } else if self.retry_at.is_some() {
            return;
        } else if self.last_pushed.as_ref() == Some(&title) {
            self.status("Metadata OK");
            return;
        }
        let host = self.config.get("server", "host").trim();
        if host.is_empty() {
            self.status("Metadata failed");
            return;
        }
        // The mount must be the stream's path, leading slash included: ingest
        // matches the update to the source by it (docs/ingest-compat.md).
        let mount = format!("/{}", self.config.get("server", "mount").trim_start_matches('/'));
        let url = format!(
            "http://{host}:{}/admin/metadata?mount={}&mode=updinfo&charset=UTF-8&song={}",
            self.config.get("server", "port"),
            quote(&mount),
            quote(&title)
        );
        let user = match self.config.get("server", "user") {
            "" => "source",
            user => user,
        };
        let credentials = format!("{user}:{}", self.config.get("server", "source_password"));
        let authorization = format!("Basic {}", base64::prelude::BASE64_STANDARD.encode(credentials));
        self.requests += 1;
        self.inflight = Some(self.requests);
        self.status("Sending metadata");
        let (request, generation, tx) = (self.requests, self.generation, self.tx.clone());
        std::thread::spawn(move || {
            let agent = ureq::Agent::new_with_config(ureq::Agent::config_builder().timeout_global(Some(REQUEST_TIMEOUT)).build());
            let outcome = match agent.get(&url).header("Authorization", &authorization).call() {
                // Icecast refuses titles for an Opus mount with a 200 and this message.
                Ok(mut response) => match response.body_mut().read_to_string() {
                    Ok(body) if body.contains("will not accept") => Outcome::Failed,
                    _ => Outcome::Sent,
                },
                Err(ureq::Error::StatusCode(500..)) => Outcome::Retry,
                Err(ureq::Error::StatusCode(_) | ureq::Error::BadUri(_) | ureq::Error::Http(_)) => Outcome::Failed,
                Err(_) => Outcome::Retry,
            };
            let _ = tx.send(Msg::Done { request, title, generation, outcome });
        });
    }

    /// Append a title the server accepted to this connection's titles file, if the
    /// settings ask for one. A resend of the same title is not written again.
    fn save_title(&mut self, title: &str) -> std::io::Result<()> {
        use std::io::Write;
        if self.config.get("recording", "save_titles") != "true" || self.titles.as_ref().is_some_and(|(_, last)| last == title) {
            return Ok(());
        }
        let path = match self.titles.take() {
            Some((path, _)) => path,
            None => {
                let folder = self.config.get("recording", "folder").trim();
                if folder.is_empty() {
                    return Err(std::io::Error::other("no folder configured"));
                }
                let folder = crate::recorder::expand_user(folder);
                std::fs::create_dir_all(&folder)?;
                let stream_name = self.config.get("server", "stream_name");
                crate::recorder::segment_path(&folder, stream_name, &crate::recorder::timestamp(), "titles.txt")
            }
        };
        let written = std::fs::File::options().append(true).create(true).open(&path).and_then(|mut file| writeln!(file, "{title}"));
        // After a failure the next title tries again, in the same file.
        self.titles = Some((path, if written.is_ok() { title.to_string() } else { String::new() }));
        written
    }

    fn done(&mut self, request: u64, title: String, generation: u64, outcome: Outcome) {
        if self.inflight != Some(request) {
            return;
        }
        self.inflight = None;
        let current = self.enabled && generation == self.generation && self.effective_title() == Some(&title);
        if current {
            match outcome {
                Outcome::Sent => {
                    self.retry_index = 0;
                    match self.save_title(&title) {
                        Ok(()) => self.status("Metadata OK"),
                        Err(error) => self.status(format!("Metadata OK; titles not saved: {error}")),
                    }
                    self.last_pushed = Some(title);
                }
                Outcome::Retry if self.retry_index < RETRY_DELAYS.len() => {
                    let delay = RETRY_DELAYS[self.retry_index];
                    self.retry_index += 1;
                    self.status(format!("Retrying metadata ({}s)", delay.as_secs()));
                    self.retry_at = Some(Instant::now() + delay);
                }
                _ => self.status("Metadata failed"),
            }
        }
        // Only a newer or forced request may follow at once: a 4xx must not loop
        // forever on the same title.
        if self.enabled && (self.pending || !current) {
            let force = self.pending_force;
            self.pending = false;
            self.pending_force = false;
            if self.retry_at.is_none() {
                self.push_current(force);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::mpsc::Receiver;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("radio-panel-rs-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn m1_read_track() {
        let d = temp_dir("m1");
        fn write(p: &Path, s: &str) {
            std::fs::write(p, s).unwrap();
        }
        let f1 = d.join("t1.txt");
        write(&f1, "  Now Playing \nsecond\n");
        assert_eq!(read_track(&f1), Some("Now Playing".to_string()));

        let f2 = d.join("t2.txt");
        write(&f2, "Title\r\nx");
        assert_eq!(read_track(&f2), Some("Title".to_string()));

        let f3 = d.join("t3.txt");
        write(&f3, "A\rB");
        assert_eq!(read_track(&f3), Some("A".to_string()));

        let f4 = d.join("t4.txt");
        write(&f4, "");
        assert_eq!(read_track(&f4), None);

        let f5 = d.join("t5.txt");
        write(&f5, "\nsecond\n");
        assert_eq!(read_track(&f5), None);

        let f6 = d.join("t6.txt");
        std::fs::write(&f6, [0xffu8, 0xfe, b'\n']).unwrap();
        assert_eq!(read_track(&f6), None);

        let f7 = d.join("t7.txt");
        write(&f7, "\u{feff}ハチ - 砂の惑星\n");
        assert_eq!(read_track(&f7), Some("ハチ - 砂の惑星".to_string()));

        assert_eq!(read_track(&d.join("nope.txt")), None);
        assert_eq!(read_track(Path::new("")), None);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn m2_watch_dir() {
        let d = temp_dir("m2");
        let a = d.join("a");
        let ab = a.join("b");
        std::fs::create_dir_all(&ab).unwrap();
        std::fs::write(a.join("file.txt"), "").unwrap();

        let p1 = d.join("a/b/c/e/track.txt");
        assert_eq!(watch_dir(&p1), Some(ab.as_path()));

        let p2 = d.join("a/b/track.txt");
        assert_eq!(watch_dir(&p2), Some(ab.as_path()));

        let p3 = a.join("file.txt/x/track.txt");
        assert_eq!(watch_dir(&p3), Some(a.as_path()));

        assert_eq!(watch_dir(Path::new("track.txt")), Some(Path::new(".")));
        assert_eq!(watch_dir(Path::new("")), None);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// A loopback HTTP server. It answers each request with the next status in
    /// `script` (200 once that is empty) and reports the request line and the
    /// Authorization header.
    fn mock_server(script: std::sync::Arc<std::sync::Mutex<Vec<u16>>>) -> (u16, Receiver<(String, String)>) {
        use std::io::{BufRead, BufReader, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, requests) = channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let lines: Vec<String> =
                    BufReader::new(&stream).lines().map_while(Result::ok).take_while(|line| !line.is_empty()).collect();
                let authorization = lines.iter().find_map(|line| line.strip_prefix("authorization: ").or(line.strip_prefix("Authorization: ")));
                let _ = tx.send((lines[0].clone(), authorization.unwrap_or_default().to_string()));
                let mut script = script.lock().unwrap();
                let status = if script.is_empty() { 200 } else { script.remove(0) };
                let _ = write!(stream, "HTTP/1.1 {status} Mock\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            }
        });
        (port, requests)
    }

    /// Wait for `wanted`, ignoring other events.
    fn wait(events: &Receiver<Event>, wanted: Event) {
        crate::streamer::tests::wait_event(events, 15, &format!("{wanted:?}"), |event| *event == wanted);
    }

    fn status(text: &str) -> Event {
        Event::Status(text.to_string())
    }

    fn track(text: &str) -> Event {
        Event::Track(text.to_string())
    }

    #[test]
    fn m3_pushes_titles_with_retry_and_override() {
        let d = temp_dir("m3");
        let script = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let (port, requests) = mock_server(script.clone());
        let request = |song: &str| {
            let (line, authorization) = requests.recv_timeout(Duration::from_secs(15)).expect("a metadata request");
            assert_eq!(line, format!("GET /admin/metadata?mount=%2Fmain.mp3&mode=updinfo&charset=UTF-8&song={song} HTTP/1.1"));
            assert_eq!(authorization, "Basic ZGo6cCB3OjE="); // dj:p w:1
        };
        let track_file = d.join("sub/track.txt");
        let mut config = Config::load(&d.join("none.conf")).unwrap();
        for (section, key, value) in [
            ("server", "host", "127.0.0.1"),
            ("server", "port", port.to_string().as_str()),
            ("server", "mount", "main.mp3"),
            ("server", "user", "dj"),
            ("server", "source_password", "p w:1"),
            ("metadata", "track_file", track_file.to_str().unwrap()),
        ] {
            config.set(section, key, value);
        }
        let (tx, events) = channel();
        let pusher = spawn(Box::new(config.clone()), move |event| drop(tx.send(event)));

        // No track file, stream not connected.
        wait(&events, track("-"));
        wait(&events, status("Disabled"));
        pusher.send(Command::SetEnabled(true));
        wait(&events, status("Waiting for stream"));
        wait(&events, status("Waiting for title"));

        // The file appears in a new directory. The first line is the title; a 5xx is
        // retried two seconds later.
        script.lock().unwrap().push(500);
        std::fs::create_dir(d.join("sub")).unwrap();
        std::fs::write(&track_file, "Artist - Tytuł & x=1\nsecond line\n").unwrap();
        wait(&events, track("Artist - Tytuł & x=1"));
        wait(&events, status("Sending metadata"));
        wait(&events, status("Retrying metadata (2s)"));
        let retried = Instant::now();
        wait(&events, status("Metadata OK"));
        assert!(retried.elapsed() >= Duration::from_millis(1900), "{:?}", retried.elapsed());
        request("Artist%20-%20Tytu%C5%82%20%26%20x%3D1");
        request("Artist%20-%20Tytu%C5%82%20%26%20x%3D1");

        // The file is replaced by a rename, then rewritten with the same title.
        std::fs::write(d.join("sub/new.txt"), "Second\n").unwrap();
        std::fs::rename(d.join("sub/new.txt"), &track_file).unwrap();
        wait(&events, track("Second"));
        wait(&events, status("Metadata OK"));
        request("Second");
        std::fs::write(&track_file, "Second\nother\n").unwrap();
        wait(&events, track("Second"));

        // A manual title overrides the file; the file changing meanwhile sends nothing.
        pusher.send(Command::SetManualTitle("  Manual  ".into()));
        wait(&events, track("Manual"));
        wait(&events, status("Metadata OK"));
        request("Manual");
        std::fs::write(&track_file, "Third\n").unwrap();
        std::thread::sleep(Duration::from_secs(1));
        pusher.send(Command::ReturnToAutomatic);
        wait(&events, track("Third"));
        wait(&events, status("Metadata OK"));
        request("Third");

        // A resend on request; a 4xx is final.
        pusher.send(Command::SendNow);
        wait(&events, status("Sending metadata"));
        wait(&events, status("Metadata OK"));
        request("Third");
        script.lock().unwrap().push(404);
        pusher.send(Command::SendNow);
        wait(&events, status("Metadata failed"));
        request("Third");

        // Saved settings name another track file.
        std::fs::write(d.join("other.txt"), "Other\n").unwrap();
        config.set("metadata", "track_file", d.join("other.txt").to_str().unwrap());
        pusher.send(Command::Configure(Box::new(config)));
        wait(&events, track("Other"));
        wait(&events, status("Metadata OK"));
        request("Other");

        // Off air: titles are still tracked, nothing is sent.
        pusher.send(Command::SetEnabled(false));
        wait(&events, status("Disabled"));
        pusher.send(Command::SetManualTitle("Unsent".into()));
        wait(&events, status("Disabled"));
        // Nothing was sent beyond the requests checked above, and reading the file
        // does not count as a change of it.
        std::thread::sleep(Duration::from_millis(700));
        drop(pusher);
        assert!(requests.try_recv().is_err());
        assert_eq!(events.try_iter().collect::<Vec<_>>(), []);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// The title reaches a local Icecast, and the ingest proxy's master.
    #[test]
    fn m6_titles_from_a_player() {
        let d = temp_dir("m6");
        let script = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let (port, requests) = mock_server(script);
        let track_file = d.join("track.txt");
        std::fs::write(&track_file, "From the file\n").unwrap();
        let mut config = Config::load(&d.join("none.conf")).unwrap();
        for (section, key, value) in [
            ("server", "host", "127.0.0.1"),
            ("server", "port", port.to_string().as_str()),
            ("metadata", "track_file", track_file.to_str().unwrap()),
            ("metadata", "player", "m6-player"),
        ] {
            config.set(section, key, value);
        }
        let fake = |title: Option<&str>| drop(crate::mpris::tests::FAKE_PLAYERS.lock().unwrap().insert("m6-player".into(), title.map(str::to_string)));
        fake(Some("ハチ - 砂の惑星"));
        let (tx, events) = channel();
        let pusher = spawn(Box::new(config.clone()), move |event| drop(tx.send(event)));
        pusher.send(Command::SetEnabled(true));

        // The player's title, not the file's, is shown and sent.
        wait(&events, track("ハチ - 砂の惑星"));
        wait(&events, status("Metadata OK"));
        let (line, _) = requests.recv_timeout(Duration::from_secs(15)).unwrap();
        assert!(line.contains("&song=%E3%83%8F%E3%83%81%20-%20%E7%A0%82%E3%81%AE%E6%83%91%E6%98%9F "), "{line}");

        // A new track arrives within a poll; a stopped player has no title.
        fake(Some("Next"));
        wait(&events, track("Next"));
        fake(None);
        wait(&events, track("-"));

        // Back to the file: its title, and the player is no longer followed.
        config.set("metadata", "player", "");
        pusher.send(Command::Configure(Box::new(config)));
        wait(&events, track("From the file"));
        fake(Some("Ignored"));
        std::thread::sleep(POLL * 3);
        let seen: Vec<Event> = events.try_iter().collect();
        assert!(!seen.contains(&track("Ignored")), "{seen:?}");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn m5_saves_sent_titles_line_by_line() {
        let d = temp_dir("m5");
        let script = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let (port, requests) = mock_server(script.clone());
        let folder = d.join("rec");
        let mut config = Config::load(&d.join("none.conf")).unwrap();
        for (section, key, value) in [
            ("server", "host", "127.0.0.1"),
            ("server", "port", port.to_string().as_str()),
            ("server", "stream_name", "My Show"),
            ("metadata", "track_file", d.join("none.txt").to_str().unwrap()),
            ("recording", "folder", folder.to_str().unwrap()),
        ] {
            config.set(section, key, value);
        }
        let files = || {
            let mut files: Vec<_> = std::fs::read_dir(&folder).map(|dir| dir.map(|entry| entry.unwrap().path()).collect()).unwrap_or_default();
            files.sort();
            files.into_iter().map(|path| (path.file_name().unwrap().to_string_lossy().into_owned(), std::fs::read_to_string(&path).unwrap())).collect::<Vec<_>>()
        };
        let (tx, events) = channel();
        let pusher = spawn(Box::new(config.clone()), move |event| drop(tx.send(event)));
        let send = |title: &str| {
            pusher.send(Command::SetManualTitle(title.into()));
            wait(&events, track(title));
        };
        pusher.send(Command::SetEnabled(true));
        wait(&events, status("Waiting for stream"));

        // Off by default: a sent title writes nothing.
        send("Zero");
        wait(&events, status("Metadata OK"));
        assert!(files().is_empty());

        // On: only titles the server took, each once, in the order sent.
        config.set("recording", "save_titles", "true");
        pusher.send(Command::Configure(Box::new(config.clone())));
        send("Artist - Tytuł");
        wait(&events, status("Metadata OK"));
        pusher.send(Command::SendNow);
        wait(&events, status("Sending metadata"));
        wait(&events, status("Metadata OK"));
        script.lock().unwrap().push(404);
        send("Refused");
        wait(&events, status("Metadata failed"));
        send("Second");
        wait(&events, status("Metadata OK"));
        let first = files();
        assert_eq!(first.len(), 1);
        assert!(first[0].0.starts_with("My_Show-") && first[0].0.ends_with(".titles.txt"), "{}", first[0].0);
        assert_eq!(first[0].1, "Artist - Tytuł\nSecond\n");

        // A new connection starts a new file.
        pusher.send(Command::SetEnabled(false));
        pusher.send(Command::SetEnabled(true));
        wait(&events, status("Metadata OK"));
        let both = files();
        assert_eq!(both.len(), 2, "{both:?}");
        assert!(both.contains(&first[0]));
        assert!(both.iter().any(|(_, text)| text == "Second\n"));

        // No folder at the next connection: the title is still sent, the status says
        // why it was not saved. A running connection keeps its file.
        config.set("recording", "folder", " ");
        pusher.send(Command::Configure(Box::new(config)));
        send("Third");
        wait(&events, status("Metadata OK"));
        pusher.send(Command::SetEnabled(false));
        pusher.send(Command::SetEnabled(true));
        wait(&events, status("Metadata OK; titles not saved: no folder configured"));
        drop(pusher);
        while requests.try_recv().is_ok() {}
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn m4_metadata_reaches_icecast_and_ingest() {
        use crate::pw::tests::{pactl, wait_for};
        use crate::streamer::tests::*;
        let _live = LIVE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let d = temp_dir("m4");
        let sink = crate::pw::spawn(SINK, |_| {});
        wait_for("sink monitor visible to pulse", || pactl(&["list", "short", "sources"]).contains(&format!("{SINK}.monitor")));
        // Japanese, full-width, an emoji, and what URLs and the ICY format treat specially.
        const TITLE: &str = "ハチ - 砂の惑星 feat. 初音ミク (Don't 100% #1 ?a=b+c & ＡＢＣ 🎵) Zażółć";
        std::fs::write(d.join("track.txt"), format!("{TITLE}\n")).unwrap();
        let mut config = Config::load(&d.join("none.conf")).unwrap();
        for (section, key, value) in [
            ("server", "host", "127.0.0.1"),
            ("server", "mount", "/main.mp3"),
            ("metadata", "track_file", d.join("track.txt").to_str().unwrap()),
        ] {
            config.set(section, key, value);
        }
        let (icecast_port, proxy_port) = (PORT.to_string(), PROXY_PORT.to_string());
        // Icecast: what a listener's player reads from the stream, not only the status page.
        let on_icecast: fn() -> bool = || icecast_status().contains(TITLE) && icy_title("/main.mp3").as_deref() == Some(TITLE);
        // ingest: what its own code forwarded to the master, decoded there.
        let on_master: fn() -> bool = || http_get(MASTER_PORT, "/status").contains(&format!("song={TITLE}\n"));
        let servers = [(icecast_port.as_str(), "source", PASSWORD, on_icecast), (proxy_port.as_str(), "dj", DJ_PASSWORD, on_master)];
        let _icecast = start_icecast(&d, PORT);
        let _ingest = start_ingest(&d);
        for (port, user, password, title_arrived) in servers {
            for (key, value) in [("port", port), ("user", user), ("source_password", password)] {
                config.set("server", key, value);
            }
            let (tx, stream_events) = channel();
            let streamer = crate::streamer::spawn(SINK, move |event| drop(tx.send(event)));
            streamer.send(crate::streamer::Command::GoLive(Box::new(config.clone())));
            wait_event(&stream_events, 15, "connected", |event| *event == crate::streamer::Event::Connected(true));
            let (tx, events) = channel();
            let pusher = spawn(Box::new(config.clone()), move |event| drop(tx.send(event)));
            pusher.send(Command::SetEnabled(true));
            wait(&events, status("Metadata OK"));
            wait_for("the title on the server", title_arrived);
        }
        // Icecast takes no titles for an Opus mount; it says so with a 200.
        for (key, value) in [("port", icecast_port.as_str()), ("user", "source"), ("source_password", PASSWORD), ("mount", "/main.opus")] {
            config.set("server", key, value);
        }
        config.set("audio", "format", "opus");
        let (tx, stream_events) = channel();
        let streamer = crate::streamer::spawn(SINK, move |event| drop(tx.send(event)));
        streamer.send(crate::streamer::Command::GoLive(Box::new(config.clone())));
        wait_event(&stream_events, 15, "connected", |event| *event == crate::streamer::Event::Connected(true));
        let (tx, events) = channel();
        let pusher = spawn(Box::new(config), move |event| drop(tx.send(event)));
        pusher.send(Command::SetEnabled(true));
        wait(&events, status("Metadata failed"));
        drop((pusher, streamer));
        drop(sink);
        let _ = std::fs::remove_dir_all(&d);
    }
}
