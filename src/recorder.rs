//! Local recording: one thread supervises a second ffmpeg that writes a segment file
//! for each connected stream, with a cue file of track changes beside it.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::ffmpeg::{audio_encoding, die_with_parent, no_fallback, safe_filename_component, terminate};

pub const CUE_HEADER: &str = "# Radio Panel track cues (seconds from segment start)\n";

pub fn command(config: &Config, sink: &str, path: &Path) -> Vec<String> {
    let (codec_args, container, _ct) = audio_encoding(config.get("audio", "format"));
    let bitrate = config.get("audio", "bitrate");
    let mut cmd: Vec<String> = vec![
        "ffmpeg".into(),
        "-hide_banner".into(),
        "-loglevel".into(),
        "warning".into(),
        "-nostdin".into(),
        "-f".into(),
        "pulse".into(),
        "-i".into(),
        format!("{sink}.monitor"),
        "-ac".into(),
        "2".into(),
    ];
    cmd.extend(codec_args.iter().copied().map(String::from));
    cmd.push("-b:a".into());
    cmd.push(format!("{bitrate}k"));
    cmd.push("-f".into());
    cmd.push(container.into());
    cmd.push(path.to_string_lossy().into_owned());
    cmd
}

pub fn segment_path(folder: &Path, stream_name: &str, stamp: &str, container: &str) -> PathBuf {
    let base = safe_filename_component(stream_name, "radio");
    let mut n = 1;
    loop {
        let extra = if n == 1 { "" } else { &format!("-{n}") };
        let candidate = folder
            .join(format!("{base}-{stamp}{extra}.{container}"));
        if !candidate.exists() && !candidate.with_extension("cue").exists() {
            return candidate;
        }
        n += 1;
    }
}

pub fn cue_line(elapsed: f64, title: &str) -> Option<String> {
    let clean = title.replace("\r\n", " ").replace(['\n', '\r'], " ");
    let clean = clean.trim();
    if clean.is_empty() {
        None
    } else {
        Some(format!("{elapsed:010.3}\t{clean}\n"))
    }
}

pub enum Command {
    /// The stream is connected: record a segment if this configuration enables it.
    Start(Box<Config>),
    Stop,
    /// The track changed: note it in the cue file of the running segment.
    SetTitle(String),
}

enum Msg {
    Command(Command),
    Quit,
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

/// `sink` is the sink whose monitor ffmpeg records. `on_status` gets the status line
/// and runs on the recorder thread.
pub fn spawn(sink: &str, on_status: impl Fn(String) + Send + 'static) -> Handle {
    let (tx, rx) = channel();
    let supervisor =
        Supervisor { sink: sink.to_string(), on_status, config: None, want: false, retry_at: None, child: None, cue: None, segment: None, last_cue_title: String::new() };
    Handle { tx, thread: Some(std::thread::spawn(move || supervisor.run(rx))) }
}

const RETRY: Duration = Duration::from_secs(2);
const POLL: Duration = Duration::from_millis(200);

struct Supervisor<F> {
    sink: String,
    on_status: F,
    config: Option<Box<Config>>,
    want: bool,
    retry_at: Option<Instant>,
    child: Option<Child>,
    /// The open cue file and the start of its segment.
    cue: Option<(File, Instant)>,
    /// The audio file of the running segment.
    segment: Option<PathBuf>,
    last_cue_title: String,
}

impl<F: Fn(String)> Supervisor<F> {
    fn run(mut self, rx: Receiver<Msg>) {
        loop {
            match rx.recv_timeout(POLL) {
                Ok(Msg::Command(Command::Start(config))) => {
                    self.want = config.get("recording", "enabled") == "true";
                    self.config = Some(config);
                    if self.want {
                        self.start_segment();
                    } else {
                        self.status("Recording disabled");
                    }
                }
                Ok(Msg::Command(Command::Stop)) => {
                    self.want = false;
                    self.retry_at = None;
                    self.end_segment();
                    self.status("Recording stopped");
                }
                Ok(Msg::Command(Command::SetTitle(title))) => self.set_title(&title),
                Err(RecvTimeoutError::Timeout) => {}
                Ok(Msg::Quit) | Err(RecvTimeoutError::Disconnected) => break,
            }
            // ponytail: ffmpeg's exit is noticed by polling, up to 200 ms late; use a
            // waiting thread if that lag ever matters.
            if let Some(child) = &mut self.child
                && !matches!(child.try_wait(), Ok(None))
            {
                self.child = None;
                self.close_cue();
                self.schedule_retry();
            }
            if self.retry_at.is_some_and(|at| Instant::now() >= at) {
                self.retry_at = None;
                self.start_segment();
            }
        }
        self.end_segment();
    }

    fn status(&self, text: impl Into<String>) {
        (self.on_status)(text.into());
    }

    fn start_segment(&mut self) {
        let Some(config) = &self.config else { return };
        if !self.want || self.child.is_some() {
            return;
        }
        let folder = config.get("recording", "folder").trim();
        if folder.is_empty() {
            self.want = false;
            self.status("Recording failed: no folder configured");
            return;
        }
        let started = (|| -> std::io::Result<(Child, File, PathBuf)> {
            let folder = expand_user(folder);
            std::fs::create_dir_all(&folder)?;
            let (_, container, _) = audio_encoding(config.get("audio", "format"));
            let path = segment_path(&folder, config.get("server", "stream_name"), &timestamp(), container);
            let mut cue = File::options().append(true).create(true).open(path.with_extension("cue"))?;
            cue.write_all(CUE_HEADER.as_bytes())?;
            let args = command(config, &self.sink, &path);
            let mut ffmpeg = std::process::Command::new(&args[0]);
            ffmpeg.args(&args[1..]).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
            die_with_parent(&mut ffmpeg);
            no_fallback(&mut ffmpeg);
            Ok((ffmpeg.spawn()?, cue, path))
        })();
        match started {
            Ok((child, cue, path)) => {
                self.child = Some(child);
                self.cue = Some((cue, Instant::now()));
                self.segment = Some(path.clone());
                self.last_cue_title.clear();
                self.status(format!("Recording: {}", path.file_name().unwrap_or_default().to_string_lossy()));
            }
            Err(_) => {
                self.status("Recording failed");
                self.schedule_retry();
            }
        }
    }

    fn end_segment(&mut self) {
        if let Some(mut child) = self.child.take() {
            terminate(&mut child, Duration::from_secs(1));
        }
        self.close_cue();
    }

    fn set_title(&mut self, title: &str) {
        let Some((cue, started)) = &mut self.cue else { return };
        if title == self.last_cue_title {
            return;
        }
        if let Some(line) = cue_line(started.elapsed().as_secs_f64(), title) {
            let _ = cue.write_all(line.as_bytes());
            self.last_cue_title = title.to_string();
        }
    }

    /// A segment without audio (ffmpeg failed at once) leaves no files: a failure
    /// that lasts would otherwise add a cue file at every retry.
    fn close_cue(&mut self) {
        if let Some((cue, _)) = self.cue.take() {
            let _ = cue.sync_all();
        }
        if let Some(segment) = self.segment.take()
            && !std::fs::metadata(&segment).is_ok_and(|file| file.len() > 0)
        {
            let _ = std::fs::remove_file(&segment);
            let _ = std::fs::remove_file(segment.with_extension("cue"));
        }
    }

    fn schedule_retry(&mut self) {
        if self.want && self.retry_at.is_none() {
            self.status("Recording failed; retrying");
            self.retry_at = Some(Instant::now() + RETRY);
        }
    }
}

/// `~` and `~/...` mean the home directory.
/// ponytail: `~user` is not expanded; nothing writes such a path into the config.
pub(crate) fn expand_user(path: &str) -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_default();
    match path.strip_prefix('~') {
        Some("") => PathBuf::from(home),
        Some(rest) if rest.starts_with('/') => PathBuf::from(home + rest),
        _ => PathBuf::from(path),
    }
}

/// Local time as `YYYYmmdd-HHMMSS`.
pub(crate) fn timestamp() -> String {
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    unsafe {
        let now = libc::time(std::ptr::null_mut());
        libc::localtime_r(&now, &mut tm);
    }
    format!("{:04}{:02}{:02}-{:02}{:02}{:02}", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn cfg(pairs: &[(&str, &str, &str)]) -> Config {
        let mut c = Config::load(Path::new("/nonexistent/radio-panel-test.conf")).unwrap();
        for (s, k, v) in pairs {
            c.set(s, k, v);
        }
        c
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("radio-panel-rs-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn r1_command_opus() {
        let c = cfg(&[("audio", "format", "opus"), ("audio", "bitrate", "96")]);
        assert_eq!(
            command(&c, "radio-dev", Path::new("/tmp/x/My Show.ogg")),
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "warning",
                "-nostdin",
                "-f",
                "pulse",
                "-i",
                "radio-dev.monitor",
                "-ac",
                "2",
                "-c:a",
                "libopus",
                "-b:a",
                "96k",
                "-f",
                "ogg",
                "/tmp/x/My Show.ogg",
            ]
        );
    }

    #[test]
    fn r1_command_mp3() {
        let c = cfg(&[("audio", "format", "mp3"), ("audio", "bitrate", "192")]);
        assert_eq!(
            command(&c, "radio-dev", Path::new("/tmp/a.mp3")),
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "warning",
                "-nostdin",
                "-f",
                "pulse",
                "-i",
                "radio-dev.monitor",
                "-ac",
                "2",
                "-c:a",
                "libmp3lame",
                "-ar",
                "44100",
                "-b:a",
                "192k",
                "-f",
                "mp3",
                "/tmp/a.mp3",
            ]
        );
    }

    #[test]
    fn r2_segment_path() {
        let d = temp_dir("r2");
        let stamp = "20261111-123456";
        let name = "My Show: Live!";
        let r = segment_path(&d, name, stamp, "mp3");
        assert_eq!(r, d.join("My_Show_Live-20261111-123456.mp3"));
        std::fs::File::create(&r).unwrap();
        let r = segment_path(&d, name, stamp, "mp3");
        assert_eq!(r, d.join("My_Show_Live-20261111-123456-2.mp3"));
        std::fs::File::create(r.with_extension("cue")).unwrap();
        let r = segment_path(&d, name, stamp, "mp3");
        assert_eq!(r, d.join("My_Show_Live-20261111-123456-3.mp3"));
        let r = segment_path(&d, "", "20261111-123456", "ogg");
        assert_eq!(r, d.join("radio-20261111-123456.ogg"));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn r3_cue_line() {
        assert_eq!(
            cue_line(12.5, "Artist - Title"),
            Some("000012.500\tArtist - Title\n".to_string())
        );
        assert_eq!(cue_line(0.0, "x"), Some("000000.000\tx\n".to_string()));
        assert_eq!(cue_line(12345.6789, "x"), Some("012345.679\tx\n".to_string()));
        assert_eq!(
            cue_line(1.0, "A\r\nB\nC  D "),
            Some("000001.000\tA B C  D\n".to_string())
        );
        assert_eq!(cue_line(1.0, "  \n "), None);
        assert_eq!(cue_line(1.0, ""), None);
    }

    #[test]
    fn r4_expand_user_and_timestamp() {
        let home = std::env::var("HOME").unwrap();
        assert_eq!(expand_user("~"), PathBuf::from(&home));
        assert_eq!(expand_user("~/Radio Recordings"), PathBuf::from(format!("{home}/Radio Recordings")));
        assert_eq!(expand_user("~other/x"), PathBuf::from("~other/x"));
        assert_eq!(expand_user("/tmp/a~b"), PathBuf::from("/tmp/a~b"));
        let stamp = timestamp();
        assert_eq!(stamp.len(), 15);
        assert!(stamp.starts_with("20") && stamp.as_bytes()[8] == b'-', "{stamp}");
    }

    /// Records the silent dev sink into a temp folder.
    #[test]
    fn records_segments_with_cues() {
        use crate::pw::tests::{pactl, wait_for};
        use crate::streamer::tests::{LIVE, SINK, ffmpeg_pids};
        let _live = LIVE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let dir = temp_dir("recorder");
        let sink = crate::pw::spawn(SINK, |_| {});
        wait_for("sink monitor visible to pulse", || pactl(&["list", "short", "sources"]).contains(&format!("{SINK}.monitor")));
        let (tx, statuses) = channel();
        let recorder = spawn(SINK, move |status| drop(tx.send(status)));
        let next = || statuses.recv_timeout(Duration::from_secs(15)).expect("a recording status");

        // Disabled by default; enabled without a folder gives up.
        let mut config = cfg(&[("server", "stream_name", "My Show")]);
        recorder.send(Command::Start(Box::new(config.clone())));
        assert_eq!(next(), "Recording disabled");
        config.set("recording", "enabled", "true");
        config.set("recording", "folder", " ");
        recorder.send(Command::Start(Box::new(config.clone())));
        assert_eq!(next(), "Recording failed: no folder configured");
        recorder.send(Command::SetTitle("Before any segment".into()));
        assert!(ffmpeg_pids().is_empty());

        // A segment in a folder that does not exist yet, with cues for changed titles.
        let folder = dir.join("new/sub");
        config.set("recording", "folder", folder.to_str().unwrap());
        recorder.send(Command::Start(Box::new(config.clone())));
        let first = next();
        assert!(first.starts_with("Recording: My_Show-") && first.ends_with(".mp3"), "{first}");
        let first = folder.join(first.trim_start_matches("Recording: "));
        for title in ["Artist - Title", "Artist - Title", "", "Two\nLines"] {
            recorder.send(Command::SetTitle(title.into()));
        }
        let cue = || std::fs::read_to_string(first.with_extension("cue")).unwrap_or_default();
        wait_for("two cue lines", || cue().lines().count() == 3);
        let cue = cue();
        let lines: Vec<&str> = cue.lines().collect();
        assert_eq!(format!("{}\n", lines[0]), CUE_HEADER);
        for (line, title) in [(lines[1], "Artist - Title"), (lines[2], "Two Lines")] {
            let (seconds, cued) = line.split_once('\t').unwrap();
            assert!(seconds.len() == 10 && seconds.parse::<f64>().is_ok(), "{line}");
            assert_eq!(cued, title);
        }

        // ffmpeg dies: a new segment starts two seconds later, with a fresh cue file.
        let pids = ffmpeg_pids();
        assert_eq!(pids.len(), 1);
        let _ = std::process::Command::new("kill").args(["-KILL", &pids[0]]).status();
        assert_eq!(next(), "Recording failed; retrying");
        let second = next();
        assert!(second.starts_with("Recording: My_Show-"), "{second}");
        let second = folder.join(second.trim_start_matches("Recording: "));
        assert_ne!(first, second);
        wait_for("audio in the second segment", || std::fs::metadata(&second).is_ok_and(|file| file.len() > 0));
        recorder.send(Command::Stop);
        assert_eq!(next(), "Recording stopped");
        assert!(ffmpeg_pids().is_empty());
        assert_eq!(std::fs::read_to_string(second.with_extension("cue")).unwrap(), CUE_HEADER);
        recorder.send(Command::Stop);
        assert_eq!(next(), "Recording stopped");

        // Dropping the handle while recording ends ffmpeg too.
        recorder.send(Command::Start(Box::new(config.clone())));
        assert!(next().starts_with("Recording: My_Show-"));
        drop(recorder);
        assert!(ffmpeg_pids().is_empty());

        drop(sink);

        // Without the sink ffmpeg fails at once instead of recording another source,
        // and the failed segments leave no files behind.
        let missing = dir.join("missing");
        config.set("recording", "folder", missing.to_str().unwrap());
        let (tx, statuses) = channel();
        let recorder = spawn("radio-dev-missing", move |status| drop(tx.send(status)));
        recorder.send(Command::Start(Box::new(config)));
        let mut seen = Vec::new();
        while let Ok(status) = statuses.recv_timeout(Duration::from_secs(5)) {
            seen.push(status);
            if seen.iter().filter(|status| *status == "Recording failed; retrying").count() == 2 {
                break;
            }
        }
        drop(recorder);
        assert_eq!(seen.iter().filter(|status| *status == "Recording failed; retrying").count(), 2, "{seen:?}");
        assert_eq!(std::fs::read_dir(&missing).unwrap().count(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
