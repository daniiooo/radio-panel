#![allow(dead_code)] // until the app uses it

use std::path::{Path, PathBuf};

use crate::config::Config;
use crate::ffmpeg::{audio_encoding, safe_filename_component};

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
}
