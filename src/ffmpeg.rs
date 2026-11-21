
use std::os::unix::process::CommandExt;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use crate::config::Config;

/// If the panel dies without cleaning up, the child must not keep running. The
/// signal is tied to the spawning thread, which must outlive the child.
pub fn die_with_parent(command: &mut Command) {
    unsafe {
        command.pre_exec(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }
}

/// ffmpeg's pulse input must fail rather than fall back when our sink is missing or
/// goes away: pipewire-pulse would capture the default sink's monitor instead, so
/// whatever the desktop plays would go on air (seen 2026-11-21).
pub fn no_fallback(command: &mut Command) {
    command.env("PULSE_PROP", "node.dont-fallback=true");
}

/// SIGTERM, then SIGKILL after `grace`. The child must not have been reaped yet,
/// so that its pid cannot belong to another process.
pub fn terminate(child: &mut Child, grace: Duration) {
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    let deadline = Instant::now() + grace;
    while matches!(child.try_wait(), Ok(None)) {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// (codec args, container, content type) for a format name; unknown -> mp3.
pub fn audio_encoding(format: &str) -> (&'static [&'static str], &'static str, &'static str) {
    match format {
        "opus" => (&["-c:a", "libopus"], "ogg", "application/ogg"),
        "vorbis" => (&["-c:a", "libvorbis"], "ogg", "application/ogg"),
        _ => (&["-c:a", "libmp3lame", "-ar", "44100"], "mp3", "audio/mpeg"),
    }
}

/// Collapse runs of characters outside `A-Za-z0-9._-` into one `_`,
/// trim leading/trailing `._-`, cap at 80 chars, else `fallback`.
pub fn safe_filename_component(value: &str, fallback: &str) -> String {
    let mut collapsed = String::with_capacity(value.len());
    let mut prev_bad = false;
    for c in value.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
            collapsed.push(c);
            prev_bad = false;
        } else if !prev_bad {
            collapsed.push('_');
            prev_bad = true;
        }
    }
    let trimmed = collapsed.trim_matches(['.', '_', '-']);
    let out: String = trimmed.chars().take(80).collect();
    if out.is_empty() {
        fallback.to_string()
    } else {
        out
    }
}

/// Percent-encode every byte except `A-Za-z0-9` and `_ . - ~`.
pub fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The full ffmpeg argument list for a SOURCE push to Icecast.
pub fn stream_command(config: &Config, sink: &str) -> Vec<String> {
    let user = config.get("server", "user");
    let user = if user.is_empty() { "source" } else { user };
    let password = config.get("server", "source_password");
    let host = config.get("server", "host");
    let port = config.get("server", "port");
    let mount = config
        .get("server", "mount")
        .trim_start_matches('/')
        .to_string();

    let (codec, container, content_type) = audio_encoding(config.get("audio", "format"));
    let bitrate = config.get("audio", "bitrate");

    let mut cmd: Vec<String> = vec![
        "ffmpeg".into(),
        "-hide_banner".into(),
        "-loglevel".into(),
        "warning".into(),
        "-nostdin".into(),
        "-progress".into(),
        "pipe:1".into(),
        "-stats_period".into(),
        "0.2".into(),
        "-f".into(),
        "pulse".into(),
        "-i".into(),
        format!("{sink}.monitor"),
        "-ac".into(),
        "2".into(),
    ];
    cmd.extend(codec.iter().map(|s| s.to_string()));
    cmd.extend(["-b:a".into(), format!("{bitrate}k")]);
    cmd.extend([
        "-content_type".into(),
        content_type.to_string(),
        "-legacy_icecast".into(),
        "1".into(),
        "-f".into(),
        container.to_string(),
    ]);
    let stream_name = config.get("server", "stream_name");
    let genre = config.get("server", "genre");
    let description = config.get("server", "description");
    let stream_url = config.get("server", "stream_url");
    if !stream_name.is_empty() {
        cmd.extend(["-ice_name".into(), stream_name.to_string()]);
    }
    if !genre.is_empty() {
        cmd.extend(["-ice_genre".into(), genre.to_string()]);
    }
    if !description.is_empty() {
        cmd.extend(["-ice_description".into(), description.to_string()]);
    }
    if !stream_url.is_empty() {
        cmd.extend(["-ice_url".into(), stream_url.to_string()]);
    }
    if config.get("server", "public") == "true" {
        cmd.extend(["-ice_public".into(), "1".into()]);
    }
    cmd.push(format!(
        "icecast://{}:{}@{}:{}/{}",
        quote(user),
        quote(password),
        host,
        port,
        mount
    ));
    cmd
}

/// Hide configured credentials in diagnostic text: the literal and quoted
/// forms become `[redacted]`, then any `scheme://userinfo@` (scheme =
/// icecast/http/https, case-insensitive; userinfo = up to the next whitespace,
/// `/` or `@`) becomes `scheme://[credentials]@` with the scheme kept as written.
pub fn scrub_diagnostic(config: &Config, text: &str) -> String {
    let user = config.get("server", "user");
    let user = if user.is_empty() { "source" } else { user };
    let password = config.get("server", "source_password");

    let mut out = text.to_string();
    let quoted_user = quote(user);
    let quoted_password = quote(password);
    for secret in [user, password, quoted_user.as_str(), quoted_password.as_str()] {
        if !secret.is_empty() {
            out = out.replace(secret, "[redacted]");
        }
    }

    let lower = out.to_ascii_lowercase();
    let needles = ["icecast://", "https://", "http://"];
    let mut result = String::with_capacity(out.len());
    let mut pos = 0;
    while pos < out.len() {
        let hit = needles
            .iter()
            .filter_map(|n| lower[pos..].find(n).map(|i| (pos + i, n.len())))
            .min_by_key(|(p, _)| *p);
        match hit {
            None => {
                result.push_str(&out[pos..]);
                break;
            }
            Some((p, nlen)) => {
                result.push_str(&out[pos..p + nlen]);
                let start = p + nlen;
                let bytes = out.as_bytes();
                let mut j = start;
                while j < bytes.len() && !matches!(bytes[j], b'@' | b'/') && !bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                if j < bytes.len() && bytes[j] == b'@' {
                    result.push_str("[credentials]@");
                    pos = j + 1;
                } else {
                    // No userinfo here; keep scanning, a later URL may carry some.
                    pos = start;
                }
            }
        }
    }
    result
}

/// The last non-empty trimmed line, or "ffmpeg exited"; capped at the last 240 chars.
pub fn final_diagnostic_line(details: &str) -> String {
    let line = details
        .lines()
        .rev()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("ffmpeg exited")
        .to_string();
    if line.chars().count() <= 240 {
        line
    } else {
        let skip = line.chars().count() - 240;
        line.chars().skip(skip).collect()
    }
}

/// `code` as a number of its own, not inside a longer one such as a port.
fn has_code(text: &str, code: &str) -> bool {
    text.match_indices(code).any(|(at, _)| {
        let digit = |c: Option<char>| c.is_some_and(|c| c.is_ascii_digit());
        !digit(text[..at].chars().next_back()) && !digit(text[at + code.len()..].chars().next())
    })
}

/// (message, retry) for a diagnostic, first match wins. Give it ffmpeg's own text, not
/// the scrubbed one: a password inside a word would change the words matched here.
pub fn classify_diagnostic(detail: &str, sink: &str) -> (&'static str, bool) {
    let d = detail.to_ascii_lowercase();
    let sink_mon = format!("{sink}.monitor").to_ascii_lowercase();
    if d.contains("mountpoint in use")
        || d.contains("mountpoint already")
        || d.contains("mountpoint is occupied")
        || d.contains("mount point in use")
        || d.contains("mount already")
        || d.contains("already in use")
        || d.contains("already occupied")
        || d.contains("already taken")
        || d.contains("409 conflict")
    {
        ("mount already occupied", false)
    } else if has_code(&d, "401")
        || d.contains("unauthorized")
        || d.contains("authentication failed")
        || d.contains("access denied")
    {
        ("authentication failed", false)
    } else if has_code(&d, "403") {
        // Icecast: no more sources or a format it refuses; a proxy in front of the
        // server (Cloudflare refuses SOURCE and PUT). A wrong password is a 401.
        ("refused by the server (403)", false)
    } else if d.contains("unknown encoder")
        || d.contains("error selecting an encoder")
        || d.contains("libmp3lame")
        || d.contains("ffmpeg error: execvp")
        || d.contains("ffmpeg error: could not start")
        || d.contains("execvp: no such file")
        || d.contains("ffmpeg not found")
    {
        ("ffmpeg encoder unavailable", false)
    } else if (d.contains("pulse")
        && (d.contains("no such")
            || d.contains("not found")
            || d.contains("invalid argument")
            || d.contains("failed to open")))
        || (d.contains(&sink_mon) && d.contains("no such"))
    {
        ("Pulse stream source unavailable", false)
    } else if d.contains("name or service not known")
        || d.contains("resolve")
        || d.contains("connection refused")
        || d.contains("timed out")
        || d.contains("timeout")
        || d.contains("network is unreachable")
        || d.contains("connection reset")
    {
        ("network connection failed", true)
    } else {
        ("stream process exited", true)
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

    #[test]
    fn t1_audio_encoding() {
        assert_eq!(
            audio_encoding("mp3"),
            (&["-c:a", "libmp3lame", "-ar", "44100"] as &[&str], "mp3", "audio/mpeg")
        );
        assert_eq!(
            audio_encoding("opus"),
            (&["-c:a", "libopus"] as &[&str], "ogg", "application/ogg")
        );
        assert_eq!(
            audio_encoding("vorbis"),
            (&["-c:a", "libvorbis"] as &[&str], "ogg", "application/ogg")
        );
        assert_eq!(
            audio_encoding("flac"),
            (&["-c:a", "libmp3lame", "-ar", "44100"] as &[&str], "mp3", "audio/mpeg")
        );
    }

    #[test]
    fn t2_safe_filename_component() {
        assert_eq!(
            safe_filename_component("My Show: Live!", "radio"),
            "My_Show_Live"
        );
        assert_eq!(safe_filename_component("..--__", "radio"), "radio");
        assert_eq!(safe_filename_component("a  b", "radio"), "a_b");
        assert_eq!(safe_filename_component("ąę", "radio"), "radio");
        let s = "a".repeat(200);
        assert_eq!(safe_filename_component(&s, "radio"), "a".repeat(80));
    }

    #[test]
    fn t3_quote() {
        assert_eq!(
            quote("a b/c@d:é~_.-"),
            "a%20b%2Fc%40d%3A%C3%A9~_.-"
        );
    }

    #[test]
    fn t4_stream_command() {
        let c = cfg(&[
            ("server", "host", "127.0.0.1"),
            ("server", "port", "8000"),
            ("server", "mount", "/main.mp3"),
            ("server", "user", "dj name"),
            ("server", "source_password", "p@ss/w:rd"),
        ]);
        let url = format!(
            "icecast://{}:{}@127.0.0.1:8000/main.mp3",
            "dj%20name",
            "p%40ss%2Fw%3Ard"
        );
        assert_eq!(
            stream_command(&c, "radio-dev"),
            vec![
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "warning",
                "-nostdin",
                "-progress",
                "pipe:1",
                "-stats_period",
                "0.2",
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
                "-content_type",
                "audio/mpeg",
                "-legacy_icecast",
                "1",
                "-f",
                "mp3",
                url.as_str(),
            ]
        );
    }

    #[test]
    fn t5_stream_command() {
        let c = cfg(&[
            ("server", "host", "127.0.0.1"),
            ("server", "port", "8000"),
            ("audio", "format", "opus"),
            ("audio", "bitrate", "96"),
            ("server", "stream_name", ""),
            ("server", "genre", ""),
            ("server", "description", ""),
            ("server", "stream_url", "https://example.invalid/x"),
            ("server", "public", "true"),
            ("server", "user", ""),
        ]);
        let url = format!("icecast://{}:{}@127.0.0.1:8000/main.mp3", "source", "hackme");
        assert_eq!(
            stream_command(&c, "radio-dev"),
            vec![
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "warning",
                "-nostdin",
                "-progress",
                "pipe:1",
                "-stats_period",
                "0.2",
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
                "-content_type",
                "application/ogg",
                "-legacy_icecast",
                "1",
                "-f",
                "ogg",
                "-ice_url",
                "https://example.invalid/x",
                "-ice_public",
                "1",
                url.as_str(),
            ]
        );
    }

    #[test]
    fn t6_scrub_diagnostic() {
        // (i)
        let c = cfg(&[
            ("server", "user", "dj name"),
            ("server", "source_password", "p@ss"),
        ]);
        let input = format!(
            "open icecast://{}:{}@127.0.0.1:8000/main.mp3 failed for dj name with p@ss",
            "dj%20name",
            "p%40ss"
        );
        let expected = format!(
            "open icecast://{}@127.0.0.1:8000/main.mp3 failed for [redacted] with [redacted]",
            "[credentials]"
        );
        assert_eq!(scrub_diagnostic(&c, &input), expected);

        // (ii)
        let c = cfg(&[
            ("server", "user", "u1"),
            ("server", "source_password", "pw1"),
        ]);
        let input = format!(
            "HTTP://{}@host/x and https://{}@b then icecast://127.0.0.1:8000/main.mp3",
            "other:secret",
            "a"
        );
        let expected = format!(
            "HTTP://{0}@host/x and https://{0}@b then icecast://127.0.0.1:8000/main.mp3",
            "[credentials]"
        );
        assert_eq!(scrub_diagnostic(&c, &input), expected);

        // (iii)
        let c = cfg(&[
            ("server", "user", ""),
            ("server", "source_password", ""),
        ]);
        let input = format!("the source is icecast://{}@h/m", "x:y");
        let expected = format!("the [redacted] is icecast://{}@h/m", "[credentials]");
        assert_eq!(scrub_diagnostic(&c, &input), expected);

        // (iv) a URL without userinfo must not end the scan: the later one is still scrubbed.
        let c = cfg(&[("server", "user", "u1"), ("server", "source_password", "pw1")]);
        let input = format!("icecast://127.0.0.1:8000/a then http://{}@h/x", "other:secret");
        let expected = format!("icecast://127.0.0.1:8000/a then http://{}@h/x", "[credentials]");
        assert_eq!(scrub_diagnostic(&c, &input), expected);

        // (v) non-ASCII userinfo (bytes 0xA0/0x85 inside a char are not whitespace).
        let input = format!("icecast://{}@h/m", "Рамка:x");
        assert_eq!(scrub_diagnostic(&c, &input), format!("icecast://{}@h/m", "[credentials]"));
    }

    #[test]
    fn t7_final_diagnostic_line() {
        assert_eq!(final_diagnostic_line("a\n\n  b  \n"), "b");
        assert_eq!(final_diagnostic_line(""), "ffmpeg exited");
        let xs = "x".repeat(300);
        assert_eq!(final_diagnostic_line(&xs), "x".repeat(240));
        let es = "é".repeat(300);
        assert_eq!(final_diagnostic_line(&es), "é".repeat(240));
    }

    /// ffmpeg itself must turn the escaped URL back into the real name and password
    /// (it does since some version; a regression would only show as "wrong password").
    #[test]
    fn t9_ffmpeg_sends_the_real_credentials() {
        use base64::Engine;
        use std::io::Read;
        for (user, password) in [("dj name", "p@ss/w:rd"), ("ハチ", "pä%ss#?&+ :x"), ("source", "dj|pw")] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port().to_string();
            let config = cfg(&[
                ("server", "host", "127.0.0.1"),
                ("server", "port", &port),
                ("server", "user", user),
                ("server", "source_password", password),
            ]);
            // The panel's own command, with a test tone in place of the sink.
            let mut cmd = stream_command(&config, "unused");
            let input = cmd.iter().position(|arg| arg == "pulse").unwrap();
            cmd.splice(input..input + 2, ["lavfi".to_string(), "-i".into(), "anullsrc".into(), "-t".into(), "2".into()]);
            cmd.retain(|arg| arg != "unused.monitor");
            let mut child = std::process::Command::new(&cmd[0]).args(&cmd[1..]).stdout(std::process::Stdio::null()).spawn().unwrap();
            // A URL ffmpeg misreads sends it elsewhere: fail instead of waiting forever.
            listener.set_nonblocking(true).unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut connection = loop {
                match listener.accept() {
                    Ok((connection, _)) => break connection,
                    Err(_) if std::time::Instant::now() < deadline => std::thread::sleep(std::time::Duration::from_millis(20)),
                    Err(_) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("ffmpeg never connected for {user:?}");
                    }
                }
            };
            connection.set_nonblocking(false).unwrap();
            connection.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
            let mut head = Vec::new();
            let mut buffer = [0; 4096];
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = connection.read(&mut buffer).unwrap();
                assert!(n > 0, "ffmpeg closed before its headers");
                head.extend_from_slice(&buffer[..n]);
            }
            let _ = child.kill();
            let _ = child.wait();
            let head = String::from_utf8_lossy(&head).into_owned();
            let auth = head.lines().find_map(|line| line.strip_prefix("Authorization: Basic ")).unwrap();
            let sent = base64::prelude::BASE64_STANDARD.decode(auth.trim()).unwrap();
            assert_eq!(String::from_utf8(sent).unwrap(), format!("{user}:{password}"));
        }
    }

    #[test]
    fn t8_classify_diagnostic() {
        assert_eq!(
            classify_diagnostic("HTTP 403 Forbidden: Mountpoint in use", "radio-dev"),
            ("mount already occupied", false)
        );
        assert_eq!(
            classify_diagnostic("Server returned 401 Unauthorized", "radio-dev"),
            ("authentication failed", false)
        );
        assert_eq!(
            classify_diagnostic("Unknown encoder 'libmp3lame'", "radio-dev"),
            ("ffmpeg encoder unavailable", false)
        );
        assert_eq!(
            classify_diagnostic("pulse: No such entity", "radio-dev"),
            ("Pulse stream source unavailable", false)
        );
        assert_eq!(
            classify_diagnostic(
                "Error opening input: Invalid argument (pulse)",
                "radio-dev"
            ),
            ("Pulse stream source unavailable", false)
        );
        assert_eq!(
            classify_diagnostic("radio-dev.monitor: No such device", "radio-dev"),
            ("Pulse stream source unavailable", false)
        );
        assert_eq!(
            classify_diagnostic("Connection refused", "radio-dev"),
            ("network connection failed", true)
        );
        assert_eq!(
            classify_diagnostic("something else", "radio-dev"),
            ("stream process exited", true)
        );
        // Codes count only as numbers of their own.
        assert_eq!(
            classify_diagnostic("Connection to tcp://127.0.0.1:14031 failed: Connection refused", "radio-dev"),
            ("network connection failed", true)
        );
        assert_eq!(classify_diagnostic("Server returned 403 Forbidden", "radio-dev"), ("refused by the server (403)", false));
        assert_eq!(classify_diagnostic("Server returned 401 Unauthorized (authorization failed)", "radio-dev"), ("authentication failed", false));
        assert_eq!(classify_diagnostic("HTTP error 401", "radio-dev"), ("authentication failed", false));
    }
}
