
use std::fs;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use ini::{EscapePolicy, Ini, LineSeparator, ParseOption, WriteOption};

/// Value of RADIO_PANEL_CONFIG if set and non-empty, else $HOME/.config/radio-panel.conf.
pub fn config_path() -> PathBuf {
    if let Some(p) = std::env::var("RADIO_PANEL_CONFIG").ok().filter(|s| !s.is_empty()) {
        return PathBuf::from(p);
    }
    let home = std::env::var("HOME").unwrap_or_default();
    PathBuf::from(home).join(".config").join("radio-panel.conf")
}

/// configparser-style percent escaping: in-memory values hold the real text;
/// on write every `%` becomes `%%`, on read every `%%` becomes `%`.
fn escape(s: &str) -> String {
    s.replace('%', "%%")
}

fn unescape(s: &str) -> String {
    s.replace("%%", "%")
}

fn defaults() -> Ini {
    let mut ini = Ini::new();
    let home = std::env::var("HOME").unwrap_or_default();
    let folder = format!("{home}/Radio Recordings");
    let track_file = format!("{home}/now-playing.txt");
    let rows = [
        ("server", "host", "stream.example.com"),
        ("server", "port", "1337"),
        ("server", "mount", "main.mp3"),
        ("server", "user", "source"),
        ("server", "source_password", "hackme"),
        ("server", "stream_name", ""),
        ("server", "genre", "Post-Avant Jazzcore"),
        ("server", "description", "Cave Explorer Committee"),
        ("server", "stream_url", ""),
        ("server", "public", "false"),
        ("server", "autoconnect", "false"),
        ("audio", "bitrate", "192"),
        ("audio", "format", "mp3"),
        ("audio", "monitor_enabled", "true"),
        ("audio", "silence_warning_seconds", "15"),
        ("recording", "enabled", "false"),
        ("recording", "save_titles", "false"),
        ("recording", "folder", folder.as_str()),
        ("metadata", "track_file", track_file.as_str()),
        // An MPRIS player id; empty means the track file.
        ("metadata", "player", ""),
    ];
    for (s, k, v) in rows {
        ini.with_section(Some(s)).set(k, v);
    }
    ini
}

/// Config: a `rust-ini` store of real (unescaped) text, plus its file path.
#[derive(Clone)]
pub struct Config {
    ini: Ini,
    path: PathBuf,
}

impl PartialEq for Config {
    /// The same values; the file path and the order of keys (`set` moves a key to
    /// the end) are not compared.
    fn eq(&self, other: &Config) -> bool {
        fn values(config: &Config) -> Vec<(Option<&str>, &str, &str)> {
            let mut values: Vec<_> = config.ini.iter().flat_map(|(section, props)| props.iter().map(move |(key, value)| (section, key, value))).collect();
            values.sort();
            values
        }
        values(self) == values(other)
    }
}

impl Config {
    /// Load `path`, overlaying any keys found in the file onto the defaults.
    /// A missing file yields defaults only; an unparsable file is an error.
    pub fn load(path: &Path) -> Result<Config, String> {
        let mut ini = defaults();
        if path.exists() {
            let text = fs::read_to_string(path).map_err(|e| e.to_string())?;
            let opt = ParseOption {
                enabled_quote: false,
                enabled_escape: false,
                ..Default::default()
            };
            let parsed: Ini = Ini::load_from_str_opt(&text, opt).map_err(|e| e.to_string())?;
            for (section, props) in parsed.iter() {
                let section = section.unwrap_or("DEFAULT");
                for (key, value) in props.iter() {
                    ini.with_section(Some(section)).set(key, unescape(value));
                }
            }
        }
        Ok(Config {
            ini,
            path: path.to_path_buf(),
        })
    }

    /// Stored value, or `""` for an unknown key.
    pub fn get(&self, section: &str, key: &str) -> &str {
        self.ini
            .get_from(Some(section), key)
            .unwrap_or_default()
    }

    /// Store `value` as the real (unescaped) text for `section`.`key`.
    pub fn set(&mut self, section: &str, key: &str, value: &str) {
        self.ini
            .with_section(Some(section))
            .set(key, value);
    }

    /// Atomically write to `self.path`: a 0600 temp file in the same directory,
    /// then rename over the target. The temp file is removed on any error.
    pub fn save(&self) -> std::io::Result<()> {
        let path = &self.path;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let pid = std::process::id();
        let dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
        // Named after the target too: two configs saved at once must not share it.
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        let temp = dir.join(format!(".{name}.{pid}"));

        // Build an escaped copy for the file (in-memory holds the real text).
        let mut out = Ini::new();
        for (section, props) in self.ini.iter() {
            for (key, value) in props.iter() {
                out.with_section(section).set(key, escape(value));
            }
        }

        let written = (|| -> std::io::Result<()> {
            let mut f = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temp)?;
            out.write_to_opt(
                &mut f,
                WriteOption {
                    escape_policy: EscapePolicy::Nothing,
                    line_separator: LineSeparator::CR,
                    kv_separator: " = ",
                },
            )?;
            f.sync_all()
        })();

        let result = written.and_then(|()| fs::rename(&temp, path));
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "radio-panel-rs-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn cleanup(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn missing_file_gives_defaults() {
        let dir = temp_dir("defaults");
        let path = dir.join("nonexistent.conf");
        let cfg = Config::load(&path).unwrap();
        assert_eq!(cfg.get("server", "port"), "1337");
        assert_eq!(cfg.get("audio", "format"), "mp3");
        assert_eq!(cfg.get("server", "no_such_key"), "");
        let home = std::env::var("HOME").unwrap();
        assert_eq!(cfg.get("recording", "folder"), format!("{home}/Radio Recordings"));
        assert_eq!(cfg.get("metadata", "track_file"), format!("{home}/now-playing.txt"));
        cleanup(&dir);
    }

    #[test]
    fn save_creates_0600_and_replaces() {
        let dir = temp_dir("save");
        let path = dir.join("sub").join("radio-panel.conf");
        let mut cfg = Config::load(&path).unwrap();
        cfg.set("server", "port", "8888");
        cfg.save().unwrap();

        // mode is exactly 0600
        let meta = fs::metadata(&path).unwrap();
        let mode = meta.permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "mode was {mode:#o}");

        // the directory contains exactly one entry (no leftover temp file)
        let entries: Vec<String> = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec!["sub".to_string()]);

        let sub_entries: Vec<String> = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(sub_entries, vec!["radio-panel.conf".to_string()]);

        // a second save with a changed value replaces it
        let mut cfg = Config::load(&path).unwrap();
        cfg.set("server", "port", "9999");
        cfg.save().unwrap();
        let cfg = Config::load(&path).unwrap();
        assert_eq!(cfg.get("server", "port"), "9999");
        cleanup(&dir);
    }

    #[test]
    fn round_trip_percent_escape() {
        let dir = temp_dir("roundtrip");
        let path = dir.join("radio-panel.conf");
        let mut cfg = Config::load(&path).unwrap();
        cfg.set("server", "source_password", "p%ss\\w\"or'd ;# =x");
        cfg.save().unwrap();

        let cfg = Config::load(&path).unwrap();
        assert_eq!(cfg.get("server", "source_password"), "p%ss\\w\"or'd ;# =x");

        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("%%"), "file text should contain %%, got: {text}");
        cleanup(&dir);
    }

    #[test]
    fn reads_python_written_input() {
        let dir = temp_dir("python-written");
        let path = dir.join("radio-panel.conf");
        fs::write(
            &path,
            "[server]\nhost = 127.0.0.1\nsource_password = a%%b\nstream_url = \n",
        )
        .unwrap();

        let cfg = Config::load(&path).unwrap();
        assert_eq!(cfg.get("server", "stream_url"), "");
        assert_eq!(cfg.get("server", "host"), "127.0.0.1");
        assert_eq!(cfg.get("server", "source_password"), "a%b");
        assert_eq!(cfg.get("server", "port"), "1337");
        cleanup(&dir);
    }

    #[test]
    fn python_can_read_our_output() {
        let dir = temp_dir("python-read");
        let path = dir.join("radio-panel.conf");
        let mut cfg = Config::load(&path).unwrap();
        cfg.set("server", "source_password", "100%_\"q\"\\x");
        cfg.save().unwrap();

        let out = Command::new("/usr/bin/python3")
            .arg("-c")
            .arg(
                "import configparser,sys; p=configparser.ConfigParser(); \
                 p.read(sys.argv[1]); sys.stdout.write(p.get('server','source_password'))",
            )
            .arg(&path)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "python exited non-zero: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&out.stdout), "100%_\"q\"\\x");
        cleanup(&dir);
    }
}
