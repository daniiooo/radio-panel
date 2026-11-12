#![allow(dead_code)] // until the app uses it

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

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> String {
        s.to_string()
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
