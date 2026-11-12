#![allow(dead_code)] // until the app uses it

use std::path::Path;

pub fn read_track(path: &Path) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

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
}
