//! Track titles from a media player over MPRIS (D-Bus), as an alternative to the
//! track file.

use std::collections::HashMap;

use zbus::blocking::Connection;
use zbus::zvariant::OwnedValue;

const PREFIX: &str = "org.mpris.MediaPlayer2.";

/// The player id in a bus name: `org.mpris.MediaPlayer2.chromium.instance20954` is
/// `chromium`. Players that run more than once add `.instance<pid>`.
fn player_id(bus_name: &str) -> Option<&str> {
    let id = bus_name.strip_prefix(PREFIX)?;
    let id = match id.split_once(".instance") {
        Some((id, pid)) if pid.chars().all(|c| c.is_ascii_digit()) => id,
        _ => id,
    };
    (!id.is_empty()).then_some(id)
}

fn bus_names(connection: &Connection) -> zbus::Result<Vec<String>> {
    let reply = connection.call_method(
        Some("org.freedesktop.DBus"),
        "/org/freedesktop/DBus",
        Some("org.freedesktop.DBus"),
        "ListNames",
        &(),
    )?;
    reply.body().deserialize()
}

/// The players on the session bus by id, sorted, each once.
pub fn players() -> Vec<String> {
    let names = Connection::session().and_then(|connection| bus_names(&connection)).unwrap_or_default();
    let mut ids: Vec<String> = names.iter().filter_map(|name| player_id(name)).map(str::to_string).collect();
    ids.sort();
    ids.dedup();
    ids
}

/// "Artist - Title" from a player's state; `None` when it plays nothing.
fn title_from(status: &str, metadata: &HashMap<String, OwnedValue>) -> Option<String> {
    if status == "Stopped" {
        // AIMP keeps "-" as title and artist while stopped.
        return None;
    }
    let text = |key: &str| metadata.get(key).and_then(|value| String::try_from(value.try_clone().ok()?).ok());
    let title = text("xesam:title").unwrap_or_default();
    let artists: Vec<String> = metadata
        .get("xesam:artist")
        .and_then(|value| Vec::<String>::try_from(value.try_clone().ok()?).ok())
        .unwrap_or_default();
    let artist = artists.iter().map(|a| a.trim()).filter(|a| !a.is_empty()).collect::<Vec<_>>().join(", ");
    match (artist.as_str(), title.trim()) {
        (_, "") => None,
        ("", title) => Some(title.to_string()),
        (artist, title) => Some(format!("{artist} - {title}")),
    }
}

/// The title the first running player with this id plays now. An error means the
/// bus itself failed; a missing or broken player is `Ok(None)`.
pub fn current_title(connection: &Connection, id: &str) -> zbus::Result<Option<String>> {
    #[cfg(test)]
    if let Some(title) = tests::FAKE_PLAYERS.lock().unwrap().get(id) {
        return Ok(title.clone());
    }
    let Some(name) = bus_names(connection)?.into_iter().find(|name| player_id(name) == Some(id)) else {
        return Ok(None);
    };
    Ok(player_title(connection, &name))
}

fn player_title(connection: &Connection, name: &str) -> Option<String> {
    let reply = connection
        .call_method(
            Some(name),
            "/org/mpris/MediaPlayer2",
            Some("org.freedesktop.DBus.Properties"),
            "GetAll",
            &("org.mpris.MediaPlayer2.Player",),
        )
        .ok()?;
    let properties: HashMap<String, OwnedValue> = reply.body().deserialize().ok()?;
    let status = properties.get("PlaybackStatus").and_then(|v| String::try_from(v.try_clone().ok()?).ok())?;
    let metadata: HashMap<String, OwnedValue> =
        properties.get("Metadata").and_then(|v| HashMap::try_from(v.try_clone().ok()?).ok()).unwrap_or_default();
    title_from(&status, &metadata)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Players tests make up, by id (each test its own id), instead of D-Bus.
    pub(crate) static FAKE_PLAYERS: std::sync::Mutex<std::collections::BTreeMap<String, Option<String>>> =
        std::sync::Mutex::new(std::collections::BTreeMap::new());

    use zbus::zvariant::Value;

    #[test]
    fn x1_player_id() {
        assert_eq!(player_id("org.mpris.MediaPlayer2.AIMP"), Some("AIMP"));
        assert_eq!(player_id("org.mpris.MediaPlayer2.chromium.instance20954"), Some("chromium"));
        assert_eq!(player_id("org.mpris.MediaPlayer2.kdeconnect.mpris_000001"), Some("kdeconnect.mpris_000001"));
        assert_eq!(player_id("org.mpris.MediaPlayer2."), None);
        assert_eq!(player_id("org.kde.StatusNotifierItem-1-1"), None);
    }

    #[test]
    fn x2_title_from_metadata() {
        let metadata = |title: &str, artists: &[&str]| {
            let artists: Vec<String> = artists.iter().map(|a| a.to_string()).collect();
            HashMap::from([
                ("xesam:title".to_string(), OwnedValue::try_from(Value::from(title)).unwrap()),
                ("xesam:artist".to_string(), OwnedValue::try_from(Value::from(artists)).unwrap()),
            ])
        };
        let m = metadata("砂の惑星", &["ハチ", " 初音ミク "]);
        assert_eq!(title_from("Playing", &m), Some("ハチ, 初音ミク - 砂の惑星".to_string()));
        // Paused keeps the title; stopped has none (AIMP then reports "-" for both).
        assert_eq!(title_from("Paused", &m), Some("ハチ, 初音ミク - 砂の惑星".to_string()));
        assert_eq!(title_from("Stopped", &metadata("-", &["-"])), None);
        assert_eq!(title_from("Playing", &metadata(" Intro ", &[])), Some("Intro".to_string()));
        assert_eq!(title_from("Playing", &metadata(" Intro ", &["", " "])), Some("Intro".to_string()));
        assert_eq!(title_from("Playing", &metadata("", &["Artist"])), None);
        assert_eq!(title_from("Playing", &HashMap::new()), None);
    }
}
