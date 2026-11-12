//! The windows, the tray, and the glue between them and the backend threads.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use iced::futures::channel::{mpsc, oneshot};
use iced::futures::{SinkExt, Stream, StreamExt};
use iced::widget::{Column, button, canvas, checkbox, column, container, pick_list, row, scrollable, slider, text, text_input};
use iced::{Center, Color, Element, Fill, Font, Point, Rectangle, Renderer, Size, Subscription, Task, Theme};
use iced::{keyboard, mouse, window};
use ksni::TrayMethods;

use crate::config::{Config, config_path};
use crate::spectrum::{BANDS, Spectrum};
use crate::{meta, pw, recorder, streamer};

const GREEN: Color = Color::from_rgb8(0x2f, 0x9e, 0x44);
const YELLOW: Color = Color::from_rgb8(0xf2, 0xc0, 0x37);
const ORANGE: Color = Color::from_rgb8(0xe8, 0x59, 0x0c);
const RED: Color = Color::from_rgb8(0xe0, 0x31, 0x31);
const BOLD: Font = Font { weight: iced::font::Weight::Bold, ..Font::DEFAULT };

/// Upload rate samples kept for the network graph: 60 s at one per 200 ms.
const HISTORY: usize = 300;
/// the old client's "Poor Connection" and "Connection Lost" shares of the target bitrate.
const POOR: f64 = 0.92;
const DROP: f64 = 0.78;
/// How often deadlines and the silence warning are checked.
const TICK: Duration = Duration::from_millis(500);
/// Longest wait at quit for ffmpeg and the sink to end (the Python app's limit). Past
/// it the process exits anyway; the server then removes the sink and ffmpeg gets
/// SIGTERM, but players on the sink get no fallback links.
const QUIT_TIMEOUT: Duration = Duration::from_secs(7);

/// Inner padding of buttons and lists; the window has many rows.
const PADDING: [u16; 2] = [4, 10];

const BITRATES: [&str; 4] = ["128", "192", "256", "320"];
const FORMATS: [(&str, &str); 3] = [("mp3", "MP3"), ("opus", "Opus"), ("vorbis", "Vorbis")];
/// Settings that are stored without surrounding whitespace.
const TRIMMED: [(&str, &str); 9] = [
    ("server", "host"),
    ("server", "mount"),
    ("server", "user"),
    ("server", "stream_name"),
    ("server", "genre"),
    ("server", "description"),
    ("server", "stream_url"),
    ("metadata", "track_file"),
    ("recording", "folder"),
];

/// An entry of a drop-down list.
#[derive(Clone, PartialEq)]
pub struct Choice {
    value: String,
    label: String,
}

impl std::fmt::Display for Choice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.label)
    }
}

#[derive(Clone)]
pub enum Message {
    ToggleMain,
    ToggleGraph,
    ToggleDetails,
    Closed(window::Id),
    Quit,
    Tray(ksni::Handle<PanelTray>),
    NoTray,
    SystemTheme(iced::theme::Mode),
    Tick,
    Frame,
    Pw(pw::Event),
    Stream(streamer::Event),
    Meta(meta::Event),
    Recording(String),
    ToggleLive,
    /// A settings field was edited: section, key, new value.
    Field(&'static str, &'static str, String),
    Save,
    Monitor(bool),
    Output(Choice),
    StreamVolume(u32),
    LocalVolume(u32),
    BrowseTrackFile,
    BrowseFolder,
    ManualTitle(String),
    SendTitle,
    ReturnToAutomatic,
}

/// The backend threads. Dropped in this order at quit: the stream ends first, the
/// sink last.
struct Backend {
    streamer: streamer::Handle,
    recorder: recorder::Handle,
    pusher: meta::Handle,
    pw: pw::Handle,
}

struct App {
    /// The settings in effect, as saved.
    config: Config,
    /// The settings as edited in the window; they take effect with "Save settings".
    form: Config,
    backend: Option<Backend>,
    /// Quit is under way; every later message is ignored.
    quitting: bool,
    /// The desktop asks for a dark color scheme.
    dark: bool,
    tray: Option<ksni::Handle<PanelTray>>,
    main: Option<window::Id>,
    graph: Option<window::Id>,
    details: Option<window::Id>,

    status: String,
    live: bool,
    error: bool,
    want_live: bool,
    /// Shown instead of the status while live: poor connection or no audio.
    warning: Option<&'static str>,
    silence_shown: bool,
    net_poor: bool,
    diagnostic: String,
    connected_at: Instant,
    last_signal: Instant,
    autoconnect_at: Option<Instant>,
    history: VecDeque<f64>,

    track: String,
    manual_title: String,
    override_active: bool,
    metadata_status: String,
    recording_status: String,
    save_hint: String,
    save_hint_until: Option<Instant>,

    audio_ready: bool,
    stream_volume: Option<u32>,
    local_volume: Option<u32>,
    /// Last volume change made here; the server's echoes of a drag are ignored briefly.
    volume_touched: Option<Instant>,
    outputs: Vec<(String, String)>,
    current_output: Option<String>,

    spectrum: Spectrum,
    clock: Instant,
}

/// Backend events on their way to the subscription that delivers them.
static EVENTS: Mutex<Option<mpsc::UnboundedReceiver<Message>>> = Mutex::new(None);

fn trace(what: &str) {
    if std::env::var_os("RADIO_PANEL_TRACE").is_some() {
        eprintln!("{what}");
    }
}

fn forward<E: 'static>(tx: &mpsc::UnboundedSender<Message>, wrap: fn(E) -> Message) -> impl Fn(E) + Send + 'static + use<E> {
    let tx = tx.clone();
    move |event| drop(tx.unbounded_send(wrap(event)))
}

fn boot() -> (App, Task<Message>) {
    let config = Config::load(&config_path()).unwrap_or_else(|error| {
        eprintln!("radio-panel: {error}");
        std::process::exit(1);
    });
    let (tx, rx) = mpsc::unbounded();
    *EVENTS.lock().unwrap() = Some(rx);
    let sink = pw::sink_name();
    let samples = tx.clone();
    let backend = Backend {
        streamer: streamer::spawn(&sink, forward(&tx, Message::Stream)),
        recorder: recorder::spawn(&sink, forward(&tx, Message::Recording)),
        pusher: meta::spawn(Box::new(config.clone()), forward(&tx, Message::Meta)),
        // Silence arrives as buffers of zeros; they would only wake the UI.
        pw: pw::spawn(&sink, move |event| {
            if !matches!(&event, pw::Event::Samples(samples) if samples.iter().all(|sample| *sample == 0.0)) {
                let _ = samples.unbounded_send(Message::Pw(event));
            }
        }),
    };
    let hidden = std::env::args().any(|argument| argument == "--hidden");
    let show = if hidden { Task::none() } else { Task::done(Message::ToggleMain) };
    (App::new(config, Some(backend)), Task::batch([iced::system::theme().map(Message::SystemTheme), show]))
}

/// Run `teardown` on a thread of its own and wait for it, at most `limit`. Whether it
/// finished in time; if not, it is left running.
fn finished_within(limit: Duration, teardown: impl FnOnce() + Send + 'static) -> bool {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        teardown();
        let _ = tx.send(());
    });
    rx.recv_timeout(limit).is_ok()
}

/// Open the window of `slot`, or close it if it is open.
fn toggle(slot: &mut Option<window::Id>, name: &str, size: Size, min_size: Size) -> Task<Message> {
    match slot.take() {
        Some(id) => {
            trace(&format!("{name}_HIDE"));
            window::close(id)
        }
        None => {
            let (id, task) = window::open(window::Settings {
                size,
                min_size: Some(min_size),
                platform_specific: window::settings::PlatformSpecific { application_id: "radio-panel".into(), ..Default::default() },
                ..Default::default()
            });
            *slot = Some(id);
            trace(&format!("{name}_SHOW"));
            task.discard()
        }
    }
}

impl App {
    fn new(config: Config, backend: Option<Backend>) -> App {
        let now = Instant::now();
        App {
            form: config.clone(),
            config,
            backend,
            quitting: false,
            dark: false,
            tray: None,
            main: None,
            graph: None,
            details: None,
            status: "Off air".into(),
            live: false,
            error: false,
            want_live: false,
            warning: None,
            silence_shown: false,
            net_poor: false,
            diagnostic: String::new(),
            connected_at: now,
            last_signal: now,
            autoconnect_at: None,
            history: VecDeque::new(),
            track: "-".into(),
            manual_title: String::new(),
            override_active: false,
            metadata_status: "Disabled".into(),
            recording_status: "Recording disabled".into(),
            save_hint: String::new(),
            save_hint_until: None,
            audio_ready: false,
            stream_volume: None,
            local_volume: None,
            volume_touched: None,
            outputs: Vec::new(),
            current_output: None,
            spectrum: Spectrum::new(pw::CAPTURE_RATE),
            clock: now,
        }
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        if self.quitting {
            return Task::none();
        }
        match message {
            Message::ToggleMain => return toggle(&mut self.main, "MAIN", Size::new(680.0, 900.0), Size::new(640.0, 300.0)),
            Message::ToggleGraph => return toggle(&mut self.graph, "GRAPH", Size::new(680.0, 200.0), Size::new(600.0, 120.0)),
            Message::ToggleDetails => return toggle(&mut self.details, "DETAILS", Size::new(640.0, 320.0), Size::new(320.0, 160.0)),
            Message::Closed(id) => {
                // Also reached by the title-bar close button: the panel stays in the tray.
                for (slot, name) in [(&mut self.main, "MAIN"), (&mut self.graph, "GRAPH"), (&mut self.details, "DETAILS")] {
                    if *slot == Some(id) {
                        *slot = None;
                        trace(&format!("{name}_CLOSED"));
                    }
                }
            }
            Message::Quit => {
                trace("QUIT");
                self.quitting = true;
                // Dropping the backend waits for ffmpeg and the sink: not on this thread.
                let backend = self.backend.take();
                let (done, ended) = oneshot::channel();
                std::thread::spawn(move || {
                    let finished = finished_within(QUIT_TIMEOUT, move || drop(backend));
                    trace(if finished { "QUIT_DONE" } else { "QUIT_TIMEOUT" });
                    let _ = done.send(());
                });
                // The panel is gone from the screen at once; the process follows.
                let windows = [self.main, self.graph, self.details].into_iter().flatten().map(window::close);
                let tray = self.tray.take().map(|tray| Task::future(tray.shutdown()).discard());
                return Task::batch(windows.chain(tray).chain([Task::future(ended).then(|_| iced::exit())]));
            }
            Message::Tray(handle) => {
                self.tray = Some(handle);
                return self.update_tray();
            }
            // Without a tray a hidden panel could never be shown.
            Message::NoTray if self.main.is_none() => return self.update(Message::ToggleMain),
            Message::NoTray => {}
            // Opening a window on Wayland reports "no preference" (iced 0.14.1 then falls
            // back to light); the desktop's last real answer stands.
            Message::SystemTheme(iced::theme::Mode::None) => {}
            Message::SystemTheme(mode) => self.dark = mode == iced::theme::Mode::Dark,
            Message::Tick => return self.tick(),
            Message::Frame => {
                self.spectrum.advance(self.clock.elapsed().as_secs_f64());
            }
            Message::Pw(event) => return self.audio_event(event),
            Message::Stream(event) => return self.stream_event(event),
            Message::Meta(meta::Event::Status(status)) => self.metadata_status = status,
            Message::Meta(meta::Event::Track(track)) => {
                if track != "-" {
                    self.record(recorder::Command::SetTitle(track.clone()));
                }
                if !self.override_active {
                    self.manual_title = if track == "-" { String::new() } else { track.clone() };
                }
                self.track = track;
            }
            Message::Recording(status) => self.recording_status = status,
            Message::ToggleLive => {
                if self.autoconnect_at.take().is_some() {
                    return self.set_status("Auto-connect cancelled".into(), false, false);
                }
                self.stream(if self.want_live { streamer::Command::Stop } else { streamer::Command::GoLive(Box::new(self.config.clone())) });
            }
            Message::Field(section, key, mut value) => {
                if matches!(key, "port" | "silence_warning_seconds") {
                    value.retain(|character| character.is_ascii_digit());
                }
                self.form.set(section, key, &value);
            }
            Message::Save => self.save(),
            Message::Monitor(on) => {
                // Saved at once, without the other fields being edited.
                for config in [&mut self.config, &mut self.form] {
                    config.set("audio", "monitor_enabled", &on.to_string());
                }
                let _ = self.config.save();
                if self.audio_ready {
                    self.audio(pw::Command::Monitor(on));
                }
                if !on {
                    self.local_volume = None;
                }
            }
            Message::Output(choice) => self.audio(pw::Command::Output(choice.value)),
            Message::StreamVolume(percent) => {
                self.stream_volume = Some(percent);
                self.volume_touched = Some(Instant::now());
                self.audio(pw::Command::StreamVolume(percent));
            }
            Message::LocalVolume(percent) => {
                self.local_volume = Some(percent);
                self.volume_touched = Some(Instant::now());
                self.audio(pw::Command::LocalVolume(percent));
            }
            Message::BrowseTrackFile => {
                let dialog = rfd::AsyncFileDialog::new()
                    .set_title("Track info file")
                    .set_directory(std::env::var("HOME").unwrap_or_default())
                    .add_filter("Text files", &["txt"])
                    .add_filter("All files", &["*"]);
                return Task::future(dialog.pick_file()).and_then(|file| Task::done(picked("metadata", "track_file", file)));
            }
            Message::BrowseFolder => {
                let folder = match self.form.get("recording", "folder") {
                    "" => std::env::var("HOME").unwrap_or_default(),
                    folder => folder.to_string(),
                };
                let dialog = rfd::AsyncFileDialog::new().set_title("Recording folder").set_directory(folder);
                return Task::future(dialog.pick_folder()).and_then(|folder| Task::done(picked("recording", "folder", folder)));
            }
            Message::ManualTitle(title) => self.manual_title = title,
            Message::SendTitle => {
                // The title in the box becomes the override, even if it is the automatic one.
                let title = self.manual_title.trim().to_string();
                if !title.is_empty() {
                    self.override_active = true;
                    self.push(meta::Command::SetManualTitle(title.clone()));
                }
                if title.is_empty() || title == self.track {
                    self.push(meta::Command::SendNow);
                }
            }
            Message::ReturnToAutomatic => {
                self.override_active = false;
                self.push(meta::Command::ReturnToAutomatic);
            }
        }
        Task::none()
    }

    fn stream(&self, command: streamer::Command) {
        if let Some(backend) = &self.backend {
            backend.streamer.send(command);
        }
    }

    fn record(&self, command: recorder::Command) {
        if let Some(backend) = &self.backend {
            backend.recorder.send(command);
        }
    }

    fn push(&self, command: meta::Command) {
        if let Some(backend) = &self.backend {
            backend.pusher.send(command);
        }
    }

    fn audio(&self, command: pw::Command) {
        if let Some(backend) = &self.backend {
            backend.pw.send(command);
        }
    }

    fn setting_is(&self, section: &str, key: &str, value: &str) -> bool {
        self.config.get(section, key) == value
    }

    fn target_kbps(&self) -> f64 {
        self.config.get("audio", "bitrate").parse().unwrap_or(192.0_f64).max(1.0)
    }

    fn set_status(&mut self, status: String, live: bool, error: bool) -> Task<Message> {
        (self.status, self.live, self.error) = (status, live, error);
        self.warning = None;
        self.silence_shown = false;
        self.net_poor = false;
        if !live {
            self.history.clear();
        }
        self.update_tray()
    }

    fn update_tray(&self) -> Task<Message> {
        let Some(handle) = self.tray.clone() else { return Task::none() };
        let (live, wanted, status) = (self.live, self.want_live, self.status.clone());
        Task::future(async move {
            handle.update(|tray| (tray.live, tray.wanted, tray.status) = (live, wanted, status)).await;
        })
        .discard()
    }

    fn should_autoconnect(&self) -> bool {
        self.audio_ready
            && self.setting_is("server", "autoconnect", "true")
            && !self.config.get("server", "host").trim().is_empty()
            && !self.config.get("server", "mount").trim().is_empty()
            && !self.want_live
    }

    fn audio_event(&mut self, event: pw::Event) -> Task<Message> {
        let echo_of_drag = self.volume_touched.is_some_and(|touched| touched.elapsed() < Duration::from_millis(300));
        match event {
            pw::Event::Ready => {
                self.audio_ready = true;
                if self.setting_is("audio", "monitor_enabled", "true") {
                    self.audio(pw::Command::Monitor(true));
                }
                if self.should_autoconnect() {
                    self.autoconnect_at = Some(Instant::now() + Duration::from_millis(1500));
                    return self.set_status("Auto-connect pending...".into(), false, false);
                }
            }
            pw::Event::StreamVolume(percent) if !echo_of_drag => self.stream_volume = Some(percent),
            pw::Event::LocalVolume(percent) if !echo_of_drag && self.setting_is("audio", "monitor_enabled", "true") => {
                self.local_volume = Some(percent);
            }
            pw::Event::StreamVolume(_) | pw::Event::LocalVolume(_) => {}
            pw::Event::Outputs { sinks, current } => (self.outputs, self.current_output) = (sinks, current),
            pw::Event::Samples(samples) => {
                if samples.iter().any(|sample| sample.abs() > 0.005) {
                    self.last_signal = Instant::now();
                }
                // Nobody sees the spectrum while the window is hidden.
                if self.main.is_some() {
                    self.spectrum.add_samples(&samples, self.clock.elapsed().as_secs_f64());
                }
            }
            pw::Event::Failed(error) => {
                self.audio_ready = false;
                (self.stream_volume, self.local_volume) = (None, None);
                return self.set_status(format!("Audio failed: {error}"), false, true);
            }
        }
        Task::none()
    }

    fn stream_event(&mut self, event: streamer::Event) -> Task<Message> {
        match event {
            streamer::Event::State { text, live, error, wanted } => {
                self.want_live = wanted;
                return self.set_status(text, live, error);
            }
            streamer::Event::Rate(kbps) => self.rate_sample(kbps),
            // Metadata and the recording follow the stream the server is receiving; a
            // started ffmpeg is not proof of that.
            streamer::Event::Connected(connected) => {
                self.push(meta::Command::SetEnabled(connected));
                if connected {
                    self.connected_at = Instant::now();
                    self.record(recorder::Command::Start(Box::new(self.config.clone())));
                    if self.track != "-" {
                        self.record(recorder::Command::SetTitle(self.track.clone()));
                    }
                } else {
                    self.record(recorder::Command::Stop);
                }
            }
            streamer::Event::Diagnostic(detail) => self.diagnostic = detail,
        }
        Task::none()
    }

    fn rate_sample(&mut self, kbps: f64) {
        if !self.want_live {
            return;
        }
        if self.history.len() == HISTORY {
            self.history.pop_front();
        }
        self.history.push_back(kbps);
        // The warning only makes sense for constant-bitrate MP3: the other codecs dip
        // far below the target on quiet audio.
        if !self.setting_is("audio", "format", "mp3") || !self.live || self.connected_at.elapsed() < Duration::from_secs(10) {
            return;
        }
        // The last five seconds.
        if self.history.len() < 25 {
            return;
        }
        let average = self.history.iter().rev().take(25).sum::<f64>() / 25.0;
        if average < self.target_kbps() * DROP && !self.net_poor {
            self.net_poor = true;
            self.warning = Some("LIVE - poor connection!");
        } else if self.net_poor && average > self.target_kbps() * POOR {
            self.clear_warning();
        }
    }

    fn clear_warning(&mut self) {
        self.warning = None;
        self.silence_shown = false;
        self.net_poor = false;
    }

    fn tick(&mut self) -> Task<Message> {
        let now = Instant::now();
        if self.save_hint_until.is_some_and(|until| now >= until) {
            self.save_hint_until = None;
            self.save_hint.clear();
        }
        self.check_silence();
        if self.autoconnect_at.is_some_and(|at| now >= at) {
            self.autoconnect_at = None;
            if self.should_autoconnect() {
                self.stream(streamer::Command::GoLive(Box::new(self.config.clone())));
            }
        }
        Task::none()
    }

    fn check_silence(&mut self) {
        let threshold = self.config.get("audio", "silence_warning_seconds").parse().unwrap_or(15).min(600);
        if threshold == 0 || !self.want_live || !self.live {
            self.silence_shown = false;
            return;
        }
        let silent = self.last_signal.elapsed() > Duration::from_secs(threshold);
        if silent && !self.silence_shown {
            self.silence_shown = true;
            self.warning = Some("LIVE - no audio detected!");
        } else if !silent && self.silence_shown {
            self.clear_warning();
        }
    }

    fn save(&mut self) {
        for (section, key) in TRIMMED {
            let trimmed = self.form.get(section, key).trim().to_string();
            self.form.set(section, key, &trimmed);
        }
        if self.form.get("server", "user").is_empty() {
            self.form.set("server", "user", "source");
        }
        if !self.form.get("server", "port").parse::<u16>().is_ok_and(|port| port > 0) {
            let previous = self.config.get("server", "port").to_string();
            self.form.set("server", "port", &previous);
        }
        let silence = self.form.get("audio", "silence_warning_seconds").parse::<u32>().unwrap_or(15).min(600);
        self.form.set("audio", "silence_warning_seconds", &silence.to_string());
        self.config = self.form.clone();
        self.save_hint = match self.config.save() {
            Err(error) => format!("Not saved: {error}"),
            Ok(()) if self.want_live => "Saved - restart the stream to apply".into(),
            Ok(()) => "Saved".into(),
        };
        self.save_hint_until = Some(Instant::now() + Duration::from_secs(4));
        self.push(meta::Command::Configure(Box::new(self.config.clone())));
    }

    fn theme(&self, _id: window::Id) -> Theme {
        if self.dark { Theme::Dark } else { Theme::Light }
    }

    fn title(&self, id: window::Id) -> String {
        if self.graph == Some(id) {
            "Radio Panel - Network".into()
        } else if self.details == Some(id) {
            "Radio Panel - Connection details".into()
        } else {
            "Radio Panel".into()
        }
    }

    fn view(&self, id: window::Id) -> Element<'_, Message> {
        if self.graph == Some(id) {
            let graph = Graph { history: &self.history, target: self.target_kbps() };
            canvas(graph).width(Fill).height(Fill).into()
        } else if self.details == Some(id) {
            let details = if self.diagnostic.is_empty() { "No recent connection details." } else { &self.diagnostic };
            scrollable(container(text(details)).padding(12).width(Fill)).into()
        } else {
            self.view_main()
        }
    }

    fn field(&self, section: &'static str, key: &'static str) -> iced::widget::TextInput<'_, Message> {
        text_input("", self.form.get(section, key)).padding(4).on_input(move |value| Message::Field(section, key, value))
    }

    fn flag<'a>(&self, label: &'a str, section: &'static str, key: &'static str) -> iced::widget::Checkbox<'a, Message> {
        checkbox(self.form.get(section, key) == "true").label(label).on_toggle(move |on| Message::Field(section, key, on.to_string()))
    }

    fn view_main(&self) -> Element<'_, Message> {
        let (status, color) = match self.warning {
            Some(warning) => (warning, Some(ORANGE)),
            None if self.live => (self.status.as_str(), Some(GREEN)),
            None => (self.status.as_str(), self.error.then_some(RED)),
        };
        let header = row![
            text(status).font(BOLD).color_maybe(color).width(Fill),
            action("Network").on_press(Message::ToggleGraph),
            action("Details").on_press_maybe((!self.diagnostic.is_empty()).then_some(Message::ToggleDetails)),
            if self.want_live {
                button("Stop").padding(PADDING).style(button::danger).on_press(Message::ToggleLive)
            } else {
                button("Go Live").padding(PADDING).on_press(Message::ToggleLive)
            },
        ]
        .spacing(8)
        .align_y(Center);

        let outputs: Vec<Choice> = self.outputs.iter().map(|(name, description)| Choice { value: name.clone(), label: description.clone() }).collect();
        let current = outputs.iter().find(|choice| Some(&choice.value) == self.current_output.as_ref()).cloned();
        let hint = format!(
            "Players see an output named \"{}\" while the panel runs - anything routed there goes on stream.",
            pw::sink_description(&pw::sink_name())
        );
        let devices = column![
            text(hint).style(text::secondary),
            labeled(
                "Local monitor",
                row![
                    checkbox(self.form.get("audio", "monitor_enabled") == "true").on_toggle(Message::Monitor),
                    pick_list(outputs, current, Message::Output).padding(PADDING).width(Fill),
                ]
                .spacing(8)
                .align_y(Center),
            ),
        ];

        let volumes = column![
            labeled("Stream", volume(self.stream_volume, "--", Message::StreamVolume)),
            labeled("Local", volume(self.local_volume, "off", Message::LocalVolume)),
        ];

        let formats: Vec<Choice> = FORMATS.iter().map(|(value, label)| Choice { value: value.to_string(), label: label.to_string() }).collect();
        let format = formats.iter().find(|choice| choice.value == self.form.get("audio", "format")).or(formats.first()).cloned();
        let bitrate = BITRATES.into_iter().find(|bitrate| *bitrate == self.form.get("audio", "bitrate"));
        let server = column![
            labeled("Host / IP", self.field("server", "host")),
            labeled("Port", self.field("server", "port").width(100)),
            labeled("Mount", self.field("server", "mount")),
            labeled("Username", text_input("source", self.form.get("server", "user")).padding(4).on_input(|value| Message::Field("server", "user", value))),
            labeled("Password", self.field("server", "source_password").secure(true)),
            labeled("Format", pick_list(formats, format, |choice: Choice| Message::Field("audio", "format", choice.value)).padding(PADDING)),
            labeled("Bitrate (kbps)", pick_list(BITRATES, bitrate, |bitrate: &str| Message::Field("audio", "bitrate", bitrate.to_string())).padding(PADDING)),
            labeled("Stream name", self.field("server", "stream_name")),
            labeled("Genre", self.field("server", "genre")),
            labeled("Description", self.field("server", "description")),
            labeled("Stream URL", self.field("server", "stream_url")),
            labeled("Silence warning (s, 0 off)", self.field("audio", "silence_warning_seconds").width(100)),
            labeled("", self.flag("List in public directory", "server", "public")),
            labeled("", self.flag("Connect automatically on startup", "server", "autoconnect")),
        ];

        let track_info = column![
            row![self.field("metadata", "track_file"), action("Browse...").on_press(Message::BrowseTrackFile)].spacing(8),
            row![
                text_input("Automatic title", &self.manual_title).padding(4).on_input(Message::ManualTitle).on_submit(Message::SendTitle),
                action("Send now").on_press(Message::SendTitle),
                action("Return to automatic").on_press_maybe(self.override_active.then_some(Message::ReturnToAutomatic)),
            ]
            .spacing(8),
            text(if self.override_active { "Manual override active" } else { "Automatic title" }),
            text(self.metadata_status.as_str()),
        ];

        let recording = column![
            labeled("", self.flag("Record a local encoded copy", "recording", "enabled")),
            labeled(
                "Folder",
                row![self.field("recording", "folder"), action("Browse...").on_press(Message::BrowseFolder)].spacing(8),
            ),
            labeled("Status", text(self.recording_status.as_str())),
        ];

        let content = column![
            header,
            text(self.track.as_str()),
            canvas(Bars(&self.spectrum)).width(Fill).height(72),
            group("Devices", devices),
            group("Volumes", volumes),
            group("Server", server),
            group("Track info", track_info),
            group("Local recording", recording),
            row![text(self.save_hint.as_str()).width(Fill), button("Save settings").padding(PADDING).on_press(Message::Save)].spacing(8).align_y(Center),
        ]
        .spacing(10)
        .padding(12);
        scrollable(content).into()
    }

    fn subscription(&self) -> Subscription<Message> {
        let mut subscriptions = vec![
            Subscription::run(backend_events),
            Subscription::run(tray),
            window::close_events().map(Message::Closed),
            iced::time::every(TICK).map(|_| Message::Tick),
            iced::system::theme_changes().map(Message::SystemTheme),
            keyboard::listen().filter_map(|event| match event {
                keyboard::Event::KeyPressed { key: keyboard::Key::Character(character), modifiers, .. }
                    if modifiers.control() && character.as_str() == "q" =>
                {
                    Some(Message::Quit)
                }
                _ => None,
            }),
        ];
        // Animate only what is visible and moving: no frames while hidden or at rest.
        if self.main.is_some() && !self.spectrum.is_idle(self.clock.elapsed().as_secs_f64()) {
            subscriptions.push(window::frames().map(|_| Message::Frame));
        }
        Subscription::batch(subscriptions)
    }
}

fn picked(section: &'static str, key: &'static str, file: rfd::FileHandle) -> Message {
    Message::Field(section, key, file.path().to_string_lossy().into_owned())
}

/// A button that is not the main action of the window.
fn action(label: &str) -> iced::widget::Button<'_, Message> {
    button(label).padding(PADDING).style(button::secondary)
}

fn labeled<'a>(label: &'a str, control: impl Into<Element<'a, Message>>) -> Element<'a, Message> {
    row![text(label).width(190), control.into()].spacing(8).align_y(Center).into()
}

fn group<'a>(title: &'a str, rows: Column<'a, Message>) -> Element<'a, Message> {
    column![text(title).size(12).style(text::secondary), container(rows.spacing(6)).padding(10).width(Fill).style(container::bordered_box)]
        .spacing(4)
        .into()
}

/// A volume slider, or `unknown` while the server has not reported the volume.
fn volume<'a>(percent: Option<u32>, unknown: &'a str, on_change: fn(u32) -> Message) -> Element<'a, Message> {
    match percent {
        Some(percent) => row![slider(0..=100, percent, on_change), text(format!("{percent}%")).width(44)].spacing(8).align_y(Center).into(),
        None => text(unknown).into(),
    }
}

fn backend_events() -> impl Stream<Item = Message> {
    iced::futures::stream::iter(EVENTS.lock().unwrap().take()).flatten()
}

/// The spectrum: 32 bars with falling peak caps.
struct Bars<'a>(&'a Spectrum);

impl canvas::Program<Message> for Bars<'_> {
    type State = ();

    fn draw(&self, _state: &(), renderer: &Renderer, theme: &Theme, bounds: Rectangle, _cursor: mouse::Cursor) -> Vec<canvas::Geometry> {
        let mut frame = canvas::Frame::new(renderer, bounds.size());
        let (width, height) = (bounds.width, bounds.height);
        let palette = theme.extended_palette();
        frame.fill_rectangle(Point::ORIGIN, bounds.size(), palette.background.weak.color);
        let gradient = canvas::gradient::Linear::new(Point::new(0.0, height), Point::ORIGIN)
            .add_stop(0.0, GREEN)
            .add_stop(0.65, YELLOW)
            .add_stop(1.0, RED);
        let step = width / BANDS as f32;
        let bar = (step.floor() - (step * 0.15).floor().max(1.0)).max(1.0);
        for band in 0..BANDS {
            let x = (band as f32 * step).floor();
            let level = (self.0.levels[band] * (height - 2.0)).floor();
            if level > 0.0 {
                frame.fill(&canvas::Path::rectangle(Point::new(x, height - 1.0 - level), Size::new(bar, level)), gradient);
            }
            let peak = height - 2.0 - (self.0.peaks[band] * (height - 2.0)).floor();
            frame.fill_rectangle(Point::new(x, peak), Size::new(bar, 2.0), palette.background.base.text);
        }
        vec![frame.into_geometry()]
    }
}

/// Upload rate of the last minute against the target bitrate, with the poor
/// connection and connection lost lines.
struct Graph<'a> {
    history: &'a VecDeque<f64>,
    target: f64,
}

impl canvas::Program<Message> for Graph<'_> {
    type State = ();

    fn draw(&self, _state: &(), renderer: &Renderer, theme: &Theme, bounds: Rectangle, _cursor: mouse::Cursor) -> Vec<canvas::Geometry> {
        let mut frame = canvas::Frame::new(renderer, bounds.size());
        let (width, height) = (bounds.width, bounds.height);
        let palette = theme.extended_palette();
        frame.fill_rectangle(Point::ORIGIN, bounds.size(), palette.background.weak.color);
        // Headroom above the target line.
        let top = self.target * 1.15;
        let y = |kbps: f64| height - 1.0 - (kbps.min(top) / top) as f32 * (height - 2.0);
        if let Some(last) = self.history.len().checked_sub(1) {
            let step = width / (HISTORY - 1) as f32;
            let start = width - step * last as f32;
            let area = canvas::Path::new(|path| {
                path.move_to(Point::new(start, height - 1.0));
                for (index, kbps) in self.history.iter().enumerate() {
                    path.line_to(Point::new(start + index as f32 * step, y(*kbps)));
                }
                path.line_to(Point::new(width, height - 1.0));
                path.close();
            });
            frame.fill(&area, Color { a: 60.0 / 255.0, ..GREEN });
            frame.stroke(&area, canvas::Stroke::default().with_color(GREEN).with_width(1.0));
        }
        for (share, color) in [(1.0, GREEN), (POOR, ORANGE), (DROP, RED)] {
            let line = y(self.target * share).floor() + 0.5;
            let dashed = canvas::Stroke { line_dash: canvas::LineDash { segments: &[4.0, 2.0], offset: 0 }, ..canvas::Stroke::default() };
            frame.stroke(&canvas::Path::line(Point::new(0.0, line), Point::new(width, line)), dashed.with_color(color).with_width(1.0));
        }
        let current = self.history.back().map_or("-".to_string(), |kbps| format!("{kbps:.0}"));
        frame.fill_text(canvas::Text {
            content: format!("{current} / {:.0} kbps", self.target),
            position: Point::new(6.0, 3.0),
            color: palette.background.base.text,
            size: 12.0.into(),
            ..Default::default()
        });
        vec![frame.into_geometry()]
    }
}

pub struct PanelTray {
    tx: mpsc::Sender<Message>,
    live: bool,
    wanted: bool,
    status: String,
}

impl PanelTray {
    fn item(label: &str, message: Message) -> ksni::MenuItem<Self> {
        ksni::menu::StandardItem {
            label: label.into(),
            activate: Box::new(move |tray: &mut Self| drop(tray.tx.try_send(message.clone()))),
            ..Default::default()
        }
        .into()
    }
}

impl ksni::Tray for PanelTray {
    fn id(&self) -> String {
        "radio-panel".into()
    }

    fn title(&self) -> String {
        "Radio Panel".into()
    }

    /// A disc with a white dot: red while live, grey otherwise.
    fn icon_pixmap(&self) -> Vec<ksni::Icon> {
        let disc: [f32; 3] = if self.live { [224.0, 56.0, 75.0] } else { [138.0, 138.0, 138.0] };
        let mut data = Vec::with_capacity(64 * 64 * 4);
        for y in 0..64 {
            for x in 0..64 {
                let distance = ((x as f32 - 31.5).powi(2) + (y as f32 - 31.5).powi(2)).sqrt();
                // Coverage of the pixel by the disc (radius 24) and by the dot (radius 8).
                let (inside, dot) = ((24.5 - distance).clamp(0.0, 1.0), (8.5 - distance).clamp(0.0, 1.0));
                data.push((inside * 255.0) as u8);
                data.extend(disc.map(|channel| (channel + (255.0 - channel) * dot) as u8));
            }
        }
        vec![ksni::Icon { width: 64, height: 64, data }]
    }

    fn tool_tip(&self) -> ksni::ToolTip {
        ksni::ToolTip { title: format!("Radio Panel - {}", self.status), ..Default::default() }
    }

    fn activate(&mut self, _x: i32, _y: i32) {
        let _ = self.tx.try_send(Message::ToggleMain);
    }

    fn menu(&self) -> Vec<ksni::MenuItem<Self>> {
        vec![
            Self::item(if self.wanted { "Stop streaming" } else { "Go Live" }, Message::ToggleLive),
            Self::item("Show / Hide", Message::ToggleMain),
            Self::item("Quit", Message::Quit),
        ]
    }
}

fn tray() -> impl Stream<Item = Message> {
    iced::stream::channel(16, async |mut output| {
        let tray = PanelTray { tx: output.clone(), live: false, wanted: false, status: "Off air".into() };
        // The handle keeps the tray registered: this one for the life of the stream.
        let handle = tray.spawn().await;
        if let Err(error) = &handle {
            eprintln!("radio-panel: no tray icon: {error}");
        }
        let _ = output
            .send(match &handle {
                Ok(handle) => Message::Tray(handle.clone()),
                Err(_) => Message::NoTray,
            })
            .await;
        trace("TRAY_READY");
        std::future::pending::<()>().await
    })
}

pub fn run() -> iced::Result {
    iced::daemon(boot, App::update, App::view)
        .title(App::title)
        .theme(App::theme)
        .subscription(App::subscription)
        .settings(iced::Settings { default_text_size: 14.into(), ..Default::default() })
        .run()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_file(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("radio-panel-rs-ui-{name}-{}.conf", std::process::id()))
    }

    fn app(name: &str) -> App {
        let _ = std::fs::remove_file(config_file(name));
        App::new(Config::load(&config_file(name)).unwrap(), None)
    }

    fn state(text: &str, live: bool, wanted: bool) -> Message {
        Message::Stream(streamer::Event::State { text: text.into(), live, error: false, wanted })
    }

    #[test]
    fn u1_poor_connection_and_silence_warnings() {
        let mut app = app("u1");
        let earlier = |seconds| Instant::now() - Duration::from_secs(seconds);
        let rates = |app: &mut App, kbps: f64| (0..25).for_each(|_| drop(app.update(Message::Stream(streamer::Event::Rate(kbps)))));

        // Not live: rates are not recorded.
        rates(&mut app, 100.0);
        assert!(app.history.is_empty());
        let _ = app.update(state("LIVE", true, true));
        let _ = app.update(Message::Stream(streamer::Event::Connected(true)));

        // Default bitrate 192: below 78% over five seconds warns, but not in the first
        // ten seconds of a connection.
        rates(&mut app, 100.0);
        assert_eq!(app.warning, None);
        app.connected_at = earlier(11);
        rates(&mut app, 100.0);
        assert_eq!(app.warning, Some("LIVE - poor connection!"));
        // 160 is above 78% but not above 92%: the warning stays until the rate recovers.
        rates(&mut app, 160.0);
        assert_eq!(app.warning, Some("LIVE - poor connection!"));
        rates(&mut app, 190.0);
        assert_eq!(app.warning, None);
        assert_eq!(app.history.len(), 100);
        (0..300).for_each(|_| drop(app.update(Message::Stream(streamer::Event::Rate(190.0)))));
        assert_eq!(app.history.len(), HISTORY);

        // Default threshold 15 s without a signal; a signal clears the warning.
        app.last_signal = earlier(14);
        let _ = app.update(Message::Tick);
        assert_eq!(app.warning, None);
        app.last_signal = earlier(16);
        let _ = app.update(Message::Tick);
        assert_eq!(app.warning, Some("LIVE - no audio detected!"));
        let _ = app.update(Message::Pw(pw::Event::Samples(vec![0.0, 0.004])));
        let _ = app.update(Message::Tick);
        assert_eq!(app.warning, Some("LIVE - no audio detected!"));
        let _ = app.update(Message::Pw(pw::Event::Samples(vec![0.0, -0.5])));
        let _ = app.update(Message::Tick);
        assert_eq!(app.warning, None);

        // A new status replaces a warning and, off air, empties the graph.
        app.last_signal = earlier(16);
        let _ = app.update(Message::Tick);
        let _ = app.update(state("Off air", false, false));
        assert_eq!((app.warning, app.status.as_str(), app.history.len()), (None, "Off air", 0));
        let _ = app.update(Message::Tick);
        assert_eq!(app.warning, None);
    }

    #[test]
    fn u2_save_normalizes_and_writes() {
        let mut app = app("u2");
        for (section, key, value) in [
            ("server", "host", "  127.0.0.1 "),
            ("server", "user", "   "),
            ("server", "source_password", "p w "),
            ("server", "port", "0"),
            ("audio", "silence_warning_seconds", "99x99"),
            ("recording", "enabled", "true"),
        ] {
            let _ = app.update(Message::Field(section, key, value.into()));
        }
        // Nothing takes effect before the save. The password is not trimmed by it; the
        // file format drops outer spaces at the next start, as it did for the Python app.
        assert_eq!(app.config.get("server", "host"), "stream.example.com");
        let _ = app.update(Message::Save);
        assert_eq!(app.config.get("server", "source_password"), "p w ");
        let saved = Config::load(&config_file("u2")).unwrap();
        for config in [&app.config, &app.form, &saved] {
            assert_eq!(config.get("server", "host"), "127.0.0.1");
            assert_eq!(config.get("server", "user"), "source");
            assert_eq!(config.get("server", "port"), "1337");
            assert_eq!(config.get("audio", "silence_warning_seconds"), "600");
            assert_eq!(config.get("recording", "enabled"), "true");
        }
        assert_eq!(app.save_hint, "Saved");
        app.save_hint_until = Some(Instant::now());
        let _ = app.update(Message::Tick);
        assert_eq!(app.save_hint, "");

        // The monitor switch is saved at once, without the edited fields.
        let _ = app.update(Message::Field("server", "host", "edited".into()));
        let _ = app.update(Message::Monitor(false));
        let saved = Config::load(&config_file("u2")).unwrap();
        assert_eq!((saved.get("audio", "monitor_enabled"), saved.get("server", "host")), ("false", "127.0.0.1"));
        let _ = std::fs::remove_file(config_file("u2"));
    }

    #[test]
    fn u3_quit_waits_for_teardown_up_to_its_limit() {
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = ran.clone();
        assert!(finished_within(Duration::from_secs(5), move || flag.store(true, std::sync::atomic::Ordering::SeqCst)));
        assert!(ran.load(std::sync::atomic::Ordering::SeqCst));

        // A teardown that hangs is given up on at the limit, not waited out.
        let (release, hold) = std::sync::mpsc::channel::<()>();
        let started = Instant::now();
        assert!(!finished_within(Duration::from_millis(200), move || {
            let _ = hold.recv();
        }));
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(200) && waited < Duration::from_secs(3), "{waited:?}");
        drop(release);

        // Once quit is under way, nothing else is acted on.
        let mut app = app("u3");
        let _ = app.update(Message::Quit);
        assert!(app.quitting);
        let _ = app.update(Message::ToggleMain);
        let _ = app.update(state("LIVE", true, true));
        assert_eq!((app.main, app.live), (None, false));
    }

    #[test]
    fn u5_volumes_follow_the_server_except_right_after_a_drag() {
        let mut app = app("u5");
        // The first reports arrive within milliseconds of the start.
        let _ = app.update(Message::Pw(pw::Event::StreamVolume(60)));
        let _ = app.update(Message::Pw(pw::Event::LocalVolume(40)));
        assert_eq!((app.stream_volume, app.local_volume), (Some(60), Some(40)));
        // The server's echo of a drag step must not pull the slider back.
        let _ = app.update(Message::StreamVolume(30));
        let _ = app.update(Message::Pw(pw::Event::StreamVolume(31)));
        assert_eq!(app.stream_volume, Some(30));
        app.volume_touched = Some(Instant::now() - Duration::from_millis(400));
        let _ = app.update(Message::Pw(pw::Event::StreamVolume(31)));
        assert_eq!(app.stream_volume, Some(31));
    }

    #[test]
    fn u4_no_preference_keeps_the_last_color_scheme() {
        use iced::theme::Mode;
        let mut app = app("u4");
        assert!(!app.dark);
        for (mode, dark) in [(Mode::Dark, true), (Mode::None, true), (Mode::Light, false), (Mode::None, false)] {
            let _ = app.update(Message::SystemTheme(mode));
            assert_eq!(app.dark, dark, "{mode:?}");
        }
    }
}
