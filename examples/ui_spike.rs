//! UI spike: iced daemon + ksni tray. Go/no-go gate for the toolkit on KDE/Wayland.
//!
//! Checks: tray icon appears; left click toggles the main window; the title-bar
//! close button hides to tray (process stays); tray menu opens a second window and
//! quits; the canvas animates; Ctrl+Q quits. `--hidden` starts with no window.
//! Every window open/close is printed, so the flow can be driven over D-Bus.

use std::time::Instant;

use iced::futures::Stream;
use iced::futures::channel::mpsc;
use iced::widget::{button, canvas, column, text};
use iced::{Color, Element, Fill, Point, Rectangle, Renderer, Size, Subscription, Task, Theme};
use iced::{keyboard, mouse, window};
use ksni::TrayMethods;

#[derive(Debug, Clone)]
enum Message {
    ToggleMain,
    OpenGraph,
    Closed(window::Id),
    Frame(Instant),
    Quit,
}

struct State {
    main: Option<window::Id>,
    graph: Option<window::Id>,
    start: Instant,
    now: Instant,
}

fn open(size: Size) -> (window::Id, Task<Message>) {
    let settings = window::Settings {
        size,
        platform_specific: window::settings::PlatformSpecific {
            application_id: "radio-panel".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    let (id, task) = window::open(settings);
    (id, task.discard())
}

fn boot() -> (State, Task<Message>) {
    let now = Instant::now();
    let state = State { main: None, graph: None, start: now, now };
    let hidden = std::env::args().any(|a| a == "--hidden");
    (state, if hidden { Task::none() } else { Task::done(Message::ToggleMain) })
}

fn update(state: &mut State, message: Message) -> Task<Message> {
    match message {
        Message::ToggleMain => match state.main.take() {
            Some(id) => {
                println!("MAIN_HIDE");
                window::close(id)
            }
            None => {
                let (id, task) = open(Size::new(680.0, 360.0));
                state.main = Some(id);
                println!("MAIN_SHOW");
                task
            }
        },
        Message::OpenGraph if state.graph.is_none() => {
            let (id, task) = open(Size::new(680.0, 300.0));
            state.graph = Some(id);
            println!("GRAPH_SHOW");
            task
        }
        Message::OpenGraph => Task::none(),
        Message::Closed(id) => {
            // Also reached by the title-bar close button: the daemon keeps running.
            if state.main == Some(id) {
                state.main = None;
                println!("MAIN_CLOSED_BY_USER");
            } else if state.graph == Some(id) {
                state.graph = None;
                println!("GRAPH_CLOSED");
            }
            Task::none()
        }
        Message::Frame(now) => {
            state.now = now;
            Task::none()
        }
        Message::Quit => {
            println!("QUIT");
            iced::exit()
        }
    }
}

fn view(state: &State, id: window::Id) -> Element<'_, Message> {
    if state.graph == Some(id) {
        return text("Second window (network graph placeholder)").into();
    }
    let seconds = state.now.duration_since(state.start).as_secs_f32();
    column![
        text("radio-panel UI spike"),
        button("Open second window").on_press(Message::OpenGraph),
        button("Hide to tray").on_press(Message::ToggleMain),
        canvas(Bars { seconds }).width(Fill).height(Fill),
    ]
    .spacing(8)
    .padding(12)
    .into()
}

fn subscription(state: &State) -> Subscription<Message> {
    let mut subs = vec![
        Subscription::run(tray),
        window::close_events().map(Message::Closed),
        keyboard::listen().filter_map(|event| match event {
            keyboard::Event::KeyPressed { key: keyboard::Key::Character(c), modifiers, .. }
                if modifiers.control() && c.as_str() == "q" =>
            {
                Some(Message::Quit)
            }
            _ => None,
        }),
    ];
    // Animate only while the main window exists: no wakeups when hidden to tray.
    if state.main.is_some() {
        subs.push(window::frames().map(Message::Frame));
    }
    Subscription::batch(subs)
}

/// Fake 32-band spectrum.
struct Bars {
    seconds: f32,
}

impl canvas::Program<Message> for Bars {
    type State = ();

    fn draw(
        &self,
        _state: &(),
        renderer: &Renderer,
        _theme: &Theme,
        bounds: Rectangle,
        _cursor: mouse::Cursor,
    ) -> Vec<canvas::Geometry> {
        let mut frame = canvas::Frame::new(renderer, bounds.size());
        let width = bounds.width / 32.0;
        for i in 0..32 {
            let level = (self.seconds * 3.0 + i as f32 * 0.4).sin() * 0.5 + 0.5;
            let height = level * bounds.height;
            frame.fill_rectangle(
                Point::new(i as f32 * width, bounds.height - height),
                Size::new(width - 2.0, height),
                Color::from_rgb(0.2, 0.7, 0.4),
            );
        }
        vec![frame.into_geometry()]
    }
}

struct PanelTray(mpsc::Sender<Message>);

impl PanelTray {
    fn item(label: &str, message: Message) -> ksni::MenuItem<Self> {
        ksni::menu::StandardItem {
            label: label.into(),
            activate: Box::new(move |tray: &mut Self| {
                let _ = tray.0.try_send(message.clone());
            }),
            ..Default::default()
        }
        .into()
    }
}

impl ksni::Tray for PanelTray {
    fn id(&self) -> String {
        "radio-panel-spike".into()
    }
    fn icon_name(&self) -> String {
        "audio-card".into()
    }
    fn title(&self) -> String {
        "Radio Panel (spike)".into()
    }
    fn activate(&mut self, _x: i32, _y: i32) {
        let _ = self.0.try_send(Message::ToggleMain);
    }
    fn menu(&self) -> Vec<ksni::MenuItem<Self>> {
        vec![
            Self::item("Show / Hide", Message::ToggleMain),
            Self::item("Second window", Message::OpenGraph),
            ksni::MenuItem::Separator,
            Self::item("Quit", Message::Quit),
        ]
    }
}

fn tray() -> impl Stream<Item = Message> {
    iced::stream::channel(16, async |output| {
        // The handle keeps the tray registered; hold it for the life of the stream.
        let _handle = PanelTray(output).spawn().await.expect("tray");
        println!("TRAY_READY");
        std::future::pending::<()>().await
    })
}

fn main() -> iced::Result {
    iced::daemon(boot, update, view)
        .title(|_: &State, _| "Radio Panel (spike)".to_string())
        .subscription(subscription)
        .run()
}
