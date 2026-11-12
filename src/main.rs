mod config;
mod ffmpeg;
mod meta;
mod pw;
mod recorder;
mod spectrum;
mod streamer;
mod ui;

fn main() -> iced::Result {
    ui::run()
}
