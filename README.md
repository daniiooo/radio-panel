# radio-panel

A small tray panel for live radio streaming on Linux with PipeWire. While it runs,
it provides an output named "Radio Stream". Whatever plays into that output is sent
to an Icecast-compatible server through ffmpeg.

- Go live with MP3, Opus or Vorbis. A lost connection is retried with backoff.
- Track titles come from a text file or from a media player over MPRIS, and are sent
  to the server as stream metadata. Titles can also be typed by hand.
- Local monitor of the stream on any output device, with separate stream and monitor
  volumes.
- Spectrum display, silence warning and an upload-rate graph.
- Optional local recording, with a cue file and a tracklist.
- Starts hidden in the tray with `--hidden`, and can connect on start.

## Requirements

- PipeWire with WirePlumber, and `ffmpeg` with the PulseAudio input and LAME
  (MP3), Opus and Vorbis encoders.
- A desktop with a StatusNotifierItem tray (KDE Plasma, or a tray extension).
- To build: a Rust toolchain (edition 2024), the PipeWire development files,
  `clang` and `pkg-config`.

## Build and run

```
cargo build --release
target/release/radio-panel
```

Route a player to "Radio Stream" once, for example in pavucontrol. WirePlumber
remembers that route and restores it each time the panel starts.

Settings are stored in `~/.config/radio-panel.conf`. The file holds the source
password and is written with mode 0600.

Environment variables:

- `RADIO_PANEL_CONFIG`: use another config file.
- `RADIO_PANEL_SINK`: use another sink name instead of `radio`, for test runs.
- `RADIO_PANEL_TRACE=1`: print window and tray events.

## Tests

`cargo test` also runs live tests. They need `icecast`, `ffmpeg`, `go`, `python3`
and the PipeWire tools (`pactl`, `pw-cli`, `pw-link`, `pw-cat`, `pw-metadata`).
They use only `127.0.0.1` and their own null sinks, and play no audio to a real
device. One test builds a harness around the
[ingest](https://github.com/example/ingest) proxy (`tests/ingest`); Go
downloads its modules on the first run.

Notes on the PipeWire behavior the panel relies on are in
`docs/pipewire-notes.md`. The ingest rules are in
`docs/ingest-compat.md`.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT), at your option.
