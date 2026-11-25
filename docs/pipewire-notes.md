# PipeWire and WirePlumber behavior the panel relies on

Checked with the `pipewire` and `libspa` crates 0.10.1 against PipeWire 1.6 and
WirePlumber 0.5.19. The PipeWire code lives in `src/pw.rs`.

## Sink and loopback

- The sink is `core.create_object::<Node>("adapter", props)` with
  `factory.name=support.null-audio-sink`, `media.class=Audio/Sink`,
  `audio.position=FL,FR` and `monitor.channel-volumes=true`. The node belongs to the
  panel's connection and disappears when the process exits, crash included.
- The local monitor is `libpipewire-module-loopback`, loaded in-process with
  `pw_context_load_module(context, "libpipewire-module-loopback", args, null)` and
  removed with `pw_impl_module_destroy`. Its playback node shows up in the registry
  under the `node.name` from `playback.props`.

## Volume

- Volume is the `Props` param with `SPA_PROP_channelVolumes` (float array, linear),
  set with `node.set_param(ParamType::Props, 0, pod)` and read back through
  `node.subscribe_params(&[ParamType::Props])` and the listener's `param` callback.
  This works on the proxy from `create_object` (stream volume) and on a proxy from
  `registry.bind(global)` (local volume on the loopback playback node).
- The sink monitor follows the sink volume, so the stream volume scales what ffmpeg
  captures.
- The sliders show pactl percent, which is cubic: `linear = (percent / 100)^3`.
  Linear 0.5 reads as 79%, 0.25 as 63%.
- WirePlumber saves and restores both volumes across runs, in
  `~/.local/state/wireplumber/stream-properties` under the keys
  `Audio/Sink:node.name:<sink>` and `Output/Audio:media.name:<sink>-local output`
  (the loopback's `node.description` plus " output"). The config file has no volume
  keys.
- The restore lands a moment after the node appears and overrides a volume set
  before it. The panel therefore never sets a volume on its own: it shows what the
  server reports and sets one only when the user moves a slider. Tests wait 500 ms
  before setting. (`state.restore-props=false` in `playback.props` would turn the
  restore off, and the persistence with it.)

## Spectrum capture

- A `StreamBox` with `target.object=<sink name>` and `stream.capture.sink=true`,
  format F32LE, flags `AUTOCONNECT | MAP_BUFFERS`.
- Without `RT_PROCESS` the `process` callback runs on the loop thread.
  `pw::keys::TARGET_OBJECT` is feature-gated; use the literal `"target.object"`.
- The panel asks for 44100 Hz, one channel at `SPA_AUDIO_CHANNEL_MONO`. The stream
  delivers buffers of zeros while nothing plays. `StreamBox<'c>` borrows the core.

## Devices and routing

- Sinks are `Node` globals with `media.class=Audio/Sink` (`node.name`,
  `node.description`, `object.serial`). `Link` globals have `link.output.node` and
  `link.input.node`. The device the monitor plays to is the input node of the link
  whose output node is the loopback playback node.
- Default sink: bind the `Metadata` global with `metadata.name=default`. Its
  `property` callback gets subject 0, key `default.audio.sink` and value
  `{"name":"<node name>"}`.
- Moving the monitor to another device: `metadata.set_property(<loopback node id>,
  "target.object", Some("Spa:Id"), Some(<target object.serial>))` on that metadata,
  which is what `pactl move-sink-input` writes. WirePlumber moves the stream and saves
  the choice (`"target":"<sink name>"` under the stream's key in `stream-properties`),
  so the device persists without a config key.
- The panel makes no links of its own. WirePlumber links players to the sink, moves
  them off it when it goes away, and pauses MPRIS players that played into it
  (`linking.pause-playback`, `linking/mpris-pause.lua`, on by default). Links made by
  an application compete with WirePlumber for the same ports: WirePlumber logs
  `link failed`, sends the player an error (`linking/link-target.lua`) and stops
  retrying it.
- Players that were running before the panel started: WirePlumber applies a saved
  target only when a stream is created, so they stay on the default device. At start
  the panel reads `stream-properties` and, for each playback stream whose key
  (`formKey` in WirePlumber's `node/state-stream.lua`) is saved to the panel's sink
  and that names no target itself, sets `target.object` once. It first waits until a
  link leaves the sink: WirePlumber saves a `target.object` write by looking the
  serial up among the nodes it knows, and saves a sink it has not seen yet as no
  target, which deletes the user's saved route.

## Connection lifetime

- Never destroy (drop) a proxy after the server removed its object: the server
  answers with a core error (`unknown resource N op:7`, id 0). Only `-EPIPE` on the
  core means the connection is lost; treating every core error as fatal kills the
  sink.
- Loop timers: `mainloop.loop_().add_timer(..)`, then
  `timer.update_timer(Some(d), Some(d)).into_sync_result()`. Listeners must be kept
  alive in a variable. `std::process::exit` skips destructors.

## ffmpeg capture

- `ffmpeg -f pulse -i <sink>.monitor` does not fail when the sink is missing or goes
  away: pipewire-pulse captures the default sink's monitor instead, which would put
  the desktop's audio on air. Both ffmpeg runs (stream and recording) get
  `PULSE_PROP=node.dont-fallback=true` (`ffmpeg::no_fallback`): opening then fails
  with "Input/output error", and a running capture ends with "Error during
  demuxing" (exit code 0), which the streamer retries.

## Test players

- A `pw-cat` player with `node.dont-fallback=true` exits with `defined target not
  found` when its sink goes away. Tests use `--target 0` (no autoconnect; WirePlumber
  ignores the stream) and link it with `pw-link`.
- A test tone into a dev sink must end before the sink does, and the player needs
  `node.dont-fallback=true node.dont-reconnect=true`; otherwise WirePlumber moves it
  to the real output device.
