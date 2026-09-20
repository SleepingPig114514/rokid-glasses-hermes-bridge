# Changelog

All notable changes to the Rokid Bridge plugin are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-09-21

### Added

- **Automatic reasoning stripping on send**: the adapter removes the gateway's
  pre-send chain-of-thought block (code / blockquote / subtext render styles)
  before a reply reaches the glasses, so the glasses never display the model's
  reasoning.
  - Applies only to this platform — the strip lives in `adapter.send()`, so
    desktop and every other platform still show reasoning as configured.
  - No manual config needed; unrecognized formats are passed through untouched
    (best-effort, never drops the real answer).

## [1.1.0] - 2026-09-21

### Changed

- **Device-command frames are now non-terminal** (`event:"message"`,
  `is_finish:false`) per the protocol contract, instead of a terminal
  `done`/`is_finish:true` frame. The request stays open while the device
  executes the command and follows up on the same `requestId`, which is what
  lets the glasses display the final reply.
- **Photo turns block inside the tool**: the `take_photo` tool waits for the
  device's follow-up image and returns the photo (plus vision description) as
  the tool result. This removes the premature first answer and the extra model
  round, and shortens end-to-end latency.
- **Automatic vision enrichment on photo**: the captured frame is described via
  the configured `auxiliary.vision` backend (not hard-coded), so the model no
  longer self-initiates a slow vision call.
- Strengthened platform hint and `take_photo` tool description: only emit the
  tool call (no preface/trailing text), answer with the final result in spoken
  language, no Markdown or process commentary.

### Added

- Abandonment safety net: a background task closes a paused turn with a `done`
  frame if the device never follows up (default 120s); photo wait itself times
  out at 90s.
- Photo-wait plumbing in the adapter (thread-safe, since tools run in a worker
  thread).

### Fixed

- Final reply being dropped by the glasses because the command frame had
  prematurely terminated the device request.

## [1.0.0] - 2026-09-20

### Added

- Initial release: standalone Hermes platform adapter connecting directly to
  the Rokid RCS outbound WebSocket (`wss://rcs.rokid.com/claw/ws/link`).
- Text, voice-to-text, and image input; images auto-downloaded to a local temp
  directory.
- Four device-command tools: `take_photo`, `take_navigation`,
  `control_calendar`, `notify_agent_off`.
- Exponential-backoff reconnection (max 10 retries, ~1–30s with jitter).
- Per-chat `requestId`/`sessionKey` tracking for multi-turn conversations.
- Passive credential check and environment-based platform enablement.
- User authorization controls (`ROKID_ALLOWED_USERS`, `ROKID_ALLOW_ALL_USERS`).

### Known Limitations

- Single device account per Hermes profile.
- Desktop app does not auto-start the messaging gateway (use
  `hermes gateway install`).
- Answers are sent as one buffered frame plus a `done` frame; per-token
  streaming is not implemented.

---

## Future Considerations

- [ ] Token-by-token answer streaming (the non-terminal command frame already
      matches the streaming model).
- [ ] Multi-device support (adapter state keyed by `linkCode`).
- [ ] Optional automatic fallback if the configured vision backend is
      unavailable.
