# Changelog

All notable changes to the Rokid Bridge plugin are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.0] - 2026-10-09

### Fixed

- **Chinese reasoning header leaked chain-of-thought to the glasses**:
  `_strip_reasoning` only matched the English gateway header
  (`💭 **Reasoning:**` and its blockquote/subtext variants). The gateway
  localizes these display strings (`locales/zh.yaml` emits `💭 **推理：**`
  etc.), so on a Chinese-locale host the reasoning block passed through
  unstripped and got voiced on-device. Matching is now anchored on the `💭`
  marker and covers both locales across all three render styles.
- **Queued second answer invisible on the glasses**: the device ignores every
  frame after a `done` frame, and all utterances within one wake session
  share the same `requestId` (observed on-device). When a follow-up had
  queued behind a running turn, closing the first answer with `done`
  discarded the queued turn's answer. `send()` now defers the `done` frame
  while a queue entry is pending: it appends a `\n\n` separator on the still
  open request and runs the queued turn there, so both answers render in one
  bubble and only the final answer closes it.

### Added

- **Follow-up queue instead of interrupt**: while a turn is running for this
  chat, new inbound messages are no longer handed to the gateway (which
  interrupted the run and swallowed the in-flight answer). They queue
  locally and drain after the current answer completes. Failsafe: if the
  running turn never answers (crash/abort), the queued message is
  force-dispatched after 120s.
- **Answer heartbeat ("…")**: while an answer is still being computed, a
  non-terminal `…` chunk is pushed every 8s (bounded at 10 minutes) so the
  glasses' ~30s request timer does not surface a timeout banner. The
  heartbeat never sends a `done` frame (an early close makes the final
  answer invisible — observed on-device) and cancels as soon as the answer
  arrives, so fast replies stream cleanly with no ack noise.
- **Bare wake word silently closed**: an utterance that is only the wake word
  ("龙虾助手", tolerating ASR fillers 呃/嗯/那个/就是… and punctuation) is a
  device wake event, not a question — it is closed with an empty `done`
  frame without invoking the model, so the glasses never voice a reply to
  being woken up. Deterministic string rule, zero LLM cost.
- **Gateway status prose suppressed**: outbound text starting with `↪`
  (run-redirect notices) or `⏳` (long-running heartbeat notices) is internal
  status prose, not an answer; it is swallowed before reaching the device so
  the glasses never voice it.

- **Mid-turn commentary cut the reply in half**: the gateway sends interim
  assistant messages (e.g. "我先执行脚本") mid-turn, marked `_interim_send`.
  The adapter treated every `send()` as turn-final and closed the request
  with a `done` frame, so the glasses stopped accepting frames and the real
  answer after the interim chunk was never displayed. Interim sends now
  deliver their text as a non-terminal chunk (with a trailing blank line)
  and never emit `done`; only the turn-final answer closes the request.

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
