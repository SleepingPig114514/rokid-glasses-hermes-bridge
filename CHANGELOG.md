# Changelog

All notable changes to the Rokid Glasses Hermes Bridge plugin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-09-20

### Added

- Initial release of the Rokid Glasses Hermes Bridge plugin
- Full protocol compatibility with official Rokid OpenClaw plugin (commit 522cf3c)
- Support for text, voice-to-text, and image messages from Rokid glasses
- Four device-command tools:
  - `take_photo` (📷): Trigger camera capture
  - `take_navigation` (🧭): Open/close navigation with POI and transport mode
  - `control_calendar` (📅): Create calendar events
  - `notify_agent_off` (👋): End conversation session
- Automatic media download to local temp directory for vision tool compatibility
- Exponential backoff reconnection logic (max 10 retries, 1-30s delay with jitter)
- Session tracking via `sessionKey` and `requestId` for multi-turn conversations
- Passive credential check (`check_requirements()`) for gateway status display
- Environment enablement seeding for automatic platform detection
- User authorization controls (`ROKID_ALLOWED_USERS`, `ROKID_ALLOW_ALL_USERS`)

### Changed

- Plugin renamed from `rokid-bridge-platform` to `rokid-glasses-hermes-bridge` for clarity
- Description updated to clarify bridge role (requires official OpenClaw plugin)
- Dependencies explicitly declared in `plugin.yaml`

### Technical Notes

- Adapter maintains process-wide `_active_adapter` handle for tool_call frame injection
- Outbound messages follow official frame structure: `answer` chunk → `done` finish frame
- Image media downloaded to `%TEMP%\hermes-rokid-media\` with timestamped filenames
- WebSocket heartbeat: 30s, max message size: 16MB

### Known Limitations

- Single device account per Hermes profile (multi-device support requires dict keyed by linkCode)
- Desktop App Messaging UI may not show plugin platforms (CLI `hermes gateway setup` works)
- Gateway does not auto-start with desktop app (issue #47800) — use `hermes gateway install`

---

## Future Considerations

- [ ] Token-by-token streaming support (currently sends full answer as one frame)
- [ ] Multi-device account support (dict keyed by linkCode)
- [ ] SSH key upload for git protocol (if repo integration added)
- [ ] Fine-grained personal access token scope handling
