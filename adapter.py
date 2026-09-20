"""Rokid AR glasses platform adapter for the Hermes gateway.

Transport: one OUTBOUND WebSocket per device account to the Rokid RCS bridge
(``wss://rcs.rokid.com/claw/ws/link?linkCode=...&linkSecret=...``). No public
endpoint is needed. Protocol is ported 1:1 from the official OpenClaw plugin
``rokid-openclaw-gateway-compatible`` (commit 522cf3c):

Inbound (device -> cloud -> here):
    {"messages": [{"role": "user", "type": "text"|"image",
                   "text"?: str, "image_url"?: str}, ...],
     "requestId": str, "sessionKey"?: str}
    {"type": "cancel", "requestId": str}

Outbound (here -> cloud -> device):
    answer chunk: {"event": "message", "data": {role:"agent", message_id,
                   agent_id, answer_stream, is_finish:false, type:"answer"}}
    finish:       {"event": "done",    "data": {...same..., answer_stream:"",
                   is_finish:true, type:"answer"}}
    tool call:    {"event": "message", "data": {role:"agent", message_id,
                   agent_id, is_finish:false, type:"tool_call",
                   tool_call:{command, ...}}}
    status:       {"type": "status", "connected": true}   # sent on open

A device-command (tool_call) is a NON-terminal mid-turn frame: the request
stays open, the device follows up on the same requestId (e.g. the photo), and
the model's final answer closes the turn with the done frame. A fallback task
sends the done frame if the device never follows up.

This MVP sends the whole agent answer as one ``answer`` frame followed by a
``done`` frame (the official plugin's buffered fallback path); token-by-token
streaming can be layered on later via the streaming consumer.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

PLATFORM_NAME = "rokid"
DEFAULT_WS_URL = "wss://rcs.rokid.com/claw/ws/link"
DEFAULT_AGENT_ID = "main"
RECONNECT_MAX_RETRIES = 10
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0
# After a terminal device-command frame, swallow the model's trailing answer text
# for this many seconds. Cleared early by the next inbound frame (the device's
# follow-up, e.g. the photo); TTL only covers the user abandoning the action.
SUPPRESS_TTL_SECONDS = 120.0

# Process-wide handle to the live adapter so the device-command tools can push
# tool_call frames down the active socket. Single device account per profile for
# now; a dict keyed by linkCode can replace this if multi-device is needed.
_active_adapter: Optional["RokidBridgeAdapter"] = None


def _backoff_delay(attempt: int) -> float:
    delay = RECONNECT_BASE_DELAY * (2 ** attempt)
    jitter = random.uniform(0, RECONNECT_BASE_DELAY)
    return min(delay + jitter, RECONNECT_MAX_DELAY)


def _strip_reasoning(text: str) -> str:
    """Remove the gateway's pre-send reasoning block so the glasses never see
    chain-of-thought. Covers the three render styles produced by gateway
    run_turn._hmwa_prepend_reasoning:

      code:      "💭 **Reasoning:**\\n```\\n...\\n```\\n\\n<answer>"
      blockquote: "> 💭 **Reasoning:**\\n> ...\\n\\n<answer>"
      subtext:   "-# 💭 Reasoning\\n-# ...\\n\\n<answer>"

    Best-effort format matching: an unrecognized shape is returned untouched
    (worst case the reasoning shows; never crash or drop the real answer).
    """
    if not text or "Reasoning" not in text:
        return text

    lines = text.split("\n")
    # Identify the first line that opens a reasoning block.
    header_idx = next(
        (
            i for i, ln in enumerate(lines)
            if ln.lstrip().startswith("💭 **Reasoning:**")
            or ln.lstrip().startswith("> 💭 **Reasoning:**")
            or ln.lstrip().startswith("-# 💭 Reasoning")
        ),
        None,
    )
    if header_idx is None:
        return text

    first = lines[header_idx].lstrip()
    body_lines: List[str] = []

    if first.startswith("💭 **Reasoning:**"):
        # Code style: header then a fenced block on the following lines.
        i = header_idx + 1
        if i < len(lines) and lines[i].strip().startswith("```"):
            i += 1  # opening fence
            while i < len(lines) and not lines[i].strip().startswith("```"):
                i += 1
            i += 1  # closing fence (if present)
        else:
            return text  # malformed; leave untouched
        body_lines = lines[i:]
    elif first.startswith("> 💭 **Reasoning:**"):
        i = header_idx + 1
        while i < len(lines) and (lines[i].lstrip().startswith(">") or not lines[i].strip()):
            i += 1
        body_lines = lines[i:]
    else:  # -# subtext
        i = header_idx + 1
        while i < len(lines) and (lines[i].lstrip().startswith("-#") or not lines[i].strip()):
            i += 1
        body_lines = lines[i:]

    result = "\n".join(body_lines).lstrip("\n")
    return result or text


class RokidBridgeAdapter(BasePlatformAdapter):
    """Outbound WebSocket bridge to a paired Rokid glasses device."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform(PLATFORM_NAME))
        extra = config.extra or {}
        self.link_code = str(
            os.getenv("ROKID_LINK_CODE") or extra.get("link_code") or extra.get("linkCode") or ""
        ).strip()
        self.link_secret = str(
            os.getenv("ROKID_LINK_SECRET") or extra.get("link_secret") or extra.get("linkSecret") or ""
        ).strip()
        self.ws_url = str(
            os.getenv("ROKID_WS_URL") or extra.get("ws_url") or extra.get("wsUrl") or DEFAULT_WS_URL
        ).strip()
        self.agent_id = str(
            os.getenv("ROKID_AGENT_ID") or extra.get("agent_id") or extra.get("agentId") or DEFAULT_AGENT_ID
        ).strip()

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._listen_task: Optional[asyncio.Task] = None
        self._running = False
        self._stopping = False
        # Stable chat (sessionKey/linkCode) -> latest inbound requestId awaiting a reply.
        self._pending_request: Dict[str, str] = {}
        # Chats whose turn is paused on a device-command frame awaiting the
        # device's follow-up (e.g. the photo); value is the monotonic deadline
        # until which outbound answer text is swallowed.
        self._suppressed_until: Dict[str, float] = {}
        # Tasks that close the paused turn with a done frame if the device
        # never follows up (user abandoned the action).
        self._suppress_tasks: Dict[str, "asyncio.Task[None]"] = {}
        # Device-command tools block here awaiting the device's follow-up image
        # frame. Tools run in a worker thread (model_tools._run_async), so the
        # signal is a threading.Event carrying the downloaded-photo result;
        # the image frame resolves it instead of dispatching a new turn, making
        # the photo the tool result on the same turn.
        self._photo_waits: Dict[str, Dict[str, Any]] = {}
        self._send_lock = asyncio.Lock()

    # ------------------------------------------------------------------ config
    def _build_ws_url(self) -> str:
        sep = "&" if "?" in self.ws_url else "?"
        return self.ws_url + sep + urlencode(
            {"linkCode": self.link_code, "linkSecret": self.link_secret}
        )

    @property
    def _chat_id(self) -> str:
        # One paired device per account => the link code is the stable DM id.
        return self.link_code or "rokid"

    # ---------------------------------------------------------------- lifecycle
    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.link_code or not self.link_secret:
            logger.error("[rokid] link_code/link_secret missing; cannot connect")
            return False

        global _active_adapter
        _active_adapter = self
        self._running = True
        self._stopping = False

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        self._listen_task = asyncio.create_task(self._connection_loop())
        # Optimistic: the loop performs the actual connect + reconnect.
        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        global _active_adapter
        self._running = False
        self._stopping = True
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except (asyncio.CancelledError, Exception):
                pass
            self._listen_task = None
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = None
        for wait in self._photo_waits.values():
            wait["event"].set()
        self._photo_waits.clear()
        for task in self._suppress_tasks.values():
            if not task.done():
                task.cancel()
        self._suppress_tasks.clear()
        self._suppressed_until.clear()
        if _active_adapter is self:
            _active_adapter = None
        self._mark_disconnected()

    async def _connection_loop(self) -> None:
        attempt = 0
        while self._running and not self._stopping:
            try:
                url = self._build_ws_url()
                logger.info("[rokid] connecting to %s (attempt %d)", self.ws_url, attempt + 1)
                async with self._session.ws_connect(url, heartbeat=30, max_msg_size=16 * 1024 * 1024) as ws:
                    self._ws = ws
                    attempt = 0
                    self._mark_connected()
                    logger.info("[rokid] connected to %s", self.ws_url)
                    await self._send_json({"type": "status", "connected": True})
                    await self._read_loop(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[rokid] connection error: %s", exc)
            finally:
                self._ws = None
                self._mark_disconnected()

            if not self._running or self._stopping:
                break
            if attempt >= RECONNECT_MAX_RETRIES:
                logger.error("[rokid] max reconnect retries (%d) exhausted", RECONNECT_MAX_RETRIES)
                break
            delay = _backoff_delay(attempt)
            attempt += 1
            logger.info("[rokid] reconnecting in %.0fms", delay * 1000)
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise

    async def _read_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    await self._handle_raw(msg.data)
                except Exception:
                    logger.exception("[rokid] error handling inbound frame")
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.warning("[rokid] ws error: %s", ws.exception())
                break
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                logger.info("[rokid] socket closed by peer")
                break

    # ------------------------------------------------------------- inbound parse
    async def _handle_raw(self, raw: str) -> None:
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.warning("[rokid] invalid JSON: %s", (raw or "")[:200])
            return
        if not isinstance(parsed, dict):
            return

        if parsed.get("type") == "cancel":
            # Hermes owns turn cancellation internally; MVP logs device cancels.
            logger.info("[rokid] device cancel requested for %s", parsed.get("requestId"))
            return

        # Accept both RCS-relayed (messages/requestId) and passthrough names.
        messages = parsed.get("messages", parsed.get("message"))
        request_id = parsed.get("requestId", parsed.get("message_id"))
        session_key = parsed.get("sessionKey")

        if isinstance(messages, list) and isinstance(request_id, str):
            await self._handle_chat(messages, request_id, session_key)
        else:
            logger.warning("[rokid] unrecognized frame: %s", raw[:200])

    async def _handle_chat(
        self, messages: List[Dict[str, Any]], request_id: str, session_key: Optional[str]
    ) -> None:
        texts: List[str] = []
        image_urls: List[str] = []
        for m in messages:
            if not isinstance(m, dict):
                continue
            mtype = m.get("type")
            if mtype == "text" and m.get("text"):
                texts.append(str(m["text"]))
            elif mtype == "image" and m.get("image_url"):
                image_urls.append(str(m["image_url"]))

        text = "\n".join(t for t in texts if t.strip())
        if not text and not image_urls:
            logger.info("[rokid] empty request %s; ignoring", request_id)
            return

        chat_id = str(session_key).strip() if session_key else self._chat_id

        # If a device-command tool on this chat is blocked waiting for the
        # photo, an image frame resolves that wait instead of starting a new
        # turn, so the photo becomes the tool result on the same turn.
        photo_wait = self._photo_waits.get(chat_id)
        if photo_wait is not None and image_urls:
            media_paths: List[str] = []
            for url in image_urls:
                path = await self._download_media(url, request_id)
                if path:
                    media_paths.append(path)
            if media_paths:
                logger.info("resolving photo wait requestId=%s chat=%s", request_id, chat_id)
                # Turn resumes inside the tool — drop suppression/close tasks.
                self._release_suppression(chat_id)
                # Auto-describe via the configured vision backend (qwen omni,
                # ~2s) so the model sees the content as the tool result without
                # making its own slow vision call. Best-effort: keep the path.
                description = await self._describe_photo(media_paths)
                photo_wait["paths"] = media_paths
                photo_wait["description"] = description
                photo_wait["event"].set()
                return
            logger.warning("photo frame download failed; falling through to normal dispatch")

        # A new inbound frame resumes any earlier paused turn — release
        # suppression and cancel the abandonment close task.
        self._release_suppression(chat_id)
        self._pending_request[chat_id] = request_id

        media_paths = []
        media_types: List[str] = []
        for url in image_urls:
            path = await self._download_media(url, request_id)
            if path:
                media_paths.append(path)
                media_types.append("image/jpeg")
        if image_urls and not media_paths:
            # Fall back to inline URLs if downloads failed so the agent still sees them.
            text = (text + "\n" if text else "") + "\n".join(image_urls)

        source = self.build_source(
            chat_id=chat_id,
            chat_name=f"Rokid {self.link_code}",
            chat_type="dm",
            user_id=chat_id,
            user_name=f"Rokid {self.link_code}",
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.PHOTO if media_paths and not text else MessageType.TEXT,
            source=source,
            message_id=request_id,
            media_urls=media_paths,
            media_types=media_types,
            metadata={"request_id": request_id, "session_key": session_key},
        )
        logger.info("[rokid] dispatch requestId=%s chat=%s images=%d", request_id, chat_id, len(media_paths))
        await self.handle_message(event)

    async def _download_media(self, url: str, request_id: str) -> Optional[str]:
        if not (url.startswith("http://") or url.startswith("https://")):
            return None
        try:
            assert self._session is not None
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    logger.warning("[rokid] image fetch %s -> HTTP %d", url, resp.status)
                    return None
                data = await resp.read()
            ext = ".jpg"
            ctype = resp.headers.get("Content-Type", "")
            if "png" in ctype:
                ext = ".png"
            out_dir = Path(tempfile.gettempdir()) / "hermes-rokid-media"
            out_dir.mkdir(parents=True, exist_ok=True)
            safe_id = "".join(c for c in request_id if c.isalnum() or c in "-_") or uuid.uuid4().hex
            path = out_dir / f"{safe_id}_{int(time.time() * 1000)}{ext}"
            path.write_bytes(data)
            return str(path)
        except Exception as exc:
            logger.warning("[rokid] image download failed (%s): %s", url, exc)
            return None

    # ------------------------------------------------- turn suppression
    def _release_suppression(self, chat_id: str) -> None:
        """Turn resumed (device follow-up arrived): stop swallowing answers and
        cancel the scheduled abandonment close."""
        self._suppressed_until.pop(str(chat_id), None)
        task = self._suppress_tasks.pop(str(chat_id), None)
        if task is not None and not task.done():
            task.cancel()

    def _is_suppressed(self, chat_id: str) -> bool:
        """True while this chat is paused on a device-command frame, when the
        model's trailing answer text must not reach the device. A background
        task owns the deadline; this only reads the flag."""
        return str(chat_id) in self._suppressed_until

    async def _close_abandoned_turn(self, chat_id: str, request_id: str, delay: float) -> None:
        """Fallback net: if the device never sends the follow-up frame, close
        its waiting request with a done frame so the turn does not hang."""
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        if self._pending_request.get(str(chat_id)) != request_id:
            return
        self._suppressed_until.pop(str(chat_id), None)
        self._suppress_tasks.pop(str(chat_id), None)
        logger.info("[rokid] no device follow-up on chat=%s; closing turn with done", chat_id)
        await self._send_json(self._answer_frame(request_id, "", True))
        if self._pending_request.get(str(chat_id)) == request_id:
            self._pending_request.pop(str(chat_id), None)

    # ------------------------------------------------------------- outbound send
    async def _send_json(self, frame: Dict[str, Any]) -> bool:
        async with self._send_lock:
            if self._ws is None or self._ws.closed:
                logger.warning("[rokid] cannot send; socket not open")
                return False
            try:
                await self._ws.send_str(json.dumps(frame, ensure_ascii=False))
                return True
            except Exception as exc:
                logger.warning("[rokid] send failed: %s", exc)
                return False

    def _answer_frame(self, request_id: str, stream: str, finish: bool) -> Dict[str, Any]:
        return {
            "event": "done" if finish else "message",
            "data": {
                "role": "agent",
                "message_id": request_id,
                "agent_id": self.agent_id,
                "answer_stream": "" if finish else stream,
                "is_finish": finish,
                "type": "answer",
            },
        }

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        # Paused on a device-command frame (awaiting the photo): swallow the
        # model's trailing text so the device stays silent until its follow-up.
        # Report success so the gateway neither retries nor surfaces an error.
        if self._is_suppressed(str(chat_id)):
            logger.info("[rokid] suppressed trailing answer on chat=%s", chat_id)
            return SendResult(success=True, message_id=self._pending_request.get(str(chat_id)))
        request_id = self._pending_request.get(str(chat_id))
        if not request_id:
            return SendResult(success=False, error="no pending device request for this chat")
        text = content or ""
        if text:
            # Glasses must never display chain-of-thought: strip the gateway's
            # pre-send reasoning block here (this adapter only -> this platform
            # only; other platforms are untouched).
            text = _strip_reasoning(text)
            if not await self._send_json(self._answer_frame(request_id, text, False)):
                return SendResult(success=False, error="socket send failed", retryable=True)
        if not await self._send_json(self._answer_frame(request_id, "", True)):
            return SendResult(success=False, error="socket done-frame failed", retryable=True)
        return SendResult(success=True, message_id=request_id)

    async def send_answer(self, chat_id: str, content: str) -> bool:
        """Helper used by device tools / future streaming paths."""
        if self._is_suppressed(str(chat_id)):
            return True
        request_id = self._pending_request.get(str(chat_id))
        if not request_id:
            return False
        return await self._send_json(self._answer_frame(request_id, content, False))

    async def send_tool_call(self, tool_call: Dict[str, Any], chat_id: Optional[str] = None) -> bool:
        """Push a device command frame (take_photo / navigation / calendar / exit).

        Per the protocol contract (protocol.ts WsBridgeToolCallFrame) this is a
        NON-terminal mid-turn frame: ``event:"message"`` / ``is_finish:false``.
        The device runs the command and sends its follow-up (e.g. the photo) as
        the next inbound frame on the SAME requestId; the model's final answer
        then closes the turn with the normal done frame. Keeping the request
        open is what lets the glasses display that final reply.
        """
        target_chat = str(chat_id or self._chat_id)
        request_id = self._pending_request.get(target_chat)
        if not request_id:
            logger.warning("[rokid] cannot emit tool_call; no pending request on %s", target_chat)
            return False
        frame = {
            "event": "message",
            "data": {
                "role": "agent",
                "message_id": request_id,
                "agent_id": self.agent_id,
                "is_finish": False,
                "type": "tool_call",
                "tool_call": tool_call,
            },
        }
        ok = await self._send_json(frame)
        if ok:
            # Pause the turn: swallow the model's trailing text until the
            # device follows up. Replace any earlier pause (same requestId can
            # carry corrected transcript frames); keep _pending_request so the
            # eventual answer is deliverable on this still-open request.
            old_task = self._suppress_tasks.pop(target_chat, None)
            if old_task is not None and not old_task.done():
                old_task.cancel()
            self._suppressed_until[target_chat] = time.monotonic() + SUPPRESS_TTL_SECONDS
            self._suppress_tasks[target_chat] = asyncio.create_task(
                self._close_abandoned_turn(target_chat, request_id, SUPPRESS_TTL_SECONDS)
            )
        return ok

    async def _describe_photo(self, paths: List[str]) -> str:
        """Run the system vision enrichment (configured auxiliary.vision) on the
        downloaded photo and return its description; empty string on failure."""
        prompt = (
            "用2-4句话简洁描述这张图片，说明主体、关键文字/数据和整体场景。"
            "如果是图表，包含重要标签和数值。"
        )
        parts: List[str] = []
        try:
            from tools.vision_tools import vision_analyze_tool
            from agent.memory_manager import sanitize_context
            for p in paths:
                raw = await vision_analyze_tool(image_url=p, user_prompt=prompt)
                obj = json.loads(raw)
                if obj.get("success") and obj.get("analysis"):
                    parts.append(sanitize_context(obj["analysis"]))
        except Exception:
            logger.debug("vision enrichment failed", exc_info=True)
        return "\n".join(parts)

    async def wait_for_photo(self, timeout: float) -> Optional[Dict[str, Any]]:
        """Block the calling device-command tool until the device's follow-up
        image frame arrives, returning the downloaded photo path(s); returns
        None on timeout (photo never came). Runs in a worker thread, so the
        blocking wait does not stall the gateway event loop."""
        chat_id = self._chat_id
        wait: Dict[str, Any] = {"event": threading.Event(), "paths": []}
        # A corrected-transcript frame could re-enter; reuse the existing wait.
        existing = self._photo_waits.get(chat_id)
        if existing is not None:
            wait = existing
        else:
            self._photo_waits[chat_id] = wait
        try:
            if await asyncio.to_thread(wait["event"].wait, timeout):
                return {
                    "paths": list(wait.get("paths") or []),
                    "description": str(wait.get("description") or ""),
                }
            return None
        finally:
            if self._photo_waits.get(chat_id) is wait:
                self._photo_waits.pop(chat_id, None)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": f"Rokid {self.link_code}", "type": "dm"}
