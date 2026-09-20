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
    tool call:    {"event": "done",    "data": {role:"agent", message_id,
                   agent_id, is_finish:true, type:"tool_call",
                   tool_call:{command, ...}}}
    status:       {"type": "status", "connected": true}   # sent on open

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

# Process-wide handle to the live adapter so the device-command tools can push
# tool_call frames down the active socket. Single device account per profile for
# now; a dict keyed by linkCode can replace this if multi-device is needed.
_active_adapter: Optional["RokidBridgeAdapter"] = None


def _backoff_delay(attempt: int) -> float:
    delay = RECONNECT_BASE_DELAY * (2 ** attempt)
    jitter = random.uniform(0, RECONNECT_BASE_DELAY)
    return min(delay + jitter, RECONNECT_MAX_DELAY)


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
        self._pending_request[chat_id] = request_id

        media_paths: List[str] = []
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
        request_id = self._pending_request.get(str(chat_id))
        if not request_id:
            return SendResult(success=False, error="no pending device request for this chat")
        text = content or ""
        if text:
            if not await self._send_json(self._answer_frame(request_id, text, False)):
                return SendResult(success=False, error="socket send failed", retryable=True)
        if not await self._send_json(self._answer_frame(request_id, "", True)):
            return SendResult(success=False, error="socket done-frame failed", retryable=True)
        return SendResult(success=True, message_id=request_id)

    async def send_answer(self, chat_id: str, content: str) -> bool:
        """Helper used by device tools / future streaming paths."""
        request_id = self._pending_request.get(str(chat_id))
        if not request_id:
            return False
        return await self._send_json(self._answer_frame(request_id, content, False))

    async def send_tool_call(self, tool_call: Dict[str, Any], chat_id: Optional[str] = None) -> bool:
        """Push a device command frame (take_photo / navigation / calendar / exit).

        A tool_call is terminal: it carries is_finish=true and closes the turn.
        """
        target_chat = chat_id or self._chat_id
        request_id = self._pending_request.get(str(target_chat))
        if not request_id:
            logger.warning("[rokid] cannot emit tool_call; no pending request on %s", target_chat)
            return False
        frame = {
            "event": "done",
            "data": {
                "role": "agent",
                "message_id": request_id,
                "agent_id": self.agent_id,
                "is_finish": True,
                "type": "tool_call",
                "tool_call": tool_call,
            },
        }
        ok = await self._send_json(frame)
        if ok:
            self._pending_request.pop(str(target_chat), None)
        return ok

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": f"Rokid {self.link_code}", "type": "dm"}
