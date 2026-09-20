"""Rokid glasses platform plugin for Hermes.

Entry point discovered by the Hermes plugin loader: ``register(ctx)`` registers
the ``rokid`` platform adapter and its four device-command tools.
"""

from __future__ import annotations

import os
from typing import Any

from .adapter import (
    DEFAULT_WS_URL,
    PLATFORM_NAME,
    RokidBridgeAdapter,
    _active_adapter,
)
from .tools import TOOLS, TOOLSET


def _creds() -> tuple[str, str]:
    code = os.getenv("ROKID_LINK_CODE", "").strip()
    secret = os.getenv("ROKID_LINK_SECRET", "").strip()
    if code and secret:
        return code, secret
    # YAML extras (gateway.platforms.rokid.extra) are checked at adapter build;
    # the passive check only sees the environment, which is enough for status.
    return code, secret


def check_requirements() -> bool:
    """PASSIVE probe: are the pairing credentials present? Never installs anything."""
    try:
        import aiohttp  # noqa: F401
    except Exception:
        return False
    code, secret = _creds()
    return bool(code and secret)


def validate_config(config: Any) -> bool:
    extra = getattr(config, "extra", {}) or {}
    code = os.getenv("ROKID_LINK_CODE") or extra.get("link_code") or extra.get("linkCode")
    secret = os.getenv("ROKID_LINK_SECRET") or extra.get("link_secret") or extra.get("linkSecret")
    return bool(code and secret)


def _env_enablement() -> dict | None:
    code, secret = _creds()
    if not (code and secret):
        return None
    seed: dict[str, Any] = {"link_code": code, "link_secret": secret}
    if os.getenv("ROKID_WS_URL"):
        seed["ws_url"] = os.getenv("ROKID_WS_URL")
    if os.getenv("ROKID_AGENT_ID"):
        seed["agent_id"] = os.getenv("ROKID_AGENT_ID")
    seed["home_channel"] = {"chat_id": code, "name": f"Rokid {code}"}
    return seed


def _device_tool_available() -> bool:
    return _active_adapter is not None


def register(ctx: Any) -> None:
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="Rokid Glasses",
        adapter_factory=lambda cfg: RokidBridgeAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["ROKID_LINK_CODE", "ROKID_LINK_SECRET"],
        install_hint="Set ROKID_LINK_CODE and ROKID_LINK_SECRET (from the glasses pairing screen).",
        env_enablement_fn=_env_enablement,
        allowed_users_env="ROKID_ALLOWED_USERS",
        allow_all_env="ROKID_ALLOW_ALL_USERS",
        max_message_length=2000,
        emoji="👓",
        platform_hint=(
            "你正通过 Rokid AR 眼镜对话。回复要简洁口语化，适合抬头显示和语音播报；"
            "需要拍照、导航、建日程或结束对话时，使用对应的 rokid 设备工具，不要只口头描述。"
        ),
    )

    for name, parameters_schema, handler, emoji, description in TOOLS:
        ctx.register_tool(
            name=name,
            toolset=TOOLSET,
            schema={
                "name": name,
                "description": description,
                "parameters": parameters_schema,
            },
            handler=handler,
            check_fn=_device_tool_available,
            is_async=True,
            description=description,
            emoji=emoji,
        )


__all__ = ["register"]
