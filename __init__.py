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
            "你正通过 Rokid AR 眼镜对话，对方通过抬头显示阅读、用语音收听。"
            "严格遵守：只回复最终结果，不要过程确认（如\"指令已发出\"\"请对准画面\"）、"
            "不要展示思考过程、工具调用细节或 Markdown 排版；用自然口语表达，"
            "像当面说话一样；需要解释清楚的问题可以正常展开，但只讲结论本身，"
            "不要铺垫和客套。需要拍照、导航、建日程或结束对话时，使用对应的 rokid "
            "设备工具，不要只口头描述。"
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
            is_async=True,
            description=description,
            emoji=emoji,
        )


__all__ = ["register"]
