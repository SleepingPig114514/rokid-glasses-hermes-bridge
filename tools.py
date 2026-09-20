"""Device-command tools for Rokid glasses.

Each tool translates a model call into a ``tool_call`` frame pushed down the
live adapter socket. Ported from the OpenClaw plugin's device-tools.ts.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

TOOLSET = "rokid"


def _adapter():
    from .adapter import _active_adapter
    return _active_adapter


def _err(msg: str) -> str:
    return f"错误：{msg}"


# ---------------------------------------------------------------------- schemas

TAKE_PHOTO_SCHEMA = {
    "type": "object",
    "properties": {},
    "required": [],
}

NAVIGATION_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["open", "close"],
            "description": "导航动作：open 打开导航，close 关闭导航",
        },
        "poi_name": {
            "type": "string",
            "description": '导航目的地名称，例如"西湖"、"杭州东站"',
        },
        "navi_type": {
            "type": "string",
            "enum": ["0", "1", "2"],
            "description": "导航类型：0 驾车、1 步行、2 骑行",
        },
    },
    "required": ["action", "poi_name"],
}

CALENDAR_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["create"],
            "description": "日程动作：create 创建日程",
        },
        "title": {"type": "string", "description": "日程标题"},
        "start_time": {"type": "string", "description": "日程开始时间，ISO 8601 格式"},
        "end_time": {"type": "string", "description": "日程结束时间，ISO 8601 格式（可选）"},
    },
    "required": ["action", "title", "start_time"],
}

AGENT_OFF_SCHEMA = {
    "type": "object",
    "properties": {},
    "required": [],
}


# --------------------------------------------------------------------- handlers

async def _emit(tool_call: Dict[str, Any]) -> str:
    adapter = _adapter()
    if adapter is None:
        return _err("Rokid 设备未连接，无法下发命令")
    ok = await adapter.send_tool_call(tool_call)
    return "命令已发送到设备" if ok else _err("设备命令下发失败（连接不可用）")


PHOTO_WAIT_TIMEOUT = 90.0


async def handle_take_photo(args: Dict[str, Any], **_kw: Any) -> str:
    adapter = _adapter()
    if adapter is None:
        return _err("Rokid 设备未连接，无法下发命令")
    ok = await adapter.send_tool_call({"command": "take_photo"})
    if not ok:
        return _err("设备命令下发失败（连接不可用）")
    # Block THIS turn until the device sends the photo back. The photo plus its
    # vision description become the tool result, so no premature answer is
    # produced and no extra/self-initiated vision call is made; the model gives
    # its single final reply after this returns.
    result = await adapter.wait_for_photo(PHOTO_WAIT_TIMEOUT)
    if not result or not result.get("paths"):
        return (
            "拍照命令已下发，但在等待时间内没有收到眼镜回传的照片（可能未拍摄或网络异常）。"
            "请据此向用户说明拍照未完成，不要假设已拍到内容。"
        )
    paths = result["paths"]
    description = result.get("description") or ""
    desc_block = f"\n画面内容：{description}" if description else "\n（未能自动生成画面描述，必要时用 vision_analyze 结合该路径查看。）"
    return (
        "眼镜已完成拍照，照片已下载到本地：" + "；".join(paths) + desc_block +
        "。用户最初的文字请求和这张照片现在合在一起处理：直接结合上述画面内容给出最终回复；"
        "如用户要求保存，可直接使用该本地文件。不要描述拍照过程，不要让用户再次确认。"
    )


async def handle_take_navigation(args: Dict[str, Any], **_kw: Any) -> str:
    action = args.get("action")
    poi = args.get("poi_name")
    if not action or not poi:
        return _err("缺少必要参数，action 和 poi_name 为必填项")
    call: Dict[str, Any] = {"command": "take_navigation", "action": action, "poi_name": poi}
    if args.get("navi_type"):
        call["navi_type"] = str(args["navi_type"])
    return await _emit(call)


async def handle_control_calendar(args: Dict[str, Any], **_kw: Any) -> str:
    action = args.get("action")
    title = args.get("title")
    start = args.get("start_time")
    if not action or not title or not start:
        return _err("缺少必要参数，action、title 和 start_time 为必填项")
    call: Dict[str, Any] = {
        "command": "control_calendar",
        "action": action,
        "title": title,
        "start_time": start,
    }
    if args.get("end_time"):
        call["end_time"] = args["end_time"]
    return await _emit(call)


async def handle_notify_agent_off(args: Dict[str, Any], **_kw: Any) -> str:
    return await _emit({"command": "notify_agent_off"})


TOOLS = (
    ("take_photo", TAKE_PHOTO_SCHEMA, handle_take_photo,
     "📷",
     '向设备下发拍照命令。当用户要求拍照、拍摄、截图、看看周围、看看这个、拍一下，'
     '或用户提到图片但实际并未提供图片（如"这张照片怎么样""帮我看看这个"），'
     '或用户想要记录眼前画面时调用。调用本工具时只输出工具调用本身，'
     '不要在调用前后输出任何文字（如"好的""请对准画面"）；工具会等待照片并在'
     '照片回来后由你给出最终回复。'),
    ("take_navigation", NAVIGATION_SCHEMA, handle_take_navigation,
     "🧭",
     "向设备下发导航命令。当用户想去某个地方、从A到B、问怎么走、问路线、"
     "提到开车/步行/骑行去某处、想回家、想去某个地点时调用。必须从用户描述中提取"
     "目的地填入 poi_name 参数，action 设为 open。如用户提到交通方式则设置 "
     "navi_type（驾车=0，步行=1，骑行=2）。如用户要求关闭/停止导航，action 设为 close。"),
    ("control_calendar", CALENDAR_SCHEMA, handle_control_calendar,
     "📅",
     "向设备下发创建日程命令。当用户想设置日程、提醒、闹钟、备忘、约会、会议安排，"
     "或提到某个时间要做某事时调用。必须从用户描述中提取日程标题填入 title，"
     "提取时间填入 start_time（ISO 8601 格式），action 设为 create。"),
    ("notify_agent_off", AGENT_OFF_SCHEMA, handle_notify_agent_off,
     "👋",
     "向设备下发退出命令。当用户说退出、结束、关闭、不聊了、拜拜、再见时调用。"),
)
