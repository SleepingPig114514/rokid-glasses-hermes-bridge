# Rokid Glasses Hermes Bridge

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Rokid AR 眼镜的 Hermes Agent 平台适配器（独立网关）**

通过 Rokid RCS 的出站 WebSocket，把眼镜接入 Hermes Agent：眼镜的语音/文本/图片消息进入 Agent，Agent 的回答和设备命令（拍照、导航、日程、退出）下发到眼镜。

## ✅ 工作原理

本插件是一个**独立、完整**的平台适配器，**不需要**安装或运行官方 OpenClaw 插件。它直接使用设备配对凭证连接：

```
wss://rcs.rokid.com/claw/ws/link?linkCode=...&linkSecret=...
```

- **无需公网入站地址**：连接由本机主动发起（出站 WebSocket）。
- 协议在官方 OpenClaw 插件 `rokid-openclaw-gateway-compatible` 的基础上移植并修正。

### 安装后需要配置什么？

| 对象 | 是否需要 | 说明 |
|------|---------|------|
| 官方 OpenClaw 插件 | ❌ 不需要 | 本插件直连 RCS，不依赖官方插件，也无需在其中配置 |
| Hermes：设备凭证 | ✅ 需要 | 设置 `ROKID_LINK_CODE` / `ROKID_LINK_SECRET` |
| Hermes：启用平台 | ✅ 需要 | 安装后**不会自动启用**，需设置 `gateway.platforms.rokid.enabled: true` |
| Hermes：视觉模型 | ⚠️ 强烈建议 | 配置 `auxiliary.vision`，否则纯文本模型看图会触发工具调用并可能超时（见下文） |

详细步骤见下方「安装」。

## 📦 安装

### 前置条件

1. Hermes Agent（桌面版或 CLI）
2. Python 依赖 `aiohttp>=3.9.0`
3. 眼镜配对界面提供的凭证：
   - `linkCode`（设备配对码）
   - `linkSecret`（设备配对密钥）
4. 一个可用的视觉模型（用于识别眼镜拍摄的画面，见下文）

### 步骤

1. **复制插件到 Hermes 插件目录**

   目录名必须为 `rokid-bridge`（需与插件名、配置 key 一致）：

   ```
   C:\Users\<你的用户名>\AppData\Local\hermes\plugins\rokid-bridge\
   ```

2. **安装 Python 依赖**

   ```bash
   pip install "aiohttp>=3.9.0"
   ```

3. **配置凭证**（二选一）

   **方法 A：`.env` 文件**（推荐）

   在用户主目录的 `.env` 文件（`C:\Users\<你的用户名>\.env`）中添加：

   ```env
   ROKID_LINK_CODE=你的 linkCode
   ROKID_LINK_SECRET=你的 linkSecret
   # 可选：自定义 WebSocket 端点
   # ROKID_WS_URL=wss://rcs.rokid.com/claw/ws/link
   # 可选：outbound 帧中的 agent_id（默认 main）
   # ROKID_AGENT_ID=main
   ```

   **方法 B：Hermes 配置**

   ```bash
   hermes config set gateway.platforms.rokid.extra.link_code "你的 linkCode"
   hermes config set gateway.platforms.rokid.extra.link_secret "你的 linkSecret"
   ```

4. **启用平台**

   ```bash
   hermes config set gateway.platforms.rokid.enabled true
   ```

5. **启动 Gateway**

   ```bash
   hermes gateway start
   ```

   设置开机自启：

   ```bash
   hermes gateway install
   ```

   > 桌面 App 默认不会自动拉起消息 Gateway，建议执行一次 `hermes gateway install`。

## 👁️ 视觉模型配置（重要）

眼镜拍照后，画面需要由视觉模型识别。在 Hermes 配置中指定 `auxiliary.vision`：

```yaml
auxiliary:
  vision:
    provider: alibaba-cn
    model: qwen3.5-omni-flash
```

- Agent 的 `image_input_mode: auto` 下，只要配置了 `auxiliary.vision`，图片就会自动交给该视觉模型识别，再把描述交给主模型。
- 本插件**跟随你的配置**，没有写死具体模型；更换视觉模型后无需改动插件，只要新模型支持视觉输入。
- 正常情况下一次图片识别约 1–3 秒。

> **为什么需要手动配置视觉模型？**
> 如果只使用普通（纯文本）语言模型，眼镜回传图片时模型无法直接理解画面，会在对话中途自行调用视觉分析工具来"看图"。这会额外增加一次模型调用并显著拖慢回合（实测可能达到数十秒甚至更久），很容易超过眼镜约 30 秒的等待窗口，导致设备提示"回复异常"、收不到最终回复。因此**推荐手动配置一个多模态视觉模型**（如上），让图片识别由系统前置、快速完成，避免回合中途的工具调用和超时。

## 🔧 配置项

| 环境变量 | 说明 | 默认值 | 必填 |
|---------|------|--------|------|
| `ROKID_LINK_CODE` | 设备配对码 | - | 是 |
| `ROKID_LINK_SECRET` | 设备配对密钥 | - | 是 |
| `ROKID_WS_URL` | RCS WebSocket 端点 | `wss://rcs.rokid.com/claw/ws/link` | 否 |
| `ROKID_AGENT_ID` | outbound 帧中的 agent_id | `main` | 否 |

## 🎯 功能

### 输入（眼镜 → Hermes）

- 语音转文本、文本消息
- 拍照图片（自动下载到本地临时目录）
- 会话取消请求（`cancel`，当前仅记录）

### 输出（Hermes → 眼镜）

- 文本回答（整段 + done 帧）
- **自动隐藏思考过程**：回复发出前会在适配器内剥离 reasoning 块，眼镜上不会显示思维链（仅对眼镜生效，桌面端等其他平台不受影响）
- 设备命令：
  - `take_photo`：拍照 📷
  - `take_navigation`：导航 🧭（驾车/步行/骑行）
  - `control_calendar`：创建日程 📅
  - `notify_agent_off`：退出对话 👋

### 拍照回合的时序

设备命令帧是非终止的中间帧，回合在工具内部等待设备回传：

```
用户："帮我看看这是什么"
 → 模型只调用 take_photo（不输出任何文字，眼镜保持静默）
 → 眼镜拍照并回传图片
 → 视觉模型（auxiliary.vision）识别画面
 → 主模型结合原始文字 + 画面给出唯一一次最终回复
```

这样不会产生提前回复，也省掉一次额外的模型往返。若设备在 90 秒内未回传照片，工具返回"拍照未完成"，回合不会挂死。

## 📝 使用示例

### 导航

```
用户："我想去西湖"
 → take_navigation，参数 {"action":"open","poi_name":"西湖","navi_type":"1"}
```

### 创建日程

```
用户："明天上午 10 点提醒我开会"
 → control_calendar，参数 {"action":"create","title":"开会",
                          "start_time":"2026-09-22T10:00:00+08:00"}
```

## 🐛 故障排查

### 启动后平台显示未配置 / "No messaging platforms enabled"

- 确认 `ROKID_LINK_CODE` 和 `ROKID_LINK_SECRET` 已加载。
- 确认插件目录名为 `rokid-bridge`，且 `gateway.platforms.rokid.enabled: true`（目录名、插件 name、配置 key 三者必须一致）。

### 无法连接 RCS

- 检查网络与凭证是否与眼镜配对界面一致。
- 查看日志中 `[rokid] connected to ...`；断线会按指数退避自动重连（最多 10 次）。

### 眼镜提示"回复异常" / 收不到回复

- 多为总耗时超过设备等待窗口（约 30 秒）。
- 确认 `auxiliary.vision` 配置的视觉模型可用且未欠费；视觉模型异常会显著拖慢或阻断回复。
- 避免主模型在回合中途自行调用视觉工具——正常流程由插件自动完成图片识别。

### 设备命令下发失败

- 设备需先发送一条消息以建立待处理请求（`requestId`）。
- 日志中应有 `[rokid] connected to ...`。

## 📁 目录结构

```
rokid-bridge/
├── plugin.yaml     # 插件清单
├── __init__.py     # 入口点（register）
├── adapter.py      # 平台适配器与 WebSocket 逻辑
├── tools.py        # 设备命令工具
├── README.md
├── CHANGELOG.md
└── LICENSE
```

## 🔗 相关资源

- [Hermes Agent 文档](https://hermes-agent.nousresearch.com/docs)
- [Rokid OpenClaw 官方插件 (Gitee)](https://gitee.com/rokid-eco/rokid-openclaw-gateway-compatible)
- [Hermes 平台适配器开发指南](https://hermes-agent.nousresearch.com/docs/gateway/platform-adapters)

## 📦 镜像仓库

- **Gitee (国内)**：https://gitee.com/he-shuying/rokid-glasses-hermes-bridge
- **GitHub (国际)**：https://github.com/SleepingPig114514/rokid-glasses-hermes-bridge

## 📄 许可证

MIT License —— 见 [LICENSE](LICENSE)。
