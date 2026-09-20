# Rokid Glasses Hermes Bridge

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**桥接适配器，将官方 Rokid OpenClaw 插件接入 Hermes Agent**

## ⚠️ 重要说明

本插件**不是**独立的 Rokid 网关 —— 它**必须配合官方 Rokid OpenClaw 插件使用**：

- **官方插件** (`rokid-openclaw-gateway-compatible`)：负责与 Rokid RCS WebSocket 建立连接，处理设备配对、消息收发
- **本桥接插件**：将官方插件的协议转换为 Hermes 平台适配器格式，使 Hermes Agent 能够接收眼镜的语音/文本/图片输入，并下发回答和设备命令（拍照、导航、日程、退出）

**如果你还没有官方 OpenClaw 插件，请先获取它并完成设备配对，拿到 `linkCode` 和 `linkSecret` 后再使用本插件。**

## 📦 安装

### 前置条件

1. 已获取 Rokid 眼镜的配对凭证：
   - `linkCode`（设备配对码）
   - `linkSecret`（设备配对密钥）
2. Hermes Agent 已安装（桌面版或 CLI）

### 步骤

1. **复制插件到 Hermes 插件目录**

   ```bash
   # 将整个插件目录复制到
   C:\Users\<你的用户名>\AppData\Local\hermes\plugins\rokid-glasses-hermes-bridge\
   ```

2. **安装 Python 依赖**

   ```bash
   pip install aiohttp>=3.9.0
   ```

3. **配置凭证**

   在 Hermes 配置中添加环境变量（二选一）：

   **方法 A：`.env` 文件**（推荐）

   在 `C:\Users\<你的用户名>\.env` 中添加：

   ```env
   ROKID_LINK_CODE=你的 linkCode
   ROKID_LINK_SECRET=你的 linkSecret
   # 可选：自定义 WebSocket 端点（默认使用官方端点）
   # ROKID_WS_URL=wss://rcs.rokid.com/claw/ws/link
   # 可选：agent_id（默认 main）
   # ROKID_AGENT_ID=main
   ```

   **方法 B：Hermes 配置**

   ```bash
   hermes config set gateway.platforms.rokid.extra.linkCode "你的 linkCode"
   hermes config set gateway.platforms.rokid.extra.linkSecret "你的 linkSecret"
   ```

4. **启用插件**

   ```bash
   hermes plugins enable rokid-glasses-hermes-bridge
   hermes config set gateway.platforms.rokid.enabled true
   ```

5. **启动 Gateway**

   ```bash
   hermes gateway start
   ```

   或设置为开机自启：

   ```bash
   hermes gateway install
   ```

## 🔧 配置项

| 环境变量 | 说明 | 默认值 | 必填 |
|---------|------|--------|------|
| `ROKID_LINK_CODE` | 设备配对码（从官方插件获取） | - | ✅ |
| `ROKID_LINK_SECRET` | 设备配对密钥（从官方插件获取） | - | ✅ |
| `ROKID_WS_URL` | Rokid RCS WebSocket 端点 | `wss://rcs.rokid.com/claw/ws/link` | ❌ |
| `ROKID_AGENT_ID` |  outbound frame 中的 agent_id | `main` | ❌ |

## 🎯 功能

### 输入（眼镜 → Hermes）

- ✅ 语音转文本消息
- ✅ 文本输入
- ✅ 拍照图片（自动下载到本地临时目录）
- ✅ 会话取消请求（`cancel` 类型）

### 输出（Hermes → 眼镜）

- ✅ 文本回答（流式/整段）
- ✅ 设备命令：
  - `take_photo`：拍照 📷
  - `take_navigation`：导航 🧭（支持驾车/步行/骑行）
  - `control_calendar`：创建日程 📅
  - `notify_agent_off`：退出对话 👋

## 📝 使用示例

### 拍照

```
用户（眼镜）："帮我看看这个"
→ Hermes 调用 `take_photo` 工具
→ 眼镜拍照并上传图片
→ Hermes 分析图片并回答
```

### 导航

```
用户（眼镜）："我想去西湖"
→ Hermes 调用 `take_navigation` 工具
→ 参数：`{"action": "open", "poi_name": "西湖", "navi_type": "1"}`
→ 眼镜打开步行导航
```

### 创建日程

```
用户（眼镜）："明天上午 10 点提醒我开会"
→ Hermes 调用 `control_calendar` 工具
→ 参数：`{"action": "create", "title": "开会", "start_time": "2026-09-21T10:00:00+08:00"}`
→ 眼镜创建日程
```

## 🐛 故障排查

### Gateway 启动后显示 "not configured"

**原因**：环境变量未加载或凭证缺失

**解决**：

```bash
# 检查凭证是否配置
hermes plugins doctor rokid-glasses-hermes-bridge

# 检查 Gateway 状态
hermes gateway status
```

### 设备消息无法到达 Hermes

**原因**：官方 OpenClaw 插件未运行或 WebSocket 连接断开

**解决**：

1. 确认官方插件已正确配置并运行
2. 检查 `ROKID_LINK_CODE` 和 `ROKID_LINK_SECRET` 是否匹配
3. 查看 Gateway 日志：

   ```bash
   hermes gateway logs --follow
   ```

### 设备命令下发失败

**原因**：设备未处于活跃会话中

**解决**：

- 确保眼镜先发送了一条消息（建立 `requestId` 会话）
- 检查 `_active_adapter` 是否已连接（日志中应有 `[rokid] connected to ...`）

## 📁 目录结构

```
rokid-glasses-hermes-bridge/
├── plugin.yaml          # 插件清单
├── __init__.py          # 入口点（register 函数）
├── adapter.py           # 平台适配器核心逻辑
├── tools.py             # 设备命令工具
├── README.md            # 本文档
├── LICENSE              # MIT 许可证
└── CHANGELOG.md         # 版本历史
```

## 🔗 相关资源

- [Hermes Agent 文档](https://hermes-agent.nousresearch.com/docs)
- [Rokid OpenClaw 官方插件](https://github.com/rokid/rokid-openclaw-gateway-compatible)（需确认实际仓库地址）
- [Hermes 平台适配器开发指南](https://hermes-agent.nousresearch.com/docs/gateway/platform-adapters)

## 📄 许可证

MIT License —— 见 [LICENSE](LICENSE) 文件

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

**开发时间**：2026-09  
**适配协议版本**：OpenClaw 兼容协议（commit 522cf3c）
