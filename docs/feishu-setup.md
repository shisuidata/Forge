# Feishu Bot Setup

## 1. Create a Feishu App

1. Go to [open.feishu.cn](https://open.feishu.cn) → Create App → Custom App
2. Name it (e.g., "Forge 数据助手")
3. Note down **App ID** and **App Secret**

## 2. Configure Permissions

In **Permission Management**, enable:

| Permission | Purpose |
|---|---|
| `im:message:send_as_bot` | Send messages as bot |
| `im:message.p2p_msg:readonly` | Receive DMs |
| `im:message.group_at_msg:readonly` | Receive @mentions in groups |

## 3. Configure Event Subscription

In **Event & Callback → Event Subscription**:

1. Set subscription mode to **HTTP**
2. Set request URL: `https://your-server/webhook/feishu`
3. Note down **Verification Token** and **Encrypt Key**
4. Subscribe to event: `im.message.receive_v1`

## 4. Configure .env

```bash
FEISHU_APP_ID=cli_xxxxxxxxxxxxxxxx
FEISHU_APP_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
FEISHU_VERIFICATION_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
FEISHU_ENCRYPT_KEY=                # leave empty if not using encryption
```

## 5. Publish the App

Go to **Version Management → Create Version → Submit for Review** (or publish directly for internal enterprise apps).

## 6. Add Bot to Workspace

After publishing, invite the bot to a group or start a DM in Feishu.

## Local Development

Use [ngrok](https://ngrok.com) to expose a local server:

```bash
uvicorn main:app --port 8000
ngrok http 8000
# Use the ngrok HTTPS URL as your webhook URL
```

## Testing

Send a message to the bot:

```
统计每个城市的用户数量
```

The bot should reply with an interactive card showing the generated SQL.
