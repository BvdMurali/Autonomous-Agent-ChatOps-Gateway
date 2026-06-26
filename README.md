# Antigravity ChatOps Gateway ⚡

A production-grade **Python FastAPI** ChatOps relay server that bridges your autonomous AI agent
to a **Telegram Mini App** frontend via a live WebSocket event bus.

## Architecture

```
[Antigravity Agent] → HTTP POST /api/agent/before-action
                              ↓
                     [FastAPI + HITL Manager (asyncio.Future)]
                              ↓ (suspends until operator decision)
          [Telegram Bot] ←→ [WebSocket /ws] ←→ [Telegram Mini App]
```

## Features

- **Human-in-the-Loop (HITL)**: Suspends agent execution mid-flight using `asyncio.Future`,
  waiting for a Telegram operator to Approve, Abort, or Override.
- **WebSocket Event Bus**: Real-time bidirectional channel between the Python backend
  and the Telegram Mini App terminal UI.
- **Telegram Bot Integration**: `/status`, `/abort`, `/dashboard` commands with inline
  keyboard callbacks via `python-telegram-bot`.
- **Telegram Mini App**: Dark glassmorphism terminal UI served at `/static/index.html`
  with live log streaming and HITL action buttons.
- **Async SQLite Telemetry**: Token cost logging and agent state tracking via `aiosqlite`.
- **Cloudflare Tunnel Ready**: Runs cleanly behind a Cloudflare TLS tunnel with graceful
  NetworkError handling.
- **Local Execution Engine**: WebSocket `execute` action spawns local subprocess commands
  and streams stdout/stderr line-by-line back to the Mini App.

## Quick Start

```bash
# 1. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set environment variables
export TELEGRAM_BOT_TOKEN="your_token"
export TELEGRAM_CHAT_ID="your_chat_id"
export WEBHOOK_URL="https://your.cloudflare.tunnel.com"

# 4. Run
uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | Yes | BotFather token |
| `TELEGRAM_CHAT_ID` | Recommended | Locks bot to admin chat |
| `WEBHOOK_URL` | Production | Cloudflare tunnel base URL |
| `PORT` | No | Server port (default: 8080) |
| `DB_PATH` | No | SQLite path (default: `./antigravity_telemetry.db`) |

## API Reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/agent/before-action` | HITL intercept hook |
| `POST` | `/api/telemetry/token-log` | Log token usage |
| `POST` | `/api/agent/state` | Update agent state key/value |
| `POST` | `/api/ws/broadcast` | Push log event to all WS clients |
| `WS` | `/ws` | Live event bus |
| `GET` | `/static/index.html` | Telegram Mini App UI |

## Telegram Bot Commands

| Command | Description |
|---|---|
| `/status` | Show gateway health, telemetry summary, HITL state |
| `/abort` | Abort the currently pending agent action |
| `/dashboard` | Open the Telegram Mini App workspace |

## License

MIT
