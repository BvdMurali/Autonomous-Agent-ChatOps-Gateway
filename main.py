"""
antigravity-telegram-gateway — main.py
Production-grade FastAPI backend with:
  - Human-in-the-Loop (HITL) manager via asyncio.Future
  - WebSocket event bus (/ws)
  - python-telegram-bot webhook/polling integration
  - Async SQLite telemetry (aiosqlite)
  - Static file serving for Telegram Mini App
"""

from __future__ import annotations

import asyncio
import logging
import os
import shlex
from contextlib import asynccontextmanager
from typing import Any

import aiosqlite
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
    WebAppInfo,
)
from telegram.error import NetworkError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ==========================================
# 1. CONFIGURATION
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("antigravity-gateway")

BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "")
MY_CHAT_ID: int | None = (
    int(os.environ["TELEGRAM_CHAT_ID"])
    if os.environ.get("TELEGRAM_CHAT_ID")
    else 1051997978
)
WEBHOOK_URL: str | None = os.environ.get("WEBHOOK_URL")
DB_PATH: str = os.environ.get("DB_PATH", "./antigravity_telemetry.db")
PORT: int = int(os.environ.get("PORT", 8080))

# Derived base URL for the Mini App button
BASE_URL: str = WEBHOOK_URL.rstrip("/") if WEBHOOK_URL else f"http://localhost:{PORT}"

# ==========================================
# 2. DATABASE LAYER
# ==========================================


async def get_db() -> aiosqlite.Connection:
    """Open a fresh async SQLite connection (caller must close)."""
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    return db


async def init_db() -> None:
    """Create tables if they don't exist — called once on startup."""
    logger.info("Initializing SQLite database at %s …", DB_PATH)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS token_logs (
                task_id          TEXT,
                input_tokens     INTEGER,
                output_tokens    INTEGER,
                estimated_cost   REAL,
                timestamp        DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS agent_state (
                key       TEXT PRIMARY KEY,
                value     TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()
    logger.info("Database tables verified/created successfully.")


# ==========================================
# 3. HUMAN-IN-THE-LOOP (HITL) MANAGER
# ==========================================


class HITLManager:
    """
    Pythonic translation of the JS onBeforeAgentAction Promise interceptor.

    An asyncio.Future is created when an ERROR context arrives.
    It suspends the caller until a Telegram user resolves it via:
      - approve()   → {"status": "APPROVED"}
      - abort()     → {"status": "ABORTED"}
      - override()  → {"status": "OVERRIDE", "text": "..."}

    Only one pending action is allowed at a time; a new one supersedes
    the previous (same semantics as the Node.js version).
    """

    def __init__(self) -> None:
        self._future: asyncio.Future[dict] | None = None
        self._context: dict | None = None
        self._message_id: int | None = None
        self._lock = asyncio.Lock()

    @property
    def is_pending(self) -> bool:
        return self._future is not None and not self._future.done()

    @property
    def pending_context(self) -> dict | None:
        return self._context

    @property
    def message_id(self) -> int | None:
        return self._message_id

    async def wait_for_decision(self, context: dict) -> dict:
        """
        Suspend until a Telegram operator resolves this future.
        If context.status != 'ERROR', returns PROCEED immediately.
        """
        if context.get("status") != "ERROR":
            return {"status": "PROCEED", "context": context}

        if not MY_CHAT_ID:
            logger.error(
                "MY_CHAT_ID not configured — auto-approving to prevent deadlock."
            )
            return {"status": "APPROVED", "warning": "MY_CHAT_ID_NOT_CONFIGURED"}

        async with self._lock:
            # Supersede any existing pending action
            if self.is_pending and self._future is not None:
                logger.warning("Superseding previous pending agent action.")
                self._future.set_result({"status": "SUPERSEDED"})

            loop = asyncio.get_event_loop()
            self._future = loop.create_future()
            self._context = context
            self._message_id = None

        result = await self._future
        return result

    def set_message_id(self, message_id: int) -> None:
        self._message_id = message_id

    def _resolve(self, result: dict) -> None:
        """Settle the future and clean up state."""
        if self._future and not self._future.done():
            self._future.set_result(result)
        self._future = None
        self._context = None
        self._message_id = None

    def approve(self) -> bool:
        if not self.is_pending:
            return False
        self._resolve({"status": "APPROVED"})
        return True

    def abort(self) -> bool:
        if not self.is_pending:
            return False
        self._resolve({"status": "ABORTED"})
        return True

    def override(self, text: str) -> bool:
        if not self.is_pending:
            return False
        self._resolve({"status": "OVERRIDE", "text": text})
        return True


# Singleton HITL manager
hitl = HITLManager()

# ==========================================
# 4. WEBSOCKET EVENT BUS
# ==========================================


class ConnectionManager:
    """Manages active WebSocket connections and broadcasts events."""

    def __init__(self) -> None:
        self.active: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.active.add(ws)
        logger.info("WebSocket client connected. Total: %d", len(self.active))

    def disconnect(self, ws: WebSocket) -> None:
        self.active.discard(ws)
        logger.info("WebSocket client disconnected. Total: %d", len(self.active))

    async def broadcast(self, payload: dict) -> None:
        """Fan-out a JSON payload to all connected clients."""
        disconnected: list[WebSocket] = []
        for ws in self.active:
            try:
                await ws.send_json(payload)
            except Exception:
                disconnected.append(ws)
        for ws in disconnected:
            self.active.discard(ws)


ws_manager = ConnectionManager()

# ==========================================
# 5. TELEGRAM BOT HANDLERS
# ==========================================


async def _auth_guard(update: Update) -> bool:
    """Return False (and reply) if the sender is not the configured admin."""
    chat_id = update.effective_chat.id if update.effective_chat else None
    if MY_CHAT_ID and chat_id != MY_CHAT_ID:
        # Allow /status regardless
        if update.message and update.message.text == "/status":
            return True
        if update.message:
            await update.message.reply_text(
                "⚠️ Access Denied: This gateway is locked to a specific administrator chat ID."
            )
        return False
    return True


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _auth_guard(update):
        return
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT COUNT(*) as count FROM token_logs"
            ) as cur:
                row = await cur.fetchone()
                log_count = row["count"] if row else 0

            async with db.execute(
                "SELECT SUM(estimated_cost) as cost FROM token_logs"
            ) as cur:
                row = await cur.fetchone()
                total_cost = row["cost"] or 0.0

            async with db.execute(
                "SELECT * FROM agent_state ORDER BY timestamp DESC LIMIT 5"
            ) as cur:
                states = await cur.fetchall()

        db_status = (
            f"Logs count: {log_count}. "
            f"Total estimated cost: ${float(total_cost):.6f}."
        )
        state_lines = (
            "\n".join(
                f"- *{s['key']}*: {s['value']} ({s['timestamp']})"
                for s in states
            )
            if states
            else "No agent states saved yet."
        )
        pending_status = (
            "⚠️ Pending User Approval / Override"
            if hitl.is_pending
            else "✅ Idle"
        )

        text = (
            "🤖 *Antigravity Gateway Status*\n\n"
            f"• *Your Chat ID:* `{update.effective_chat.id}`\n"
            f"• *Configured MY\\_CHAT\\_ID:* `{MY_CHAT_ID or 'Not set'}`\n"
            f"• *Engine State:* {pending_status}\n\n"
            f"• *Telemetry Summary:* {db_status}\n\n"
            f"• *Latest Agent States:*\n{state_lines}"
        )
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception as err:
        logger.exception("Error in /status handler")
        await update.message.reply_text(f"❌ Error fetching status: {err}")


async def abort_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _auth_guard(update):
        return
    logger.info("[Bot] /abort received from chat %s", update.effective_chat.id)
    msg_id = hitl.message_id
    resolved = hitl.abort()
    if resolved:
        if msg_id and MY_CHAT_ID:
            try:
                await context.bot.edit_message_text(
                    chat_id=MY_CHAT_ID,
                    message_id=msg_id,
                    text="🛑 *Aborted by administrator*",
                    parse_mode="Markdown",
                )
            except Exception:
                pass
        await update.message.reply_text(
            "🛑 *Execution Aborted.* Resuming agent loop with aborted status.",
            parse_mode="Markdown",
        )
        await ws_manager.broadcast(
            {"type": "hitl_resolved", "status": "ABORTED"}
        )
    else:
        await update.message.reply_text("❌ No active pending action to abort.")


async def dashboard_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Task 4: Reply with an InlineKeyboard containing a WebAppInfo button
    that opens the Telegram Mini App hosted at BASE_URL/static/index.html.
    """
    if not await _auth_guard(update):
        return
    mini_app_url = f"{BASE_URL}/static/index.html"
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🖥️ Open Workspace", web_app=WebAppInfo(url=mini_app_url))]]
    )
    await update.message.reply_text(
        "Open your live Antigravity workspace:",
        reply_markup=keyboard,
    )


async def approve_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    logger.info("[Bot] approve_continue callback from chat %s", update.effective_chat.id)
    query = update.callback_query
    resolved = hitl.approve()
    if resolved:
        await query.answer("🚀 Action approved. Continuing…")
        try:
            await query.edit_message_text("✅ *Approved & Continued*", parse_mode="Markdown")
        except Exception:
            pass
        await ws_manager.broadcast(
            {"type": "hitl_resolved", "status": "APPROVED"}
        )
    else:
        await query.answer("No active pending action is waiting for approval.")


async def text_message_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not await _auth_guard(update):
        return
    text = update.message.text or ""
    logger.info(
        "[Bot] text message from chat %s: %r", update.effective_chat.id, text
    )
    if hitl.is_pending:
        msg_id = hitl.message_id
        resolved = hitl.override(text)
        if resolved:
            await update.message.reply_text(
                f'✍️ *Prompt Override Received:* "{text}"\nResuming agent execution loop…',
                parse_mode="Markdown",
            )
            if msg_id and MY_CHAT_ID:
                try:
                    await context.bot.edit_message_text(
                        chat_id=MY_CHAT_ID,
                        message_id=msg_id,
                        text=f'✍️ *Overridden with:* "{text}"',
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass
            await ws_manager.broadcast(
                {"type": "hitl_resolved", "status": "OVERRIDE", "text": text}
            )
    else:
        if not text.startswith("/"):
            await update.message.reply_text(
                "No active agent interrupts are pending. "
                "Send /status to query the gateway state."
            )


# ==========================================
# 6. TELEGRAM APPLICATION FACTORY
# ==========================================


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Global PTB error handler.

    Catches transient network errors (ConnectError, TimedOut) that occur
    when the bot tries to call api.telegram.org from inside a Cloudflare
    tunnel and logs them without crashing the application.  All other
    exceptions are re-raised so they surface in the logs for triage.
    """
    err = context.error
    if isinstance(err, (NetworkError, TimedOut)):
        # Transient Telegram API connectivity issue — safe to ignore
        logger.warning(
            "Transient Telegram network error (will not retry): %s: %s",
            type(err).__name__,
            err,
        )
        return
    # For anything else, log the full traceback
    logger.exception("Unhandled exception in PTB handler", exc_info=err)


def build_telegram_app() -> Application:
    """Build and configure the python-telegram-bot Application."""
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("abort", abort_command))
    app.add_handler(CommandHandler("dashboard", dashboard_command))
    app.add_handler(CallbackQueryHandler(approve_callback, pattern="^approve_continue$"))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_handler)
    )
    # Register the global error handler so NetworkErrors are caught gracefully
    app.add_error_handler(error_handler)
    return app


# ==========================================
# 7. FASTAPI LIFESPAN (startup / shutdown)
# ==========================================

telegram_app: Application | None = None


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    """
    Startup:  init DB, build bot, register webhook (or start polling).
    Shutdown: stop polling if running.
    """
    global telegram_app

    await init_db()

    if not BOT_TOKEN:
        logger.warning(
            "TELEGRAM_BOT_TOKEN is not set. Bot functionality will be disabled."
        )
        yield
        return

    telegram_app = build_telegram_app()
    await telegram_app.initialize()

    using_webhook = False
    if WEBHOOK_URL:
        webhook_full = f"{WEBHOOK_URL.rstrip('/')}/telegram-webhook"
        try:
            await telegram_app.bot.set_webhook(webhook_full)
            logger.info("🔗 Webhook registered: %s", webhook_full)
            using_webhook = True
        except Exception as err:
            logger.error("❌ Failed to set webhook: %s. Falling back to long-polling mode…", err)

    if not using_webhook:
        logger.info("Starting bot in long-polling mode…")
        await telegram_app.updater.start_polling()
        await telegram_app.start()
        logger.info("✅ Bot is running in polling mode.")

    yield  # ← server is live here

    # Graceful shutdown
    if using_webhook:
        try:
            await telegram_app.bot.delete_webhook()
        except Exception:
            pass
    else:
        try:
            await telegram_app.updater.stop()
            await telegram_app.stop()
        except Exception:
            pass
    await telegram_app.shutdown()
    logger.info("Bot shut down cleanly.")


# ==========================================
# 8. FASTAPI APP & ROUTES
# ==========================================

app = FastAPI(
    title="Antigravity Telegram Gateway",
    description="Python FastAPI ChatOps relay with HITL and WebSocket event bus",
    version="2.0.0",
    lifespan=lifespan,
)


# ==========================================
# 9. LOCAL EXECUTION ENGINE
# ==========================================


async def _log(message: str, level: str = "info") -> None:
    """Broadcast a structured log line to all connected Mini App clients."""
    await ws_manager.broadcast({"type": "log", "message": message, "level": level})


async def _task_generate_readme() -> None:
    """Generate a comprehensive README.md for the project."""
    await _log("[agent] 📝 Generating README.md …", "system")
    await asyncio.sleep(0.5)

    readme = """\
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
"""

    # Write file without blocking the event loop
    await asyncio.to_thread(
        lambda: open("README.md", "w", encoding="utf-8").write(readme)
    )
    await _log("[agent] ✅ README.md written to project root.", "success")


async def _task_list_files() -> None:
    """List files in the project directory."""
    await _log("[agent] 📂 Listing project files …", "system")
    process = await asyncio.create_subprocess_shell(
        "find . -not -path './.git/*' -not -path './.venv/*' "
        "-not -path './_archive/*' -not -name '*.pyc' | sort",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await process.communicate()
    for line in stdout.decode().splitlines():
        await _log(line, "info")
    await _log("[agent] ✅ Done.", "success")


async def _task_shell(cmd: str) -> bool:
    """
    Stream a shell command's stdout/stderr line-by-line back to the Mini App.
    ⚠️  Security note: only expose this to authenticated operators (MY_CHAT_ID guard).
    """
    await _log(f"[shell] $ {cmd}", "system")
    process = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    # Stream stdout line by line without buffering the whole output
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        await _log(line.decode().rstrip(), "info")

    await process.wait()
    rc = process.returncode

    if rc != 0:
        stderr_data = await process.stderr.read()
        for line in stderr_data.decode().splitlines():
            if line.strip():
                await _log(f"[stderr] {line}", "warn")
        await _log(f"[shell] ❌ Exited with code {rc}", "warn")
        return False
    else:
        await _log(f"[shell] ✅ Exited cleanly (code 0)", "success")
        return True


async def run_agent_task(prompt: str) -> None:
    """
    Execution engine entry point.

    Executes the Antigravity agent CLI:
      agy run {safe_prompt}

    Results are broadcast to all connected Mini App clients via ws_manager.
    Logs prompt status ("SUCCESS" / "FAILED") to agent_state table.
    """
    await _log(f"[agent] 🧠 Analyzing prompt: {prompt!r}", "system")
    
    # 2. THE FIX: Safely escape the natural language prompt
    safe_prompt = shlex.quote(prompt)
    cli_command = f"./agy chat {safe_prompt}"

    status = "FAILED"
    try:
        # Execute the command (stream stdout/stderr back)
        success = await _task_shell(cli_command)
        if success:
            status = "SUCCESS"
    except Exception as exc:
        logger.exception("Execution engine error")
        await _log(f"[agent] ❌ Task failed: {exc}", "error")
    finally:
        # Add SQLite database insertion query logging prompt and status
        try:
            db = await get_db()
            try:
                await db.execute(
                    """
                    INSERT INTO agent_state (key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE
                        SET value = excluded.value, timestamp = CURRENT_TIMESTAMP
                    """,
                    (prompt, status),
                )
                await db.commit()
            finally:
                await db.close()
            logger.info("Agent execution logged to database: %s -> %s", prompt, status)
        except Exception as db_err:
            logger.exception("Error saving agent run state to telemetry database: %s", db_err)


# --- WebSocket endpoint ---

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Bidirectional event bus:
      - Broadcasts agent log events pushed via ws_manager.broadcast()
      - Receives JSON commands: { "action": "approve"|"abort"|"override", "text": "..." }
    """
    await ws_manager.connect(websocket)
    try:
        while True:
            data: dict[str, Any] = await websocket.receive_json()
            action = data.get("action", "")
            # Debug: log every incoming WS frame so we can diagnose dispatch issues
            logger.debug("[WS] received action=%r data=%s", action, data)

            if action == "approve":
                resolved = hitl.approve()
                await websocket.send_json(
                    {"type": "ack", "action": "approve", "resolved": resolved}
                )
                if resolved:
                    await ws_manager.broadcast(
                        {"type": "hitl_resolved", "status": "APPROVED"}
                    )

            elif action == "abort":
                resolved = hitl.abort()
                await websocket.send_json(
                    {"type": "ack", "action": "abort", "resolved": resolved}
                )
                if resolved:
                    await ws_manager.broadcast(
                        {"type": "hitl_resolved", "status": "ABORTED"}
                    )

            elif action == "override":
                override_text = data.get("text", "")
                resolved = hitl.override(override_text)
                await websocket.send_json(
                    {"type": "ack", "action": "override", "resolved": resolved}
                )
                if resolved:
                    await ws_manager.broadcast(
                        {"type": "hitl_resolved", "status": "OVERRIDE", "text": override_text}
                    )

            elif action == "log":
                # Fan-out this log line to all clients.
                message = data.get("message", "")
                level   = data.get("level", "info")
                await ws_manager.broadcast({"type": "log", "message": message, "level": level})

                # —————————————————————————————————————————————
                # BACKWARD-COMPAT EXECUTION HOOK
                # Old / cached versions of the Mini App send
                #   { "action": "log", "message": "[user] <prompt>" }
                # instead of the newer { "action": "execute", "prompt": ... }.
                # We intercept that pattern here so the engine fires regardless
                # of which HTML version the client has cached.
                # —————————————————————————————————————————————
                if message.startswith("[user] "):
                    user_prompt = message[len("[user] "):].strip()
                    if user_prompt:
                        logger.info("[WS] log-compat path → executing prompt: %r", user_prompt)
                        asyncio.create_task(run_agent_task(user_prompt))

            elif action == "execute":
                # New-style dispatch from updated Mini App HTML.
                prompt = data.get("prompt", "").strip()
                if prompt:
                    await ws_manager.broadcast(
                        {"type": "log", "message": f"[user] {prompt}", "level": "info"}
                    )
                    logger.info("[WS] execute action → spawning task for prompt: %r", prompt)
                    asyncio.create_task(run_agent_task(prompt))
                    await websocket.send_json({"type": "ack", "action": "execute", "queued": True})
                else:
                    await websocket.send_json({"type": "error", "message": "Empty prompt."})

            else:
                await websocket.send_json(
                    {"type": "error", "message": f"Unknown action: {action}"}
                )

    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as exc:
        logger.exception("WebSocket error: %s", exc)
        ws_manager.disconnect(websocket)


# --- Telegram webhook receiver ---

@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    """
    Receives Telegram updates via webhook and feeds them to python-telegram-bot.

    Always returns 200 OK so Telegram does not retry the same update.  Any
    handler-level errors (e.g. transient NetworkError talking back to the API)
    are caught by the PTB error_handler registered in build_telegram_app().
    """
    if telegram_app is None:
        raise HTTPException(status_code=503, detail="Bot not initialised")
    body = await request.json()
    try:
        update = Update.de_json(body, telegram_app.bot)
        await telegram_app.process_update(update)
    except Exception as exc:
        # Log but still return 200 — returning non-200 causes Telegram to
        # keep retrying the same update which can create a retry storm.
        logger.exception("Error processing Telegram update: %s", exc)
    return JSONResponse({"ok": True})


# --- HITL intercept hook ---

async def _send_intercept_message(context: dict) -> None:
    """
    Send the Telegram intercept card with the Approve button.
    Called right after creating the HITL future so the message_id
    can be stored for later editing.
    """
    if not telegram_app or not MY_CHAT_ID:
        return
    error_details = context.get("error", "Unknown error occurred")
    task_id = context.get("taskId", "unspecified-task")
    message_text = (
        "⚠️ *Agent Action Intercept*\n\n"
        f"• *Task ID:* `{task_id}`\n"
        f"• *Status:* `ERROR`\n"
        f"• *Error Details:* `{error_details}`\n\n"
        "Please choose an action below or reply with a message to *override* the prompt."
    )
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🚀 Approve & Continue", callback_data="approve_continue")]]
    )
    try:
        msg = await telegram_app.bot.send_message(
            chat_id=MY_CHAT_ID,
            text=message_text,
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
        hitl.set_message_id(msg.message_id)
        # Also broadcast pending event to Mini App
        await ws_manager.broadcast(
            {
                "type": "hitl_pending",
                "taskId": task_id,
                "error": error_details,
                "message_id": msg.message_id,
            }
        )
    except Exception as err:
        logger.error("Error sending intercept message to Telegram: %s", err)


@app.post("/api/agent/before-action")
async def before_action(request: Request):
    """
    Python equivalent of the JS onBeforeAgentAction() export.

    If context.status == 'ERROR':
      1. Creates an asyncio.Future (HITL suspension)
      2. Sends the Telegram intercept card with Approve button
      3. Awaits operator decision (approve / abort / override)
      4. Broadcasts resolution to all WebSocket clients

    POST body: { "status": "ERROR"|"OK", "taskId": "...", "error": "..." }
    """
    context: dict = await request.json()
    logger.info("API: before-action hook triggered with context=%s", context)

    if context.get("status") != "ERROR":
        return JSONResponse({"success": True, "result": {"status": "PROCEED", "context": context}})

    if not MY_CHAT_ID:
        logger.error("MY_CHAT_ID not configured — auto-approving.")
        return JSONResponse(
            {"success": True, "result": {"status": "APPROVED", "warning": "MY_CHAT_ID_NOT_CONFIGURED"}}
        )

    # Start awaiting the future (creates it inside wait_for_decision)
    future_task = asyncio.create_task(hitl.wait_for_decision(context))

    # Fire the Telegram intercept card now (stores message_id on hitl)
    await _send_intercept_message(context)

    # Block until the operator resolves the future
    result = await future_task
    await ws_manager.broadcast({"type": "hitl_resolved", **result})
    return JSONResponse({"success": True, "result": result})


# --- Telemetry: token log ---

@app.post("/api/telemetry/token-log")
async def token_log(request: Request):
    body = await request.json()
    task_id = body.get("task_id")
    input_tokens = body.get("input_tokens")
    output_tokens = body.get("output_tokens")
    estimated_cost = body.get("estimated_cost")

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO token_logs (task_id, input_tokens, output_tokens, estimated_cost)
                VALUES (?, ?, ?, ?)
                """,
                (task_id, input_tokens, output_tokens, estimated_cost),
            )
            await db.commit()
        await ws_manager.broadcast(
            {
                "type": "telemetry",
                "task_id": task_id,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "estimated_cost": estimated_cost,
            }
        )
        return JSONResponse(
            {"success": True, "message": "Telemetry token log registered successfully."}
        )
    except Exception as err:
        logger.exception("Error writing token log")
        raise HTTPException(status_code=500, detail=str(err))


# --- Telemetry: agent state ---

@app.post("/api/agent/state")
async def agent_state(request: Request):
    body = await request.json()
    key = body.get("key")
    value = body.get("value")

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO agent_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE
                    SET value = excluded.value, timestamp = CURRENT_TIMESTAMP
                """,
                (key, value),
            )
            await db.commit()
        await ws_manager.broadcast(
            {"type": "state_update", "key": key, "value": value}
        )
        return JSONResponse(
            {"success": True, "message": "Agent state registered/updated successfully."}
        )
    except Exception as err:
        logger.exception("Error writing agent state")
        raise HTTPException(status_code=500, detail=str(err))


# --- WebSocket broadcast helper (external agent use) ---

@app.post("/api/ws/broadcast")
async def ws_broadcast(request: Request):
    """
    Convenience endpoint for external processes to push log lines
    to all connected Mini App clients without opening a WebSocket.

    POST body: { "type": "log", "message": "...", "level": "info|warn|error" }
    """
    payload = await request.json()
    await ws_manager.broadcast(payload)
    return JSONResponse({"success": True, "clients": len(ws_manager.active)})


# --- Favicon (suppresses browser 404 noise) ---

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    from fastapi.responses import Response
    return Response(status_code=204)


# --- Health check ---

@app.get("/")
async def root():
    return {
        "name": "antigravity-telegram-gateway",
        "version": "2.0.0",
        "status": "online",
        "hitl_pending": hitl.is_pending,
        "ws_clients": len(ws_manager.active),
    }


# --- Static files (Telegram Mini App) ---
# Mount AFTER all API routes to avoid route conflicts.
# We add a custom route for index.html so Telegram's WebView always fetches
# a fresh copy instead of serving a stale cached version.

from fastapi.responses import FileResponse


@app.get("/static/index.html", include_in_schema=False)
async def mini_app_index():
    return FileResponse(
        "static/index.html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
        },
    )


app.mount("/static", StaticFiles(directory="static"), name="static")
