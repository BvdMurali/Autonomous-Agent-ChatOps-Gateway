import express from 'express';
import { Telegraf } from 'telegraf';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

// ==========================================
// 1. CONFIGURATION
// ==========================================

// Telegram Bot Configuration
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const MY_CHAT_ID = process.env.TELEGRAM_CHAT_ID ? parseInt(process.env.TELEGRAM_CHAT_ID, 10) : 1051997978;

// ==========================================
// 2. DATABASE INITIALIZATION
// ==========================================

console.log('Initializing SQLite database...');
const db = await open({
  filename: './antigravity_telemetry.db',
  driver: sqlite3.Database
});

await db.exec(`
  CREATE TABLE IF NOT EXISTS token_logs (
    task_id TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    estimated_cost REAL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  
  CREATE TABLE IF NOT EXISTS agent_state (
    key TEXT PRIMARY KEY,
    value TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  );
`);
console.log('Database tables verified/created successfully.');

// ==========================================
// 3. TELEGRAF BOT SETUP
// ==========================================

const bot = new Telegraf(BOT_TOKEN);

// Track the currently active unsettled promise
let pendingAction = null;

// Security/Authorization Middleware:
// If MY_CHAT_ID is set, block any message or callback query not originating from that ID.
// Always allow /status so the user can easily discover their chat ID.
bot.use((ctx, next) => {
  const isStatusCmd = ctx.message?.text === '/status';
  if (MY_CHAT_ID && ctx.chat && ctx.chat.id !== MY_CHAT_ID) {
    if (isStatusCmd) return next();
    return ctx.reply('⚠️ Access Denied: This gateway is locked to a specific administrator chat ID.');
  }
  return next();
});

// Command: /status
bot.command('status', async (ctx) => {
  try {
    const logCount = await db.get('SELECT COUNT(*) as count FROM token_logs');
    const totalCost = await db.get('SELECT SUM(estimated_cost) as cost FROM token_logs');
    const latestState = await db.all('SELECT * FROM agent_state ORDER BY timestamp DESC LIMIT 5');

    const dbStatus = `Logs count: ${logCount?.count || 0}. Total estimated cost: $${(totalCost?.cost || 0).toFixed(6)}.`;
    const stateText = latestState.length > 0
      ? latestState.map(s => `- *${s.key}*: ${s.value} (${s.timestamp})`).join('\n')
      : 'No agent states saved yet.';

    const pendingStatus = pendingAction ? '⚠️ Pending User Approval / Override' : '✅ Idle';

    const response = `🤖 *Antigravity Gateway Status*\n\n` +
                     `• *Your Chat ID:* \`${ctx.chat.id}\`\n` +
                     `• *Configured MY_CHAT_ID:* \`${MY_CHAT_ID || 'Not set (please edit server.js)'}\`\n` +
                     `• *Engine State:* ${pendingStatus}\n\n` +
                     `• *Telemetry Summary:* ${dbStatus}\n\n` +
                     `• *Latest Agent States:*\n${stateText}`;

    await ctx.reply(response, { parse_mode: 'Markdown' });
  } catch (err) {
    console.error('Error fetching status:', err);
    await ctx.reply(`❌ Error fetching status: ${err.message}`);
  }
});

// Command: /abort
bot.command('abort', async (ctx) => {
  console.log(`[Bot] /abort command received from chat ${ctx.chat.id}`);
  if (pendingAction) {
    const resolveFn = pendingAction.resolve;
    const msgId = pendingAction.messageId;
    
    // Clear pending state
    pendingAction = null;

    // Update the message card if possible
    if (msgId && MY_CHAT_ID) {
      try {
        await bot.telegram.editMessageText(
          MY_CHAT_ID,
          msgId,
          null,
          `🛑 *Aborted by administrator*`
        );
      } catch (e) {
        console.error(e);
      }
    }
    
    await ctx.reply('🛑 *Execution Aborted.* Resuming agent loop with aborted status.', { parse_mode: 'Markdown' });
    resolveFn({ status: 'ABORTED' });
  } else {
    await ctx.reply('❌ No active pending action to abort.');
  }
});

// Inline button handler for "🚀 Approve & Continue"
bot.action('approve_continue', async (ctx) => {
  console.log(`[Bot] approve_continue action received from chat ${ctx.chat?.id}`);
  if (pendingAction) {
    await ctx.answerCbQuery('🚀 Action approved. Continuing...');
    const resolveFn = pendingAction.resolve;
    const msgId = pendingAction.messageId;
    
    // Clear pending state
    pendingAction = null;

    // Update the telegram message card inline
    try {
      await ctx.editMessageText('✅ *Approved & Continued*', { parse_mode: 'Markdown' });
    } catch (e) {
      console.error('Failed to update telegram message text:', e);
    }

    // Settle promise with approved status
    resolveFn({ status: 'APPROVED' });
  } else {
    await ctx.answerCbQuery('No active pending action is waiting for approval.');
  }
});

// General Text Handler (detects prompt overrides when promise is pending)
bot.on('text', async (ctx) => {
  console.log(`[Bot] text message received from chat ${ctx.chat.id}: "${ctx.message.text}"`);
  if (pendingAction) {
    const overrideText = ctx.message.text;
    const resolveFn = pendingAction.resolve;
    const msgId = pendingAction.messageId;

    // Clear pending state
    pendingAction = null;

    await ctx.reply(`✍️ *Prompt Override Received:* "${overrideText}"\nResuming agent execution loop...`, { parse_mode: 'Markdown' });

    // Update the original intercept message card to note the override
    if (msgId && MY_CHAT_ID) {
      try {
        await bot.telegram.editMessageText(
          MY_CHAT_ID,
          msgId,
          null,
          `✍️ *Overridden with:* "${overrideText}"`
        );
      } catch (e) {
        console.error('Failed to update telegram message text:', e);
      }
    }

    // Settle promise with the new text context override
    resolveFn({ status: 'OVERRIDE', text: overrideText });
  } else {
    // Basic catch-all when no pending state is active
    if (ctx.message.text.startsWith('/')) return;
    await ctx.reply('No active agent interrupts are pending. Send /status to query the gateway state.');
  }
});

// ==========================================
// 4. INTERCEPT HOOK FUNCTION
// ==========================================

/**
 * Core framework intercept hook wrapped inside an unsettled JavaScript Promise.
 * If status is 'ERROR', wait for user approval or prompt override.
 * 
 * @param {object} context - Agent action/execution context
 * @returns {Promise<object>} Settles when user approves or overrides
 */
export function onBeforeAgentAction(context) {
  return new Promise((resolve, reject) => {
    // If context status is not ERROR, proceed immediately
    if (context.status !== 'ERROR') {
      return resolve({ status: 'PROCEED', context });
    }

    if (!MY_CHAT_ID) {
      console.error('MY_CHAT_ID is not configured. Automatically resolving as approved to prevent deadlocks.');
      return resolve({ status: 'APPROVED', warning: 'MY_CHAT_ID_NOT_CONFIGURED' });
    }

    // Cancel / supersede any existing pending action to avoid memory leaks or conflicting prompts
    if (pendingAction) {
      console.warn('Superseding previous pending agent action.');
      pendingAction.resolve({ status: 'SUPERSEDED' });
    }

    pendingAction = { resolve, reject, context };

    const errorDetails = context.error || 'Unknown error occurred';
    const taskId = context.taskId || 'unspecified-task';
    const messageText = `⚠️ *Agent Action Intercept*\n\n` +
                        `• *Task ID:* \`${taskId}\`\n` +
                        `• *Status:* \`ERROR\`\n` +
                        `• *Error Details:* \`${errorDetails}\`\n\n` +
                        `Please choose an action below or reply with a message to *override* the prompt.`;

    bot.telegram.sendMessage(MY_CHAT_ID, messageText, {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [
            { text: '🚀 Approve & Continue', callback_data: 'approve_continue' }
          ]
        ]
      }
    }).then((msg) => {
      if (pendingAction) {
        pendingAction.messageId = msg.message_id;
      }
    }).catch((err) => {
      console.error('Error sending intercept message to Telegram:', err);
    });
  });
}

// ==========================================
// 5. EXPRESS HTTP APP SETUP
// ==========================================

const app = express();
app.use(express.json());

// Express webhook endpoint
app.post('/telegram-webhook', (req, res, next) => {
  // Pass to Telegraf webhook callback handler
  next();
}, bot.webhookCallback('/telegram-webhook'));

// Helper HTTP endpoint to simulate / test the intercept hook
app.post('/api/agent/before-action', async (req, res) => {
  try {
    const context = req.body || {};
    console.log('API call: before-action hook triggered', context);
    const result = await onBeforeAgentAction(context);
    res.json({ success: true, result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Telemetry API: Log tokens
app.post('/api/telemetry/token-log', async (req, res) => {
  const { task_id, input_tokens, output_tokens, estimated_cost } = req.body;
  try {
    await db.run(
      `INSERT INTO token_logs (task_id, input_tokens, output_tokens, estimated_cost)
       VALUES (?, ?, ?, ?)`,
      [task_id, input_tokens, output_tokens, estimated_cost]
    );
    res.json({ success: true, message: 'Telemetry token log registered successfully.' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Telemetry API: Agent State
app.post('/api/agent/state', async (req, res) => {
  const { key, value } = req.body;
  try {
    await db.run(
      `INSERT INTO agent_state (key, value)
       VALUES (?, ?)
       ON CONFLICT(key) DO UPDATE SET value=excluded.value, timestamp=CURRENT_TIMESTAMP`,
      [key, value]
    );
    res.json({ success: true, message: 'Agent state registered/updated successfully.' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Generic base endpoint
app.get('/', (req, res) => {
  res.json({ name: 'antigravity-telegram-gateway', status: 'online' });
});

// ==========================================
// 6. PORT & SERVER BOOT
// ==========================================

const PORT = 8080;
const WEBHOOK_URL = process.env.WEBHOOK_URL;

app.listen(PORT, async () => {
  console.log(`\n🚀 ChatOps relay server is live on http://localhost:${PORT}`);
  
  if (WEBHOOK_URL) {
    try {
      const fullWebhookUrl = `${WEBHOOK_URL.replace(/\/$/, '')}/telegram-webhook`;
      await bot.telegram.setWebhook(fullWebhookUrl);
      console.log(`🔗 Webhook URL set successfully: ${fullWebhookUrl}`);
    } catch (err) {
      console.error('❌ Failed to register webhook via bot.telegram.setWebhook:', err);
    }
  } else {
    console.log('⚠️ WEBHOOK_URL env variable not defined. Webhook registration skipped.');
    console.log('🤖 Starting bot in polling mode for local testing...');
    try {
      bot.launch();
      console.log('✅ Bot is running in polling mode.');
    } catch (err) {
      console.error('❌ Failed to launch bot in polling mode:', err);
    }
  }
});

// Enable graceful stop
process.once('SIGINT', () => {
  try {
    bot.stop('SIGINT');
  } catch (err) {
    // Ignore error if bot was not running in polling mode
  }
  process.exit(0);
});
process.once('SIGTERM', () => {
  try {
    bot.stop('SIGTERM');
  } catch (err) {
    // Ignore error if bot was not running in polling mode
  }
  process.exit(0);
});
