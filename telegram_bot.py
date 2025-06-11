import logging
import sqlite3
import time
import asyncio
from datetime import datetime, timedelta
from contextlib import contextmanager
from typing import Optional, List, Tuple

from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.error import TelegramError, TimedOut, RetryAfter
from telegram.request import HTTPXRequest
import aiosqlite

# --- Configuration ---
class Config:
    TOKEN = "7961276861:AAGQWvI1uu-oiphmfhnmWcObpzMp9BcEEBk"  # Replace with your bot token
    DB_PATH = "activity.db"
    LOG_LEVEL = "DEBUG"
    CHECK_INTERVAL = 30
    BATCH_SIZE = 100
    API_DELAY = 0.2
    BATCH_DELAY = 5
    MAX_RETRIES = 3
    MAX_MENTIONS_PER_MESSAGE = 20
    MAX_INACTIVE_PER_CHECK = 1000
    BOT_NAME = "ActivityGuard"  # Bot branding name
    SUPPORT_CONTACT = "@YourSupportHandle"  # Replace with your support contact

# --- Logging Setup ---
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# --- Database Manager ---
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        try:
            self._init_db()
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    def _init_db(self):
        with self._get_connection() as conn:
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS activity (
                        user_id INTEGER,
                        group_id INTEGER,
                        last_active INTEGER,
                        PRIMARY KEY (user_id, group_id)
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS settings (
                        group_id INTEGER PRIMARY KEY,
                        autokick INTEGER DEFAULT 0
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS check_interval (
                        group_id INTEGER PRIMARY KEY,
                        interval_seconds INTEGER
                    )
                    """
                )
                conn.commit()
                logger.debug("Database initialized successfully")
            except sqlite3.Error as e:
                logger.error(f"Database initialization error: {e}")
                raise

    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        try:
            yield conn
        finally:
            conn.close()

    async def update_activity(self, user_id: int, group_id: int):
        async with aiosqlite.connect(self.db_path) as conn:
            try:
                await conn.execute(
                    "REPLACE INTO activity (user_id, group_id, last_active) VALUES (?, ?, ?)",
                    (user_id, group_id, int(time.time())),
                )
                await conn.commit()
                logger.debug(f"Updated activity for user {user_id} in group {group_id}")
            except aiosqlite.Error as e:
                logger.error(f"Error updating activity: {e}")
                raise

    async def get_inactive_users(self, group_id: int, cutoff: int) -> List[Tuple[int]]:
        async with aiosqlite.connect(self.db_path) as conn:
            try:
                cursor = await conn.execute(
                    "SELECT user_id FROM activity WHERE group_id = ? AND last_active < ? LIMIT ?",
                    (group_id, cutoff, Config.MAX_INACTIVE_PER_CHECK),
                )
                users = await cursor.fetchall()
                logger.debug(f"Found {len(users)} inactive users in group {group_id}")
                return users
            except aiosqlite.Error as e:
                logger.error(f"Error fetching inactive users: {e}")
                raise

    async def get_autokick_status(self, group_id: int) -> bool:
        async with aiosqlite.connect(self.db_path) as conn:
            try:
                cursor = await conn.execute(
                    "SELECT autokick FROM settings WHERE group_id = ?", (group_id,)
                )
                row = await cursor.fetchone()
                status = bool(row[0]) if row else False
                logger.debug(f"Autokick status for group {group_id}: {status}")
                return status
            except aiosqlite.Error as e:
                logger.error(f"Error getting autokick status: {e}")
                raise

    async def set_autokick(self, group_id: int, status: bool):
        async with aiosqlite.connect(self.db_path) as conn:
            try:
                await conn.execute(
                    "REPLACE INTO settings (group_id, autokick) VALUES (?, ?)",
                    (group_id, int(status)),
                )
                await conn.commit()
                logger.debug(f"Set autokick to {status} for group {group_id}")
            except aiosqlite.Error as e:
                logger.error(f"Error setting autokick: {e}")
                raise

    async def set_check_interval(self, group_id: int, interval: int):
        async with aiosqlite.connect(self.db_path) as conn:
            try:
                await conn.execute(
                    "REPLACE INTO check_interval (group_id, interval_seconds) VALUES (?, ?)",
                    (group_id, interval),
                )
                await conn.commit()
                logger.debug(f"Set check interval to {interval}s for group {group_id}")
            except aiosqlite.Error as e:
                logger.error(f"Error setting check interval: {e}")
                raise

    async def get_check_intervals(self) -> List[Tuple[int, int]]:
        async with aiosqlite.connect(self.db_path) as conn:
            try:
                cursor = await conn.execute(
                    "SELECT group_id, interval_seconds FROM check_interval"
                )
                intervals = await cursor.fetchall()
                logger.debug(f"Retrieved {len(intervals)} check intervals")
                return intervals
            except aiosqlite.Error as e:
                logger.error(f"Error fetching check intervals: {e}")
                raise

# --- Utility Functions ---
def parse_interval(text: str) -> Optional[int]:
    if len(text) < 2:
        return None
    try:
        num = int(text[:-1])
        unit = text[-1].lower()
        units = {"m": 60, "h": 3600}
        return num * units[unit] if unit in units else None
    except (ValueError, KeyError):
        return None

async def retry_api_call(func, *args, max_retries=Config.MAX_RETRIES, **kwargs):
    """Retry Telegram API calls with exponential backoff."""
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except RetryAfter as e:
            wait_time = e.retry_after + 1
            logger.warning(f"Rate limit hit, retrying after {wait_time}s (attempt {attempt + 1}/{max_retries})")
            await asyncio.sleep(wait_time)
        except TimedOut:
            wait_time = 2 ** attempt
            logger.warning(f"Timeout, retrying after {wait_time}s (attempt {attempt + 1}/{max_retries})")
            await asyncio.sleep(wait_time)
        except TelegramError as e:
            logger.error(f"API error: {e}")
            if attempt == max_retries - 1:
                raise
    raise TelegramError("Max retries reached")

# --- Telegram Handlers ---
async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        member = await retry_api_call(
            context.bot.get_chat_member,
            update.effective_chat.id,
            update.effective_user.id
        )
        is_admin = member.status in ["administrator", "creator"]
        logger.debug(f"User {update.effective_user.id} admin check: {is_admin}")
        return is_admin
    except TelegramError as e:
        logger.error(f"Failed to check admin status: {e}")
        return False

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.chat.type in ["group", "supergroup"]:
        try:
            user_id = update.message.from_user.id
            group_id = update.message.chat.id
            db = context.bot_data['db']
            await db.update_activity(user_id, group_id)
        except Exception as e:
            logger.error(f"Error in message handler: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_message = (
        f"🌟 **Welcome to {Config.BOT_NAME}!** 🌟\n\n"
        f"I'm your group activity manager, keeping your community lively and engaged! 🚀\n\n"
        f"**What I Can Do:**\n"
        f"✅ Monitor user activity\n"
        f"🔔 Notify inactive users\n"
        f"🛠️ Auto-remove inactive members (optional)\n\n"
        f"**Get Started:**\n"
        f"👉 Use `/setcheck 5m` to start monitoring (e.g., every 5 minutes).\n"
        f"👉 Enable auto-kick with `/autokick on` (admins only).\n\n"
        f"Need help? Contact {Config.SUPPORT_CONTACT} 📩\n"
        f"--- *{Config.BOT_NAME} - Keeping Groups Active!* ---"
    )
    await update.message.reply_text(welcome_message, parse_mode="Markdown")

async def toggle_autokick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text(
            f"🚫 **Access Denied**\nOnly admins can use this command. Contact {Config.SUPPORT_CONTACT} for assistance.",
            parse_mode="Markdown"
        )
        return
    
    if len(context.args) != 1 or context.args[0].lower() not in ["on", "off"]:
        await update.message.reply_text(
            f"❓ **Usage Instructions**\nUse `/autokick on` or `/autokick off` to toggle auto-kick.\n"
            f"--- *{Config.BOT_NAME}* ---",
            parse_mode="Markdown"
        )
        return
    
    try:
        group_id = update.effective_chat.id
        status = context.args[0].lower() == "on"
        db = context.bot_data['db']
        await db.set_autokick(group_id, status)
        status_emoji = "✅" if status else "❌"
        await update.message.reply_text(
            f"{status_emoji} **Auto-Kick Updated**\n"
            f"Auto-kick is now *{'enabled' if status else 'disabled'}* for this group.\n"
            f"Inactive users will {'be removed' if status else 'not be removed'} automatically.\n"
            f"--- *{Config.BOT_NAME}* ---",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Error in toggle_autokick: {e}")
        await update.message.reply_text(
            f"⚠️ **Oops!**\nSomething went wrong. Please try again or contact {Config.SUPPORT_CONTACT}.\n"
            f"--- *{Config.BOT_NAME}* ---",
            parse_mode="Markdown"
        )

async def set_check_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text(
            f"🚫 **Access Denied**\nOnly admins can use this command. Contact {Config.SUPPORT_CONTACT} for assistance.",
            parse_mode="Markdown"
        )
        return
    
    if not context.args:
        await update.message.reply_text(
            f"❓ **Usage Instructions**\nUse `/setcheck <interval>` (e.g., `5m`, `1h`).\n"
            f"Supported intervals: `5m`, `15m`, `30m`, `1h`, `2h`, `3h`, `6h`, `12h`, `24h`.\n"
            f"--- *{Config.BOT_NAME}* ---",
            parse_mode="Markdown"
        )
        return
    
    interval = parse_interval(context.args[0])
    if interval is None:
        await update.message.reply_text(
            f"⚠️ **Invalid Interval**\nPlease use formats like `5m` or `1h`. "
            f"Supported: `5m`, `15m`, `30m`, `1h`, `2h`, `3h`, `6h`, `12h`, `24h`.\n"
            f"--- *{Config.BOT_NAME}* ---",
            parse_mode="Markdown"
        )
        return
    
    try:
        group_id = update.effective_chat.id
        db = context.bot_data['db']
        await db.set_check_interval(group_id, interval)
        for job in context.job_queue.get_jobs_by_name(f"inactive_check_{group_id}"):
            job.schedule_removal()
        context.job_queue.run_repeating(
            callback=check_inactive,
            interval=interval,
            first=interval,
            data=group_id,
            name=f"inactive_check_{group_id}",
        )
        await update.message.reply_text(
            f"⏰ **Monitoring Activated**\n"
            f"{Config.BOT_NAME} will check for inactive users every *{context.args[0]}*.\n"
            f"Inactive users will be notified, and auto-kick will apply if enabled.\n"
            f"--- *{Config.BOT_NAME}* ---",
            parse_mode="Markdown"
        )
        logger.info(f"Scheduled inactive check for group {group_id} every {interval}s")
    except Exception as e:
        logger.error(f"Error in set_check_interval: {e}")
        await update.message.reply_text(
            f"⚠️ **Oops!**\nSomething went wrong. Please try again or contact {Config.SUPPORT_CONTACT}.\n"
            f"--- *{Config.BOT_NAME}* ---",
            parse_mode="Markdown"
        )

async def check_inactive(context: ContextTypes.DEFAULT_TYPE):
    group_id = context.job.data
    try:
        db = context.bot_data['db']
        async with aiosqlite.connect(Config.DB_PATH) as conn:
            try:
                cursor = await conn.execute(
                    "SELECT interval_seconds FROM check_interval WHERE group_id = ?", (group_id,)
                )
                row = await cursor.fetchone()
                if not row:
                    logger.warning(f"No check interval found for group {group_id}")
                    return
                interval = row[0]
            except aiosqlite.Error as e:
                logger.error(f"Error fetching interval for group {group_id}: {e}")
                return
        
        cutoff = int(time.time()) - interval
        inactive_users = await db.get_inactive_users(group_id, cutoff)
        
        if not inactive_users:
            logger.debug(f"No inactive users found in group {group_id}")
            return
        
        logger.debug(f"Processing {len(inactive_users)} inactive users in group {group_id}")
        
        mentions = []
        autokick_users = []
        for i in range(0, len(inactive_users), Config.BATCH_SIZE):
            batch = inactive_users[i:i + Config.BATCH_SIZE]
            logger.debug(f"Processing batch {i // Config.BATCH_SIZE + 1} with {len(batch)} users")
            
            for (uid,) in batch:
                try:
                    member = await retry_api_call(
                        context.bot.get_chat_member,
                        group_id,
                        uid
                    )
                    if member.user.is_bot:
                        continue
                    if member.status in ['administrator', 'creator']:
                        continue
                    username = member.user.username
                    if username:
                        user_mention = f"@{username}"
                        mentions.append(user_mention)
                    else:
                        logger.debug(f"User {uid} has no username, skipping mention")
                    autokick_users.append(uid)
                    await asyncio.sleep(Config.API_DELAY)
                except TelegramError as e:
                    logger.warning(f"Failed to get user {uid} in group {group_id}: {e}")
                    continue
            
            if i + Config.BATCH_SIZE < len(inactive_users):
                await asyncio.sleep(Config.BATCH_DELAY)
        
        if not mentions:
            logger.debug(f"No users with usernames to mention in group {group_id}")
        else:
            for i in range(0, len(mentions), Config.MAX_MENTIONS_PER_MESSAGE):
                chunk = mentions[i:i + Config.MAX_MENTIONS_PER_MESSAGE]
                mention_list = "\n".join(f"{j+1}. {user}" for j, user in enumerate(chunk))
                message = (
                    f"🚨 **Inactive Users Alert** 🚨\n"
                    f"These users have been inactive for *{interval//60} minutes*:\n\n"
                    f"{mention_list}\n\n"
                    f"💬 *Stay active to remain in the group!*\n"
                    f"--- *{Config.BOT_NAME}* ---"
                )
                try:
                    await retry_api_call(
                        context.bot.send_message,
                        chat_id=group_id,
                        text=message,
                        parse_mode="Markdown"
                    )
                    logger.info(f"Sent message with {len(chunk)} mentions to group {group_id}")
                    await asyncio.sleep(Config.API_DELAY)
                except TelegramError as e:
                    logger.error(f"Failed to send message to group {group_id}: {e}")
                    continue
        
        if await db.get_autokick_status(group_id):
            for uid in autokick_users:
                try:
                    member = await retry_api_call(
                        context.bot.get_chat_member,
                        group_id,
                        uid
                    )
                    if member.status in ["administrator", "creator"]:
                        continue
                    await retry_api_call(
                        context.bot.ban_chat_member,
                        group_id,
                        uid
                    )
                    await retry_api_call(
                        context.bot.unban_chat_member,
                        group_id,
                        uid
                    )
                    logger.info(f"Kicked user {uid} from group {group_id}")
                    await asyncio.sleep(Config.API_DELAY)
                except TelegramError as e:
                    logger.warning(f"Failed to kick user {uid} in group {group_id}: {e}")
                    continue
    except Exception as e:
        logger.error(f"Error in check_inactive for group {group_id}: {e}")

# --- Main Application ---
async def main():
    if not Config.TOKEN:
        logger.error("TELEGRAM_TOKEN is not set")
        raise ValueError("TELEGRAM_TOKEN is not set")

    db = Database(Config.DB_PATH)
    request = HTTPXRequest(read_timeout=30, write_timeout=30, connect_timeout=30)
    max_retries = 3
    for attempt in range(max_retries):
        try:
            app = (
                ApplicationBuilder()
                .token(Config.TOKEN)
                .arbitrary_callback_data(True)
                .request(request)
                .build()
            )
            break
        except TimedOut as e:
            if attempt < max_retries - 1:
                logger.warning(f"Timeout occurred, retrying ({attempt + 1}/{max_retries})...")
                await asyncio.sleep(5)
            else:
                logger.error("Max retries reached, exiting...")
                raise
    
    app.bot_data['db'] = db
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("autokick", toggle_autokick))
    app.add_handler(CommandHandler("setcheck", set_check_interval))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    
    try:
        intervals = await db.get_check_intervals()
        for group_id, interval in intervals:
            app.job_queue.run_repeating(
                callback=check_inactive,
                interval=interval,
                first=interval,
                data=group_id,
                name=f"inactive_check_{group_id}",
            )
            logger.info(f"Scheduled inactive check for group {group_id} every {interval}s")
    except Exception as e:
        logger.error(f"Error scheduling existing intervals: {e}")
    
    logger.info("✅ Bot is running with auto-checks")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    
    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())