# main.py — TikTok/Instagram Downloader + Premium Store via FreedomPay KG
import os
import asyncio
import tempfile
import shutil
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict

import aiosqlite
import aiohttp
from yt_dlp import YoutubeDL

from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup,
    FSInputFile, BotCommand
)

# ----------------- Настройки -----------------
API_TOKEN = os.getenv("TOKEN")  # токен бота
if not API_TOKEN:
    raise SystemExit("ERROR: Установи токен бота")

# FreedomPay KG (твой токен / API)
FREEDOMPAY_API_KEY = "6618536796:TEST:545158"  # сюда вставляешь API ключ FreedomPay

DB_PATH = "bot_users.db"
DOWNLOAD_WORKERS = 1
LOG_LEVEL = logging.INFO
ADMIN_IDS = [6705555401]  # <- твой ID

# Лимиты по премиуму
LIMITS = {
    "обычный": {"daily": 4, "queue": True, "high_res": False},
    "золотой": {"daily": 10, "queue": True, "high_res": False},
    "алмазный": {"daily": None, "queue": False, "high_res": True},
}

# yt-dlp форматы
YDL_FORMATS = {
    "high": "bestvideo+bestaudio/best",
    "normal": "best[ext=mp4]/best",
}

# Logging
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ----------------- Bot / Queue -----------------
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

@dataclass
class DownloadJob:
    id: str
    user_id: int
    chat_id: int
    url: str
    premium_level: str
    request_time: float

download_queue: deque[DownloadJob] = deque()
queue_lock = asyncio.Lock()
awaiting_link: Dict[int, bool] = {}
last_links: Dict[int, str] = {}

# ----------------- Database -----------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                premium TEXT DEFAULT 'обычный',
                downloads_today INTEGER DEFAULT 0,
                premium_until TEXT,
                last_reset TEXT
            )
        """)
        await db.commit()
    logger.info("DB initialized")

async def ensure_user(user_id: int, username: Optional[str]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users(id, username, last_reset) VALUES(?,?,?)",
            (user_id, username, datetime.utcnow().isoformat())
        )
        await db.commit()

async def get_user_row(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, username, premium, downloads_today, premium_until, last_reset FROM users WHERE id=?",
            (user_id,)
        ) as cur:
            return await cur.fetchone()

async def set_premium(user_id: int, level: str, days: Optional[int] = None):
    until = (datetime.utcnow() + timedelta(days=days)).isoformat() if days else None
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET premium=?, premium_until=? WHERE id=?",
            (level, until, user_id)
        )
        await db.commit()

async def increment_download(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET downloads_today = downloads_today + 1 WHERE id=?", (user_id,))
        await db.commit()

async def reset_if_needed(user_id: int):
    row = await get_user_row(user_id)
    if not row:
        return
    last_reset = row[5]
    if not last_reset:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET last_reset=? WHERE id=?", (datetime.utcnow().isoformat(), user_id))
            await db.commit()
        return
    last_dt = datetime.fromisoformat(last_reset)
    if datetime.utcnow() - last_dt >= timedelta(days=1):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET downloads_today=0, last_reset=? WHERE id=?", (datetime.utcnow().isoformat(), user_id))
            await db.commit()

async def can_user_download(user_id: int) -> bool:
    await reset_if_needed(user_id)
    row = await get_user_row(user_id)
    if not row:
        return True
    premium = row[2]
    downloads_today = row[3] or 0
    limit = LIMITS[premium]["daily"]
    return limit is None or downloads_today < limit

# ----------------- UI / Commands -----------------
def main_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🎬 Скачать видео", callback_data="download")],
        [InlineKeyboardButton(text="💎 Премиум подписка", callback_data="premium")],
    ])

def premium_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Купить Золотой (120 звёзд)", callback_data="buy_gold")],
        [InlineKeyboardButton(text="💰 Купить Алмазный (250 звёзд)", callback_data="buy_diamond")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]
    ])

async def register_commands():
    commands = [
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="profile", description="Профиль"),
        BotCommand(command="download", description="Скачать видео"),
        BotCommand(command="premium", description="Информация о премиум")
    ]
    await bot.set_my_commands(commands)

# ----------------- Handlers -----------------
@dp.message(CommandStart())
async def start_handler(msg: Message):
    await ensure_user(msg.from_user.id, msg.from_user.username)
    await msg.answer(
        "Привет! 👋\nЭтот бот скачивает видео с TikTok и Instagram.\nНажми кнопку «Скачать видео» и отправь ссылку.",
        reply_markup=main_buttons()
    )

@dp.message(Command("profile"))
async def cmd_profile(msg: Message):
    await ensure_user(msg.from_user.id, msg.from_user.username)
    row = await get_user_row(msg.from_user.id)
    if row:
        _, username, premium, downloads_today, premium_until, _ = row
        until_text = f"\nПремиум активен до: {premium_until}" if premium_until else ""
        await msg.answer(
            f"👤 Профиль\nЮзер: @{username or msg.from_user.id}\nПремиум: {premium}{until_text}\nСкачиваний сегодня: {downloads_today}"
        )
    else:
        await msg.answer("Профиль не найден. Нажми /start")

# ----------------- Premium -----------------
@dp.message(Command("premium"))
async def cmd_premium(msg: Message):
    text = (
        "💎 Премиум уровни:\n"
        "- обычный: 4 видео в день, обычное разрешение, очередь\n"
        "- золотой: 10 видео в день, обычное разрешение, очередь\n"
        "- алмазный: неограниченно, высокое разрешение, без очереди\n\n"
        "Выбери премиум ниже:"
    )
    await msg.answer(text, reply_markup=premium_buttons())

# ----------------- Callbacks -----------------
@dp.callback_query(lambda c: c.data == "profile")
async def cb_profile(cq: CallbackQuery):
    await cmd_profile(cq.message)
    await cq.answer()

@dp.callback_query(lambda c: c.data == "premium")
async def cb_premium(cq: CallbackQuery):
    await cmd_premium(cq.message)
    await cq.answer()

@dp.callback_query(lambda c: c.data.startswith("buy_"))
async def cb_buy(cq: CallbackQuery):
    user_id = cq.from_user.id
    if cq.data == "buy_gold":
        # Здесь можно вставить ссылку на FreedomPay
        link = f"https://freedompay.kg/pay?product=gold&user={user_id}"
    else:
        link = f"https://freedompay.kg/pay?product=diamond&user={user_id}"
    await cq.message.answer(f"💳 Для оплаты перейдите по ссылке:\n{link}")
    await cq.answer("Ссылка на оплату сгенерирована!")

@dp.callback_query(lambda c: c.data == "back_main")
async def cb_back(cq: CallbackQuery):
    await cq.message.answer("Главное меню:", reply_markup=main_buttons())
    await cq.answer()

# ----------------- Download -----------------
@dp.callback_query(lambda c: c.data == "download")
async def cb_download(cq: CallbackQuery):
    user_id = cq.from_user.id
    last = last_links.get(user_id)
    if last:
        await process_incoming_link(user_id, cq.message.chat.id, last, cq.message)
    else:
        awaiting_link[user_id] = True
        await cq.message.answer("📩 Отправь ссылку на TikTok или Instagram")
    await cq.answer()

@dp.message()
async def handle_message(msg: Message):
    user_id = msg.from_user.id
    text = (msg.text or "").strip()
    is_link = any(x in text for x in ("tiktok.com", "vm.tiktok", "instagram.com/reel", "instagram.com/p"))

    if is_link:
        await process_incoming_link(user_id, msg.chat.id, text, msg)
        return

    if awaiting_link.get(user_id):
        awaiting_link[user_id] = False
        if is_link:
            await process_incoming_link(user_id, msg.chat.id, text, msg)
        else:
            await msg.answer("❌ Пожалуйста, отправь ссылку на TikTok или Instagram.")
        return

    await msg.answer("Нажми «Скачать видео» или используй /download.", reply_markup=main_buttons())

# ----------------- Download Logic -----------------
async def enqueue_download(job: DownloadJob):
    async with queue_lock:
        if not LIMITS[job.premium_level]["queue"]:
            download_queue.appendleft(job)
        else:
            download_queue.append(job)
    logger.info("Job queued: %s", job)

async def download_worker():
    logger.info("Download worker started")
    loop = asyncio.get_event_loop()
    async with aiohttp.ClientSession() as session:
        while True:
            job = None
            async with queue_lock:
                if download_queue:
                    job = download_queue.popleft()
            if not job:
                await asyncio.sleep(0.5)
                continue

            if not await can_user_download(job.user_id):
                await bot.send_message(job.chat_id, "❌ Лимит скачиваний на сегодня достигнут.")
                continue

            tmpdir = tempfile.mkdtemp(prefix="bot_dl_")
            try:
                filename = None
                if "tiktok" in job.url:
                    filename = await download_tiktok(job.url, session)
                elif "instagram.com" in job.url:
                    filename = await download_instagram(job.url, session)
                else:
                    await bot.send_message(job.chat_id, "❌ Ссылка не поддерживается.")
                    continue

                if filename and os.path.exists(filename):
                    await bot.send_chat_action(job.chat_id, "upload_video")
                    fs = FSInputFile(filename)
                    await bot.send_video(job.chat_id, video=fs, supports_streaming=True)
                    await increment_download(job.user_id)
                    size_mb = os.path.getsize(filename) / 1024 / 1024
                    await bot.send_message(job.chat_id, f"✅ Готово! {size_mb:.1f} MB")
                else:
                    await bot.send_message(job.chat_id, "❌ Ошибка скачивания видео.")
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
            await asyncio.sleep(0.2)

async def process_incoming_link(user_id: int, chat_id: int, link: str, msg_obj: Optional[Message] = None):
    last_links[user_id] = link
    await ensure_user(user_id, None)
    row = await get_user_row(user_id)
    premium_level = row[2] if row else "обычный"

    job = DownloadJob(id=str(uuid.uuid4()), user_id=user_id, chat_id=chat_id, url=link, premium_level=premium_level, request_time=time.time())
    await enqueue_download(job)
    if msg_obj:
        await msg_obj.answer("⏳ Загрузка началась, подождите...")

# ----------------- TikTok / Instagram Download -----------------
async def download_tiktok(url: str, session: aiohttp.ClientSession):
    temp_dir = tempfile.mkdtemp(prefix="tt_dl_")
    out_file = os.path.join(temp_dir, "video.mp4")
    loop = asyncio.get_event_loop()

    def run_ydl():
        ydl_opts = {
            "format": YDL_FORMATS["normal"],
            "outtmpl": os.path.join(temp_dir, "%(id)s.%(ext)s"),
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True
        }
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return ydl.prepare_filename(info)

    try:
        filename = await loop.run_in_executor(None, run_ydl)
        return filename
    finally:
        pass

async def download_instagram(url: str, session: aiohttp.ClientSession):
    temp_dir = tempfile.mkdtemp(prefix="ig_dl_")
    out_file = os.path.join(temp_dir, "video.mp4")
    loop = asyncio.get_event_loop()

    def run_ydl():
        ydl_opts = {
            "format": YDL_FORMATS["normal"],
            "outtmpl": os.path.join(temp_dir, "%(id)s.%(ext)s"),
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True
        }
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return ydl.prepare_filename(info)

    try:
        filename = await loop.run_in_executor(None, run_ydl)
        return filename
    finally:
        pass

# ----------------- Run -----------------
async def main():
    await init_db()
    await register_commands()
    workers = [asyncio.create_task(download_worker()) for _ in range(DOWNLOAD_WORKERS)]
    try:
        logger.info("Bot starting polling")
        await dp.start_polling(bot)
    finally:
        for w in workers:
            w.cancel()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())