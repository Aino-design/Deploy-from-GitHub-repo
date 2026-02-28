# main.py
import os
import re
import json
import asyncio
import tempfile
import shutil
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

import aiosqlite
import aiohttp
from yt_dlp import YoutubeDL
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, LabeledPrice, PreCheckoutQuery,
    FSInputFile
)

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")  # <- вставь токен
if not TOKEN or TOKEN.startswith("PASTE_"):
    raise SystemExit("ERROR: Вставь реальный токен в TOKEN в main.py")

ADMIN_IDS = [6705555401]  # <- твой Telegram ID для уведомлений

DB_PATH = "bot_users.db"
DOWNLOAD_WORKERS = 1
LOG_LEVEL = logging.INFO

# Payments (Telegram Stars)
STARS_PROVIDER_TOKEN = ""  # пусто для Telegram Stars
STARS_CURRENCY = "XTR"

# Premium pricing/durations
GOLD_PRICE_STARS = 120
GOLD_DAYS = 30
DIAMOND_PRICE_STARS = 250
DIAMOND_DAYS = 90

# yt-dlp
YDL_FORMAT = "best[ext=mp4]/best"
COOKIES_FILE = "cookies.txt" if os.path.exists("cookies.txt") else None
FFMPEG_LOCATION = None

# limits
LIMITS = {"обычный": 4, "золотой": 10, "алмазный": None}

# Logging
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------- Bot init ----------------
bot = Bot(token=TOKEN)
dp = Dispatcher()

# ---------------- Types & queues ----------------
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

# ---------------- DB helpers ----------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                premium TEXT DEFAULT 'обычный',
                downloads_today INTEGER DEFAULT 0,
                last_reset TEXT,
                premium_expires TEXT
            )
        """)
        await db.commit()

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
            "SELECT id, username, premium, downloads_today, last_reset, premium_expires FROM users WHERE id=?",
            (user_id,)
        ) as cur:
            return await cur.fetchone()

async def set_premium(user_id: int, level: str, days: Optional[int] = None):
    expires = (datetime.utcnow() + timedelta(days=days)).isoformat() if days else None
    async with aiosqlite.connect(DB_PATH) as db:
        if expires:
            await db.execute("UPDATE users SET premium=?, premium_expires=? WHERE id=?", (level, expires, user_id))
        else:
            await db.execute("UPDATE users SET premium=? WHERE id=?", (level, user_id))
        await db.commit()

async def increment_download(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET downloads_today = downloads_today + 1 WHERE id=?", (user_id,))
        await db.commit()

async def reset_if_needed(user_id: int):
    row = await get_user_row(user_id)
    if not row:
        return
    last_reset = row[4]
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
    premium = row[2] or "обычный"
    downloads_today = row[3] or 0
    limit = LIMITS.get(premium, 4)
    return (limit is None) or (downloads_today < limit)

async def is_premium_active(user_id: int) -> Tuple[bool, str]:
    row = await get_user_row(user_id)
    if not row:
        return False, "обычный"
    premium, expires = row[2], row[5]
    if expires:
        if datetime.utcnow() < datetime.fromisoformat(expires):
            return True, premium
        else:
            await set_premium(user_id, "обычный")
            return False, "обычный"
    return premium != "обычный", premium

# ---------------- URL detection ----------------
def is_youtube_url(url: str) -> bool:
    return "youtube.com" in url.lower() or "youtu.be" in url.lower()

def is_tiktok_url(url: str) -> bool:
    return "tiktok.com" in url.lower() or "vm.tiktok" in url.lower() or "vt.tiktok.com" in url.lower()

def is_instagram_url(url: str) -> bool:
    return "instagram.com" in url.lower() or "instagr.am" in url.lower()

# ---------------- Download TikTok/Instagram ----------------
def run_yt_dlp_blocking(url: str, outdir: str):
    ydl_opts = {
        "format": YDL_FORMAT,
        "outtmpl": os.path.join(outdir, "%(id)s.%(ext)s"),
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "http_headers": {"User-Agent": "Mozilla/5.0"},
    }
    if COOKIES_FILE:
        ydl_opts["cookiefile"] = COOKIES_FILE
    if FFMPEG_LOCATION:
        ydl_opts["ffmpeg_location"] = FFMPEG_LOCATION
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        filename = ydl.prepare_filename(info)
        return filename

# ---------------- Queue ----------------
@dataclass
class SimpleJob:
    user_id: int
    chat_id: int
    url: str
    premium_level: str

download_queue: deque[SimpleJob] = deque()
queue_lock = asyncio.Lock()

async def enqueue_download(job: SimpleJob):
    async with queue_lock:
        download_queue.append(job)

async def download_worker():
    while True:
        job = None
        async with queue_lock:
            if download_queue:
                job = download_queue.popleft()
        if not job:
            await asyncio.sleep(0.5)
            continue
        if is_youtube_url(job.url):
            await bot.send_message(job.chat_id, "❌ Бот не поддерживает YouTube Shorts.")
            continue
        if is_tiktok_url(job.url) or is_instagram_url(job.url):
            tmpdir = tempfile.mkdtemp()
            try:
                filename = await asyncio.get_event_loop().run_in_executor(None, run_yt_dlp_blocking, job.url, tmpdir)
                await bot.send_video(job.chat_id, FSInputFile(filename))
                await increment_download(job.user_id)
                await bot.send_message(job.chat_id, "✅ Готово!")
            except Exception as e:
                await bot.send_message(job.chat_id, f"❌ Ошибка при скачивании: {e}")
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
        else:
            await bot.send_message(job.chat_id, "❌ Этот бот может скачивать только TikTok и Instagram.")

# ---------------- Commands ----------------
@dp.message(CommandStart())
async def start_handler(msg: Message):
    await ensure_user(msg.from_user.id, msg.from_user.username)
    await msg.answer(
        "Привет! 👋\n"
        "Я могу скачивать видео и фото из TikTok и Instagram.\n"
        "Для скачивания отправь ссылку или используй команду /download"
    )

@dp.message(Command("profile"))
async def profile_handler(msg: Message):
    await ensure_user(msg.from_user.id, msg.from_user.username)
    row = await get_user_row(msg.from_user.id)
    if row:
        _, username, premium, downloads_today, _, premium_expires = row
        await msg.answer(
            f"👤 Профиль\n"
            f"Юзер: @{username or msg.from_user.id}\n"
            f"Премиум: {premium}\n"
            f"Истекает: {premium_expires or 'нет'}\n"
            f"Скачиваний сегодня: {downloads_today}"
        )

@dp.message(Command("download"))
async def download_command(msg: Message):
    awaiting_link[msg.from_user.id] = True
    await msg.answer("📩 Отправь ссылку на TikTok или Instagram:")

@dp.message(Command("premium"))
async def premium_info(msg: Message):
    await msg.answer(
        "💎 Уровни премиума:\n"
        "Обычный: 4 загрузки/день, обычное разрешение, очередь\n"
        f"Золотой: 10 загрузок/день, обычное разрешение, очередь — {GOLD_PRICE_STARS} ⭐ (30 дней)\n"
        f"Алмазный: неограниченно, высокое разрешение, без очереди — {DIAMOND_PRICE_STARS} ⭐ (90 дней)\n"
        "Для покупки используй команду /buy_premium"
    )

@dp.message(Command("buy_premium"))
async def buy_premium(msg: Message):
    kb_text = (
        f"Доступные тарифы:\n"
        f"/buy_gold — Золотой ({GOLD_PRICE_STARS} ⭐ / 30 дней)\n"
        f"/buy_diamond — Алмазный ({DIAMOND_PRICE_STARS} ⭐ / 90 дней)\n"
    )
    await msg.answer(kb_text)

@dp.message(Command("buy_gold"))
async def buy_gold(msg: Message):
    payload = f"premium:gold:{msg.from_user.id}:{GOLD_DAYS}:{uuid.uuid4().hex}"
    prices = [LabeledPrice(label=f"Золотой ({GOLD_DAYS} дней)", amount=GOLD_PRICE_STARS)]
    await bot.send_invoice(msg.chat.id, title="Золотой премиум", description="Покупка премиума", payload=payload,
                           provider_token=STARS_PROVIDER_TOKEN, currency=STARS_CURRENCY, prices=prices, start_parameter="premium")

@dp.message(Command("buy_diamond"))
async def buy_diamond(msg: Message):
    payload = f"premium:diamond:{msg.from_user.id}:{DIAMOND_DAYS}:{uuid.uuid4().hex}"
    prices = [LabeledPrice(label=f"Алмазный ({DIAMOND_DAYS} дней)", amount=DIAMOND_PRICE_STARS)]
    await bot.send_invoice(msg.chat.id, title="Алмазный премиум", description="Покупка премиума", payload=payload,
                           provider_token=STARS_PROVIDER_TOKEN, currency=STARS_CURRENCY, prices=prices, start_parameter="premium")

# ---------------- PreCheckout ----------------
@dp.pre_checkout_query()
async def pre_checkout(pre: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre.id, ok=True)

@dp.message()
async def generic_handler(msg: Message):
    user_id = msg.from_user.id
    text = (msg.text or "").strip()
    if awaiting_link.get(user_id):
        awaiting_link[user_id] = False
        await enqueue_download(SimpleJob(user_id=user_id, chat_id=msg.chat.id, url=text, premium_level=(await get_user_row(user_id))[2]))
    else:
        await msg.answer("Неизвестная команда. Используй /download, /profile, /premium или /buy_premium")

# ---------------- Run ----------------
async def main():
    await init_db()
    asyncio.create_task(download_worker())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())