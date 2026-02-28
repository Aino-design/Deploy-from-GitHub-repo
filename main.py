# main.py
"""
Telegram bot:
- TikTok downloader (видео, фото, музыка)
- Instagram downloader (Reels / посты / фото)
- YouTube НЕ поддерживается, при ссылке выводится вежливое сообщение
- Магазин премиума через Telegram Stars
- Лимиты по премиуму:
    обычный: 4 видео/день, загрузка в очереди, обычное разрешение
    золотой: 10 видео/день, загрузка в очереди, обычное разрешение
    алмазный: неограниченно, загрузка вне очереди, высокое разрешение
- SQLite для пользователей + expiry
"""
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
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup,
    FSInputFile, BotCommand, InputMediaPhoto,
    LabeledPrice, PreCheckoutQuery
)

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")  # <- вставь токен
ADMIN_IDS = [6705555401]  # <- твой Telegram ID
DB_PATH = "bot_users.db"
DOWNLOAD_WORKERS = 1
LOG_LEVEL = logging.INFO

# Payments (Telegram Stars)
STARS_PROVIDER_TOKEN = ""  # токен провайдера, пусто для Stars
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

# Limits
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
        await db.execute("INSERT OR IGNORE INTO users(id, username, last_reset) VALUES(?,?,?)",
                         (user_id, username, datetime.utcnow().isoformat()))
        await db.commit()

async def get_user_row(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, username, premium, downloads_today, last_reset, premium_expires FROM users WHERE id=?", (user_id,)) as cur:
            return await cur.fetchone()

async def set_premium(user_id: int, level: str, days: Optional[int] = None):
    expires = None
    if days:
        expires = (datetime.utcnow() + timedelta(days=days)).isoformat()
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
    limit = LIMITS.get(premium)
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

# ---------------- UI ----------------
def main_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🎬 Скачать видео", callback_data="download")],
        [InlineKeyboardButton(text="ℹ️ О боте", callback_data="about")],
        [InlineKeyboardButton(text="💎 Премиум", callback_data="premium")],
    ])

async def register_commands():
    await bot.set_my_commands([
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="profile", description="Профиль"),
        BotCommand(command="download", description="Скачать видео"),
        BotCommand(command="about", description="О боте"),
        BotCommand(command="premium", description="Информация о премиум"),
        BotCommand(command="grant_premium", description="(Админ) выдать премиум")
    ])

# ---------------- URL detection ----------------
def is_youtube_url(url: str) -> bool:
    return url and ("youtube.com" in url.lower() or "youtu.be" in url.lower())

def is_tiktok_url(url: str) -> bool:
    return url and ("tiktok.com" in url.lower() or "vm.tiktok" in url.lower() or "vt.tiktok.com" in url.lower())

def is_instagram_url(url: str) -> bool:
    return url and ("instagram.com" in url.lower() or "instagr.am" in url.lower())

# ---------------- Download helpers ----------------
def run_yt_dlp_blocking(url: str, outdir: str, ydl_format: Optional[str] = None) -> Tuple[str, dict]:
    if is_youtube_url(url):
        raise RuntimeError("YouTube видео не поддерживается")
    ydl_opts = {"format": ydl_format or YDL_FORMAT, "outtmpl": os.path.join(outdir, "%(id)s.%(ext)s"),
                "quiet": True, "no_warnings": True, "noplaylist": True, "http_headers": {"User-Agent": "Mozilla/5.0"}}
    if COOKIES_FILE:
        ydl_opts["cookiefile"] = COOKIES_FILE
    if FFMPEG_LOCATION:
        ydl_opts["ffmpeg_location"] = FFMPEG_LOCATION
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        filename = ydl.prepare_filename(info)
        return filename, info

# ---------------- TikTok download ----------------
async def download_tiktok_content(url: str) -> dict:
    tmpdir = tempfile.mkdtemp(prefix="ttjob_")
    images, audio_file = [], None
    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, timeout=20) as resp:
            html = await resp.text()
    found = re.findall(r"https?://[^\s'\"<>]+?\.(?:jpe?g|png|webp)", html)
    seen = set()
    for u in found:
        if u not in seen:
            seen.add(u)
            images.append(u)
    return {"type": "photos_urls" if images else "video", "images": images, "audio_url": audio_file, "tmpdir": tmpdir}

# ---------------- Queue ----------------
async def enqueue_download(job: DownloadJob):
    async with queue_lock:
        if job.premium_level == "алмазный":
            download_queue.appendleft(job)
        else:
            download_queue.append(job)

async def download_worker():
    loop = asyncio.get_event_loop()
    while True:
        job = None
        async with queue_lock:
            if download_queue:
                job = download_queue.popleft()
        if not job:
            await asyncio.sleep(0.5)
            continue
        if not await can_user_download(job.user_id):
            await bot.send_message(job.chat_id, "❌ Лимит скачиваний на сегодня исчерпан.")
            continue
        if is_youtube_url(job.url):
            await bot.send_message(job.chat_id, "❌ YouTube видео не поддерживается.")
            continue
        if is_tiktok_url(job.url):
            try:
                res = await download_tiktok_content(job.url)
                if res["type"] == "photos_urls":
                    for img in res["images"][:10]:
                        await bot.send_photo(job.chat_id, img)
                await increment_download(job.user_id)
            except Exception as e:
                await bot.send_message(job.chat_id, f"❌ Ошибка TikTok: {e}")
        elif is_instagram_url(job.url):
            tmpdir = tempfile.mkdtemp()
            try:
                filename, _ = await loop.run_in_executor(None, run_yt_dlp_blocking, job.url, tmpdir, None)
                await bot.send_video(job.chat_id, FSInputFile(filename))
                await increment_download(job.user_id)
            except Exception as e:
                await bot.send_message(job.chat_id, f"❌ Ошибка Instagram: {e}")
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
        else:
            await bot.send_message(job.chat_id, "❌ Сайт не поддерживается.")

# ---------------- Payments ----------------
def build_price(label: str, stars_amount: int) -> List[LabeledPrice]:
    return [LabeledPrice(label=label, amount=stars_amount)]

@dp.callback_query(lambda c: c.data == "premium")
async def cb_premium(cq: CallbackQuery):
    active, level = await is_premium_active(cq.from_user.id)
    text = f"У тебя уже активен премиум: {level}." if active else "Выбери тариф премиума и оплати звёздами."
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Золотой — {GOLD_PRICE_STARS}⭐ (30 дней)", callback_data="buy_gold")],
        [InlineKeyboardButton(text=f"Алмазный — {DIAMOND_PRICE_STARS}⭐ (3 месяца)", callback_data="buy_diamond")],
        [InlineKeyboardButton(text="Назад", callback_data="menu_back")],
    ])
    await cq.message.answer(text, reply_markup=kb)
    await cq.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("buy_"))
async def cb_buy(cq: CallbackQuery):
    data = cq.data
    if data == "buy_gold":
        days, amount, key = GOLD_DAYS, GOLD_PRICE_STARS, "gold"
    else:
        days, amount, key = DIAMOND_DAYS, DIAMOND_PRICE_STARS, "diamond"
    payload = f"premium:{key}:{cq.from_user.id}:{days}:{uuid.uuid4().hex}"
    prices = build_price(f"{key} премиум", amount)
    await bot.send_invoice(
        chat_id=cq.from_user.id,
        title=f"{key} премиум",
        description=f"Покупка {key} премиума",
        payload=payload,
        provider_token=STARS_PROVIDER_TOKEN,
        currency=STARS_CURRENCY,
        prices=prices,
        start_parameter="premium-buy"
    )
    await cq.answer()

@dp.pre_checkout_query()
async def process_pre_checkout(pre: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre.id, ok=True)

@dp.message()
async def handle_payments_and_messages(msg: Message):
    sp = getattr(msg, "successful_payment", None)
    if sp:
        payload = sp.invoice_payload
        try:
            _, level_key, intended_user_id, days_str, _ = payload.split(":")
            if int(intended_user_id) != msg.from_user.id:
                await msg.answer("Ошибка: ID плательщика не совпадает.")
                return
            days = int(days_str)
            level_name = "золотой" if level_key == "gold" else "алмазный"
            await set_premium(msg.from_user.id, level_name, days)
            await msg.answer(f"✅ Оплата принята! Премиум: {level_name} на {days} дней.")
        except Exception as e:
            await msg.answer("Оплата прошла, но ошибка при выдаче премиума.")
        return
    await generic_message_handler(msg)

# ---------------- Handlers ----------------
@dp.message(CommandStart())
async def start_handler(msg: Message):
    await ensure_user(msg.from_user.id, msg.from_user.username)
    await msg.answer(
        "Привет! 👋\nЯ скачиваю медиа из TikTok и Instagram.\n"
        "Отправь ссылку или нажми «Скачать видео».",
        reply_markup=main_buttons()
    )

@dp.message(Command("profile"))
async def cmd_profile(msg: Message):
    row = await get_user_row(msg.from_user.id)
    if row:
        _, username, premium, downloads_today, _, premium_expires = row
        await msg.answer(f"👤 Профиль\nЮзер: @{username or msg.from_user.id}\nПремиум: {premium}\nИстекает: {premium_expires or 'нет'}\nСкачиваний сегодня: {downloads_today}")
    else:
        await msg.answer("Профиль не найден. Нажми /start")

@dp.message(Command("about"))
async def cmd_about(msg: Message):
    await msg.answer("Бот скачивает TikTok и Instagram. Поддерживаются видео и фото-посты (фото + музыка).")

@dp.message(Command("grant_premium"))
async def cmd_grant_premium(msg: Message):
    if msg.from_user.id not in ADMIN_IDS:
        await msg.answer("❌ Только админ может выдавать премиум.")
        return
    parts = (msg.text or "").split()
    if len(parts) < 3:
        await msg.answer("Использование: /grant_premium <user_id> <обычный|золотой|алмазный> [days]")
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        await msg.answer("Неверный user_id.")
        return
    level = parts[2].lower()
    days = int(parts[3]) if len(parts) >= 4 else None
    await ensure_user(target_id, None)
    await set_premium(target_id, level, days)
    await msg.answer(f"✅ Премиум {level} выдан пользователю {target_id}.")
    try:
        await bot.send_message(target_id, f"Тебе выдали премиум: {level} (админ {msg.from_user.id})")
    except Exception:
        pass

# ---------------- Incoming links ----------------
async def process_incoming_link(user_id: int, chat_id: int, link: str, msg_obj: Optional[Message] = None):
    last_links[user_id] = link
    await ensure_user(user_id, None)
    row = await get_user_row(user_id)
    premium_level = row[2] if row else "обычный"
    job = DownloadJob(id=str(uuid.uuid4()), user_id=user_id, chat_id=chat_id, url=link, premium_level=premium_level, request_time=time.time())
    await enqueue_download(job)
    if msg_obj:
        await msg_obj.answer("⏳ Загрузка началась, подождите...")
    else:
        await bot.send_message(chat_id, "⏳ Загрузка началась, подождите...")

async def generic_message_handler(msg: Message):
    text = (msg.text or "").strip()
    if any(x in text for x in ("tiktok.com", "instagram.com")):
        await process_incoming_link(msg.from_user.id, msg.chat.id, text, msg)
        return
    if awaiting_link.get(msg.from_user.id):
        awaiting_link[msg.from_user.id] = False
        await msg.answer("❌ Пожалуйста, отправь ссылку на TikTok или Instagram.")
        return
    await msg.answer("Нажми «Скачать видео» или отправь ссылку на TikTok / Instagram.", reply_markup=main_buttons())

# ---------------- Run ----------------
async def main():
    await init_db()
    await register_commands()
    workers = [asyncio.create_task(download_worker()) for _ in range(DOWNLOAD_WORKERS)]
    try:
        await dp.start_polling(bot)
    finally:
        for w in workers:
            w.cancel()
        try:
            await bot.session.close()
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(main())