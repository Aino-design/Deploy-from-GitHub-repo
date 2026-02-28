# main.py
"""
Телеграм-бот: скачивание TikTok и Instagram (yt-dlp), явная блокировка YouTube.
В UI: кнопки Профиль, Скачать видео, О боте, Премиум.
Админ-команда: /grant_premium <user_id> <обычный|золотой|алмазный>
Удалена кнопка случайного TikTok.
"""

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
from typing import Optional, Dict, Tuple

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

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")  # <- обязательно вставь токен
if not TOKEN or TOKEN.startswith("PASTE_"):
    raise SystemExit("ERROR: Вставь токен в переменную TOKEN в main.py")

DB_PATH = "bot_users.db"
DOWNLOAD_WORKERS = 1
LOG_LEVEL = logging.INFO

# admin ids — укажи свои числовые id админов
ADMIN_IDS = [6705555401]  # <- замените на свои id или оставьте пустым []

# limits by premium level
LIMITS = {"обычный": 4, "золотой": 10, "алмазный": None}  # None = unlimited

# yt-dlp settings
YDL_FORMAT = "best[ext=mp4]/best"
COOKIES_FILE = "cookies.txt" if os.path.exists("cookies.txt") else None
FFMPEG_LOCATION = None  # можно указать путь к ffmpeg, если нужно

# Logging
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------- Bot / Dispatcher ----------------
bot = Bot(token=TOKEN)
dp = Dispatcher()

# ---------------- Small data types ----------------
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
awaiting_link: Dict[int, bool] = {}  # user_id -> waiting for link
last_links: Dict[int, str] = {}  # last sent link from user

# ---------------- Database ----------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                premium TEXT DEFAULT 'обычный',
                downloads_today INTEGER DEFAULT 0,
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
        async with db.execute("SELECT id, username, premium, downloads_today, last_reset FROM users WHERE id=?", (user_id,)) as cur:
            return await cur.fetchone()

async def set_premium(user_id: int, level: str):
    async with aiosqlite.connect(DB_PATH) as db:
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
    premium = row[2]
    downloads_today = row[3] or 0
    limit = LIMITS.get(premium, 4)
    if limit is None:
        return True
    return downloads_today < limit

# ---------------- UI / commands ----------------
def main_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🎬 Скачать видео", callback_data="download")],
        [InlineKeyboardButton(text="ℹ️ О боте", callback_data="about")],
        [InlineKeyboardButton(text="💎 Премиум", callback_data="premium")],
    ])

async def register_commands():
    commands = [
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="profile", description="Профиль"),
        BotCommand(command="download", description="Скачать видео"),
        BotCommand(command="about", description="О боте"),
        BotCommand(command="premium", description="Информация о премиум"),
        BotCommand(command="grant_premium", description="(Админ) выдать премиум: /grant_premium <user_id> <level>")
    ]
    try:
        await bot.set_my_commands(commands)
    except Exception:
        logger.exception("Could not set bot commands")

# ---------------- Handlers ----------------
@dp.message(CommandStart())
async def start_handler(msg: Message):
    await ensure_user(msg.from_user.id, msg.from_user.username)
    await msg.answer(
        "Привет! 👋\n\n"
        "Я могу скачивать видео из TikTok и Instagram (Reels / посты / IGTV).\n"
        "❌ YouTube не поддерживается — при отправке YouTube-ссылки я сразу сообщу об этом.\n\n"
        "Отправь ссылку на видео или нажми «Скачать видео».",
        reply_markup=main_buttons()
    )

@dp.message(Command("profile"))
async def cmd_profile(msg: Message):
    await ensure_user(msg.from_user.id, msg.from_user.username)
    row = await get_user_row(msg.from_user.id)
    if row:
        _, username, premium, downloads_today, _ = row
        await msg.answer(f"👤 Профиль\nЮзер: @{username or msg.from_user.id}\nПремиум: {premium}\nСкачиваний сегодня: {downloads_today}")
    else:
        await msg.answer("Профиль не найден. Нажми /start")

@dp.message(Command("about"))
async def cmd_about(msg: Message):
    await msg.answer("Этот бот скачивает TikTok и Instagram (через yt-dlp). YouTube не поддерживается. Файлы удаляются после отправки.")

@dp.message(Command("premium"))
async def cmd_premium(msg: Message):
    await msg.answer(
        "💎 Премиум уровни:\n"
        "- обычный: 4 видео/день\n"
        "- золотой: 10 видео/день\n"
        "- алмазный: неограниченно + приоритет\n\n"
        "Выдать премиум может только админ."
    )

@dp.message(Command("grant_premium"))
async def cmd_grant_premium(msg: Message):
    if msg.from_user.id not in ADMIN_IDS:
        await msg.answer("❌ Только админ может выдавать премиум.")
        return
    parts = (msg.text or "").split()
    if len(parts) < 3:
        await msg.answer("Использование: /grant_premium <user_id> <обычный|золотой|алмазный>")
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        await msg.answer("Неверный user_id.")
        return
    level = parts[2].lower()
    if level not in LIMITS:
        await msg.answer("Неверный уровень премиума.")
        return
    await ensure_user(target_id, None)
    await set_premium(target_id, level)
    await msg.answer(f"✅ Премиум {level} выдан пользователю {target_id}.")
    try:
        await bot.send_message(target_id, f"Тебе выдали премиум: {level} (админ {msg.from_user.id})")
    except Exception:
        pass

# callbacks
@dp.callback_query(lambda c: c.data == "profile")
async def cb_profile(cq: CallbackQuery):
    await cmd_profile(cq.message)
    await cq.answer()

@dp.callback_query(lambda c: c.data == "about")
async def cb_about(cq: CallbackQuery):
    await cmd_about(cq.message)
    await cq.answer()

@dp.callback_query(lambda c: c.data == "premium")
async def cb_premium(cq: CallbackQuery):
    await cmd_premium(cq.message)
    await cq.answer()

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

# ---------------- Queue / worker ----------------
async def enqueue_download(job: DownloadJob):
    async with queue_lock:
        if job.premium_level == "алмазный":
            download_queue.appendleft(job)
        else:
            download_queue.append(job)
    logger.info("Job queued: %s", job)

# helper: run yt-dlp blocking (for Instagram & TikTok if possible)
class YouTubeNotSupported(Exception):
    pass

def run_yt_dlp_blocking(url: str, outdir: str, ydl_format: Optional[str] = None) -> Tuple[str, dict]:
    if "youtube.com" in url or "youtu.be" in url:
        # do not attempt to download YouTube
        raise YouTubeNotSupported()

    ydl_opts = {
        "format": ydl_format or YDL_FORMAT,
        "outtmpl": os.path.join(outdir, "%(id)s.%(ext)s"),
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "http_headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    }
    if COOKIES_FILE:
        ydl_opts["cookiefile"] = COOKIES_FILE
    if FFMPEG_LOCATION:
        ydl_opts["ffmpeg_location"] = FFMPEG_LOCATION

    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        filename = ydl.prepare_filename(info)
        return filename, info

async def download_tiktok_fallback(url: str, session: Optional[aiohttp.ClientSession] = None) -> str:
    temp_dir = tempfile.mkdtemp(prefix="tt_dl_")
    out_file = os.path.join(temp_dir, "video.mp4")
    own_session = False
    if session is None:
        session = aiohttp.ClientSession()
        own_session = True
    try:
        api = f"https://www.tikwm.com/api/?url={url}"
        async with session.get(api, timeout=20) as resp:
            if resp.status != 200:
                raise Exception(f"API returned {resp.status}")
            data = await resp.json()
            video_url = (data.get("data") or {}).get("play") or (data.get("data") or {}).get("download")
            if not video_url:
                text = await resp.text()
                import re
                urls = re.findall(r'https?://[^\s"\']+', text)
                candidates = [u for u in urls if ".mp4" in u or "v.tiktok" in u or "vm.tiktok" in u]
                video_url = candidates[0] if candidates else None
            if not video_url:
                raise Exception("No video URL found in API response")

            async with session.get(video_url, timeout=60) as vf:
                if vf.status != 200:
                    raise Exception(f"Video URL returned {vf.status}")
                with open(out_file, "wb") as f:
                    while True:
                        chunk = await vf.content.read(1024 * 32)
                        if not chunk:
                            break
                        f.write(chunk)
                if os.path.exists(out_file) and os.path.getsize(out_file) > 1000:
                    return out_file
                else:
                    raise Exception("Downloaded file is too small or missing")
    except Exception as e:
        try:
            shutil.rmtree(temp_dir)
        except Exception:
            pass
        raise Exception(f"TikTok download failed: {e}")
    finally:
        if own_session:
            await session.close()

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

            logger.info("Processing job: %s", job)

            if not await can_user_download(job.user_id):
                try:
                    await bot.send_message(job.chat_id, "❌ Лимит скачиваний на сегодня достигнут.")
                except Exception:
                    logger.exception("notify error")
                continue

            tmpdir = tempfile.mkdtemp(prefix="bot_dl_")
            try:
                filename = None
                info = {}

                # Immediate YouTube response (safety)
                if "youtube.com" in job.url or "youtu.be" in job.url:
                    try:
                        await bot.send_message(job.chat_id, "❌ Этот бот не может загружать YouTube видео.")
                    except Exception:
                        pass
                    continue

                # TikTok handling
                if "tiktok" in job.url or "vm.tiktok" in job.url:
                    try:
                        # try yt-dlp first
                        try:
                            filename, info = await loop.run_in_executor(None, run_yt_dlp_blocking, job.url, tmpdir, None)
                        except YouTubeNotSupported:
                            # safe guard, should not happen here
                            await bot.send_message(job.chat_id, "❌ Этот бот не может загружать YouTube видео.")
                            continue
                        except Exception as e:
                            logger.debug("yt-dlp for tiktok failed: %s", e)
                            # fallback
                            try:
                                filename = await download_tiktok_fallback(job.url, session=session)
                                info = {}
                            except Exception as e2:
                                logger.exception("TikTok fallback failed: %s", e2)
                                await bot.send_message(job.chat_id, "❌ Не удалось скачать TikTok (yt-dlp и fallback не сработали).")
                                continue
                    except Exception as e:
                        logger.exception("TikTok download error for %s", job.url)
                        try:
                            await bot.send_message(job.chat_id, f"❌ Ошибка при скачивании TikTok: {e}")
                        except Exception:
                            pass
                        try:
                            shutil.rmtree(tmpdir)
                        except Exception:
                            pass
                        continue

                # Instagram handling
                elif "instagram.com" in job.url or "instagr.am" in job.url:
                    try:
                        filename, info = await loop.run_in_executor(None, run_yt_dlp_blocking, job.url, tmpdir, None)
                    except YouTubeNotSupported:
                        await bot.send_message(job.chat_id, "❌ Этот бот не может загружать YouTube видео.")
                        continue
                    except Exception as e:
                        logger.exception("Instagram download error for %s", job.url)
                        try:
                            await bot.send_message(job.chat_id, f"❌ Ошибка при скачивании Instagram: {e}")
                        except Exception:
                            pass
                        try:
                            shutil.rmtree(tmpdir)
                        except Exception:
                            pass
                        continue

                else:
                    try:
                        await bot.send_message(job.chat_id, "❌ Этот бот не может загружать видео с этого сайта.")
                    except Exception:
                        pass
                    continue

                # thumbnail (optional)
                thumb_path = None
                thumbnail_url = info.get("thumbnail") if isinstance(info, dict) else None
                if thumbnail_url:
                    try:
                        async with session.get(thumbnail_url, timeout=15) as resp:
                            if resp.status == 200:
                                data = await resp.read()
                                thumb_path = os.path.join(tmpdir, "thumb.jpg")
                                with open(thumb_path, "wb") as f:
                                    f.write(data)
                    except Exception:
                        thumb_path = None

                # send file
                if filename and os.path.exists(filename):
                    try:
                        await bot.send_chat_action(job.chat_id, "upload_video")
                        fs = FSInputFile(filename)
                        if thumb_path and os.path.exists(thumb_path):
                            thumb = FSInputFile(thumb_path)
                            await bot.send_video(job.chat_id, video=fs, thumbnail=thumb, supports_streaming=True)
                        else:
                            await bot.send_video(job.chat_id, video=fs, supports_streaming=True)
                        size_mb = os.path.getsize(filename) / 1024 / 1024
                        await bot.send_message(job.chat_id, f"✅ Готово! {size_mb:.1f} MB")
                        await increment_download(job.user_id)
                    except Exception as e:
                        logger.exception("Failed to send video")
                        try:
                            await bot.send_message(job.chat_id, f"❌ Ошибка отправки видео: {e}")
                        except Exception:
                            pass
                    finally:
                        # cleanup file and parent
                        try:
                            os.remove(filename)
                        except Exception:
                            pass
                        try:
                            if thumb_path and os.path.exists(thumb_path):
                                os.remove(thumb_path)
                        except Exception:
                            pass
                        try:
                            parent = os.path.dirname(filename)
                            if parent and parent != tmpdir and parent.startswith(tempfile.gettempdir()):
                                shutil.rmtree(parent, ignore_errors=True)
                        except Exception:
                            pass
                else:
                    try:
                        await bot.send_message(job.chat_id, "❌ Файл не найден после скачивания.")
                    except Exception:
                        pass
            finally:
                try:
                    shutil.rmtree(tmpdir, ignore_errors=True)
                except Exception:
                    pass
            await asyncio.sleep(0.2)

# ---------------- Incoming messages ----------------
async def process_incoming_link(user_id: int, chat_id: int, link: str, msg_obj: Optional[Message] = None):
    last_links[user_id] = link
    await ensure_user(user_id, None)
    row = await get_user_row(user_id)
    premium_level = row[2] if row else "обычный"

    # If link is YouTube -> immediately inform user
    if "youtube.com" in link or "youtu.be" in link:
        if msg_obj:
            await msg_obj.answer("❌ Этот бот не может загружать YouTube видео.")
        else:
            await bot.send_message(chat_id, "❌ Этот бот не может загружать YouTube видео.")
        return

    if not await can_user_download(user_id):
        if msg_obj:
            await msg_obj.answer("❌ Лимит скачиваний на сегодня исчерпан.")
        else:
            await bot.send_message(chat_id, "❌ Лимит скачиваний на сегодня исчерпан.")
        return

    job = DownloadJob(id=str(uuid.uuid4()), user_id=user_id, chat_id=chat_id, url=link, premium_level=premium_level, request_time=time.time())
    await enqueue_download(job)

    if msg_obj:
        await msg_obj.answer("⏳ Загрузка началась, пожалуйста подождите...")
    else:
        await bot.send_message(chat_id, "⏳ Загрузка началась, пожалуйста подождите...")

@dp.message()
async def handle_message(msg: Message):
    user_id = msg.from_user.id
    text = (msg.text or "").strip()

    is_link = any(x in text for x in ("youtube.com", "youtu.be", "tiktok.com", "vm.tiktok", "instagram.com", "instagr.am"))
    if is_link:
        # If user had clicked download and awaiting_link true, still process
        await process_incoming_link(user_id, msg.chat.id, text, msg)
        return

    if awaiting_link.get(user_id):
        awaiting_link[user_id] = False
        if is_link:
            await process_incoming_link(user_id, msg.chat.id, text, msg)
        else:
            await msg.answer("❌ Пожалуйста, отправь ссылку на TikTok или Instagram.")
        return

    await msg.answer("Нажми «Скачать видео» или используй /download. Для справки /about", reply_markup=main_buttons())

# ---------------- Run ----------------
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
        try:
            await bot.session.close()
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(main())