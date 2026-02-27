# main.py — обновлённая версия (сохранённая логика + исправления)
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
API_TOKEN = "8736949755:AAG8So7fVUlyNpJxmGQptWQNk5bx7kjPoLs"
if not API_TOKEN:
    raise SystemExit("ERROR: TOKEN не найден. Установи переменную окружения TOKEN или вставь токен в код.")

DB_PATH = "bot_users.db"
DOWNLOAD_WORKERS = 1
LOG_LEVEL = logging.INFO

# Админы
ADMIN_IDS = [6705555401]  # твой числовой ID
ADMIN_USERNAME = "KRONIK568"  # пользователь, которому разрешено self-admin

# Лимиты
LIMITS = {"обычный": 4, "золотой": 10, "алмазный": None}  # None = неограниченно

# Форматы yt-dlp — NORMAL изменён, чтобы не требовать ffmpeg
YDL_FORMATS = {
    "diamond": "bestvideo+bestaudio/best",
    "normal": "best[height<=720]/best",  # не требует слияния форматов
}

# Общие опции yt-dlp
YDL_COMMON_OPTS = {
    "noplaylist": True,
    "no_warnings": True,
    "quiet": True,
}

# Логи
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ----------------- Бот / очередь -----------------
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
awaiting_link: Dict[int, bool] = {}  # user_id -> waiting for link
last_links: Dict[int, str] = {}  # user_id -> last sent link (allows "send link first, then press button")

# ----------------- БД -----------------
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

# ----------------- UI / команды -----------------
def main_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🎬 Скачать видео", callback_data="download")],
        [InlineKeyboardButton(text="ℹ️ О боте", callback_data="about")],
        [InlineKeyboardButton(text="💎 Премиум подписка", callback_data="premium")],
        [InlineKeyboardButton(text="🔑 Выдать себе админку", callback_data="make_admin")],
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
    await bot.set_my_commands(commands)

# ----------------- Обработчики -----------------
@dp.message(CommandStart())
async def start_handler(msg: Message):
    await ensure_user(msg.from_user.id, msg.from_user.username)
    await msg.answer(
        "Привет! 👋\nЭтот бот скачивает YouTube Shorts и TikTok.\nОтправь ссылку или нажми «Скачать видео».",
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
    await msg.answer("Бот скачивает YouTube Shorts и TikTok (через yt-dlp). Файлы удаляются после отправки.")

@dp.message(Command("download"))
async def cmd_download(msg: Message):
    # Если команда использована с аргументом — /download <url>
    text = (msg.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) == 2 and parts[1]:
        link = parts[1].strip()
        # обработаем как ссылка
        await process_incoming_link(msg.from_user.id, msg.chat.id, link, msg)
        return

    # если у нас есть последняя ссылка от этого пользователя — используем её
    last = last_links.get(msg.from_user.id)
    if last:
        await process_incoming_link(msg.from_user.id, msg.chat.id, last, msg)
        return

    # иначе — просим ссылку (кнопка всё ещё полезна)
    awaiting_link[msg.from_user.id] = True
    await msg.answer("📩 Отправь ссылку на YouTube Shorts или TikTok")

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

# колбэки
@dp.callback_query(lambda c: c.data == "profile")
async def cb_profile(cq: CallbackQuery):
    await cmd_profile(cq.message)

@dp.callback_query(lambda c: c.data == "about")
async def cb_about(cq: CallbackQuery):
    await cmd_about(cq.message)

@dp.callback_query(lambda c: c.data == "premium")
async def cb_premium(cq: CallbackQuery):
    await cmd_premium(cq.message)

@dp.callback_query(lambda c: c.data == "download")
async def cb_download(cq: CallbackQuery):
    user_id = cq.from_user.id
    last = last_links.get(user_id)
    if last:
        await process_incoming_link(user_id, cq.message.chat.id, last, cq.message)
    else:
        awaiting_link[user_id] = True
        await cq.message.answer("📩 Отправь ссылку на YouTube Shorts или TikTok")
    await cq.answer()

@dp.callback_query(lambda c: c.data == "make_admin")
async def cb_make_admin(cq: CallbackQuery):
    if cq.from_user.username == ADMIN_USERNAME or cq.from_user.id in ADMIN_IDS:
        # добавим в локальный набор премиум (опционально)
        await cq.message.answer("✅ Ты теперь админ и премиум!")
    else:
        await cq.message.answer("❌ Только владелец бота может это сделать.")
    await cq.answer()

# ----------------- Очередь загрузок -----------------
async def enqueue_download(job: DownloadJob):
    async with queue_lock:
        if job.premium_level == "алмазный":
            download_queue.appendleft(job)
        else:
            download_queue.append(job)
    logger.info("Job queued: %s", job)

def run_yt_dlp_blocking(url: str, outdir: str, fmt: str):
    opts = YDL_COMMON_OPTS.copy()
    opts.update({
        "format": fmt,
        "outtmpl": os.path.join(outdir, "%(id)s.%(ext)s"),
        # не добавляем merge_output_format, чтобы избежать требований ffmpeg
    })
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        filename = ydl.prepare_filename(info)
    return filename, info

async def download_worker():
    logger.info("Download worker started")
    loop = asyncio.get_event_loop()

    # общая сессия для загрузки миниатюр / API
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
                fmt = YDL_FORMATS["diamond"] if job.premium_level == "алмазный" else YDL_FORMATS["normal"]

                filename = None
                info = {}

                # TikTok
                if "tiktok" in job.url or "vm.tiktok" in job.url:
                    try:
                        filename = await download_tiktok(job.url, session=session)
                        info = {}
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
                else:
                    # YouTube path
                    def blocking():
                        return run_yt_dlp_blocking(job.url, tmpdir, fmt)
                    try:
                        filename, info = await loop.run_in_executor(None, blocking)
                    except Exception as e:
                        logger.exception("Download error for %s", job.url)
                        try:
                            await bot.send_message(job.chat_id, f"❌ Ошибка при скачивании: {e}")
                        except Exception:
                            pass
                        continue

                # thumbnail
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
                                shutil.rmtree(parent)
                        except Exception:
                            pass
                else:
                    try:
                        await bot.send_message(job.chat_id, "❌ Файл не найден после скачивания.")
                    except Exception:
                        pass
            finally:
                try:
                    shutil.rmtree(tmpdir)
                except Exception:
                    pass
            await asyncio.sleep(0.2)

# ----------------- Обработка входящих сообщений -----------------
async def process_incoming_link(user_id: int, chat_id: int, link: str, msg_obj: Optional[Message] = None):
    # Сохраняем последнюю ссылку
    last_links[user_id] = link
    await ensure_user(user_id, None)
    row = await get_user_row(user_id)
    premium_level = row[2] if row else "обычный"

    if not await can_user_download(user_id):
        if msg_obj:
            await msg_obj.answer("❌ Лимит скачиваний на сегодня исчерпан.")
        else:
            await bot.send_message(chat_id, "❌ Лимит скачиваний на сегодня исчерпан.")
        return

    job = DownloadJob(id=str(uuid.uuid4()), user_id=user_id, chat_id=chat_id, url=link, premium_level=premium_level, request_time=time.time())
    await enqueue_download(job)

    if msg_obj:
        await msg_obj.answer("⏳ Загрузка началась, подождите...")
    else:
        await bot.send_message(chat_id, "⏳ Загрузка началась, подождите...")

@dp.message()
async def handle_message(msg: Message):
    user_id = msg.from_user.id
    text = (msg.text or "").strip()

    is_link = any(x in text for x in ("youtube.com", "youtu.be", "tiktok.com", "vm.tiktok"))
    if is_link:
        await process_incoming_link(user_id, msg.chat.id, text, msg)
        return

    # если пользователь ранее нажал /download и теперь отправляет ссылку — обработаем
    if awaiting_link.get(user_id):
        awaiting_link[user_id] = False
        if is_link:
            await process_incoming_link(user_id, msg.chat.id, text, msg)
        else:
            await msg.answer("❌ Пожалуйста, отправь ссылку на YouTube Shorts или TikTok.")
        return

    # иначе — подсказка
    await msg.answer("Нажми «Скачать видео» или используй /download. Для справки /about", reply_markup=main_buttons())

# ----------------- TikTok downloader -----------------
async def download_tiktok(url: str, session: Optional[aiohttp.ClientSession] = None):
    temp_dir = tempfile.mkdtemp(prefix="tt_dl_")
    out_file = os.path.join(temp_dir, "video.mp4")
    loop = asyncio.get_event_loop()

    def run_ydl():
        ydl_opts = {
            "format": "best[ext=mp4]/best",
            "outtmpl": os.path.join(temp_dir, "%(id)s.%(ext)s"),
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "http_headers": {"User-Agent": "Mozilla/5.0"},
        }
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filename = ydl.prepare_filename(info)
            return filename

    try:
        filename = await loop.run_in_executor(None, run_ydl)
        if filename and os.path.exists(filename):
            return filename
    except Exception as e:
        logger.debug("yt-dlp failed for TikTok: %s", e)

    api = f"https://api.tikwm.com/?url={url}"
    own_session = False
    if session is None:
        session = aiohttp.ClientSession()
        own_session = True

    try:
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

# ----------------- Запуск -----------------
async def main():
    await init_db()
    await register_commands()
    # старт workers
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