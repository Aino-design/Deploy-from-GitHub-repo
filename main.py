# main.py
"""
Телеграм-бот:
- поддержка TikTok (включая фото-посты: скачивание картинок + отдельно музыка)
- поддержка Instagram (через yt-dlp)
- YouTube заблокирован (бот отвечает понятным сообщением)
- UI: Профиль, Скачать видео, О боте, Премиум
- Админ: /grant_premium <user_id> <обычный|золотой|алмазный>
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
from typing import Optional, Dict, Tuple, List

import aiosqlite
import aiohttp
from yt_dlp import YoutubeDL

from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup,
    FSInputFile, BotCommand, InputMediaPhoto
)

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")  # <- ВАЖНО: вставь токен сюда
if not TOKEN or TOKEN.startswith("PASTE_"):
    raise SystemExit("ERROR: Вставь токен в переменную TOKEN в main.py")

DB_PATH = "bot_users.db"
DOWNLOAD_WORKERS = 1
LOG_LEVEL = logging.INFO

# admin ids — укажи свои id если нужно
ADMIN_IDS = [6705555401]  # замените на свои id или оставьте []

# limits by premium level
LIMITS = {"обычный": 4, "золотой": 10, "алмазный": None}  # None = unlimited

# yt-dlp settings
YDL_FORMAT = "best[ext=mp4]/best"
COOKIES_FILE = "cookies.txt" if os.path.exists("cookies.txt") else None
FFMPEG_LOCATION = None  # укажи путь, если нужно

# Logging
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------- Bot / Dispatcher ----------------
bot = Bot(token=TOKEN)
dp = Dispatcher()

# ---------------- Data types ----------------
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

# ---------------- Database helpers ----------------
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
        "❌ YouTube не поддерживается — при отправке YouTube-ссылки я сразу сообщу.\n\n"
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
        "- золотой: 10 видео/день\n        - алмазный: неограниченно + приоритет\n\n"
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

# ---------------- yt-dlp helpers ----------------
class YouTubeNotSupported(Exception):
    pass

def is_youtube_url(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    return "youtube.com" in u or "youtu.be" in u

def is_tiktok_url(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    return "tiktok.com" in u or "vm.tiktok" in u

def is_instagram_url(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    return "instagram.com" in u or "instagr.am" in u

def run_yt_dlp_blocking(url: str, outdir: str, ydl_format: Optional[str] = None) -> Tuple[str, dict]:
    """
    Blocking call to yt-dlp that downloads and returns (filename, info).
    Raises YouTubeNotSupported if URL is YouTube.
    """
    if is_youtube_url(url):
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

# ---------------- TikTok handler (video & photo-post) ----------------
async def download_tiktok(url: str) -> dict:
    """
    Returns dict:
      { "type": "video", "file": "/path/to/video.mp4" }
    or
      { "type": "photos", "images": [url1, url2,...], "audio_file": "/tmp/audio.ext" or None }
    """
    temp_dir = tempfile.mkdtemp(prefix="tt_dl_")
    loop = asyncio.get_event_loop()

    # Step 1: extract info (no download) to detect type
    def extract_info():
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "skip_download": True,
            "http_headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        }
        with YoutubeDL(ydl_opts) as ydl:
            return ydl.extract_info(url, download=False)

    try:
        info = await loop.run_in_executor(None, extract_info)
    except Exception as e:
        # propagate for caller to handle
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass
        raise

    # Heuristic: treat as video if formats available or ext is mp4 or duration present
    is_video = False
    if isinstance(info, dict):
        if info.get("formats") or info.get("ext") == "mp4" or info.get("duration"):
            is_video = True

    if is_video:
        # download video via yt-dlp
        def download_video():
            ydl_opts = {
                "format": "best[ext=mp4]/best",
                "outtmpl": os.path.join(temp_dir, "%(id)s.%(ext)s"),
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
                data = ydl.extract_info(url, download=True)
                return ydl.prepare_filename(data)

        try:
            filename = await loop.run_in_executor(None, download_video)
            return {"type": "video", "file": filename, "tmpdir": temp_dir}
        except Exception:
            # cleanup and re-raise
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass
            raise

    # Otherwise: treat as photo-post (slide show)
    images: List[str] = []
    # collect thumbnails (best-effort)
    thumbs = info.get("thumbnails") or []
    for t in thumbs:
        url_t = t.get("url") if isinstance(t, dict) else None
        if url_t:
            images.append(url_t)

    # additional attempts: some extractors provide 'display_id' or 'requested_formats' etc.
    # fallback: try to look for 'image' fields or 'entries' with thumbnails
    if not images:
        # if entries exist (sometimes playlist-style), collect thumbnails
        entries = info.get("entries") or []
        for e in entries:
            e_thumbs = e.get("thumbnails") or []
            for t in e_thumbs:
                u = t.get("url") if isinstance(t, dict) else None
                if u:
                    images.append(u)

    # Try to download audio (bestaudio) separately — best-effort
    audio_file = None
    try:
        def download_audio():
            ydl_opts = {
                "format": "bestaudio",
                "outtmpl": os.path.join(temp_dir, "audio.%(ext)s"),
                "quiet": True,
                "no_warnings": True,
                "noplaylist": True,
                "http_headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            }
            if COOKIES_FILE:
                ydl_opts["cookiefile"] = COOKIES_FILE
            with YoutubeDL(ydl_opts) as ydl:
                info2 = ydl.extract_info(url, download=True)
                # prepare_filename returns something like /tmp/audio.m4a or .webm
                return ydl.prepare_filename(info2)
        audio_file = await loop.run_in_executor(None, download_audio)
    except Exception:
        audio_file = None  # it's fine if audio couldn't be downloaded

    # if still no images found, try to parse some image urls from info fields
    if not images:
        # check common keys
        for key in ("image", "thumbnail", "cover", "poster"):
            v = info.get(key)
            if isinstance(v, str) and v.startswith("http"):
                images.append(v)

    # Finalize. If absolutely no images and no audio, treat as error
    if not images and not audio_file:
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass
        raise RuntimeError("Не удалось обнаружить фото/аудио в этом TikTok-посте.")

    return {"type": "photos", "images": images, "audio_file": audio_file, "tmpdir": temp_dir}

# ---------------- TikTok fallback (old API) - kept optional but not used directly here ----------------
async def download_tiktok_fallback(url: str, session: Optional[aiohttp.ClientSession] = None) -> str:
    temp_dir = tempfile.mkdtemp(prefix="tt_fallback_")
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
        raise Exception(f"TikTok fallback failed: {e}")
    finally:
        if own_session:
            await session.close()

# ---------------- Download worker ----------------
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

            # Quick YouTube safety
            if is_youtube_url(job.url):
                try:
                    await bot.send_message(job.chat_id, "❌ Этот бот не может загружать YouTube видео.")
                except Exception:
                    pass
                continue

            # Prepare tmpdir for this job (many functions return tmpdir themselves)
            tmpdir_job = tempfile.mkdtemp(prefix="job_")
            try:
                # TikTok
                if is_tiktok_url(job.url):
                    try:
                        res = await download_tiktok(job.url)
                    except YouTubeNotSupported:
                        await bot.send_message(job.chat_id, "❌ Этот бот не может загружать YouTube видео.")
                        continue
                    except Exception as e:
                        logger.exception("TikTok download error for %s: %s", job.url, e)
                        try:
                            await bot.send_message(job.chat_id, f"❌ Ошибка при скачивании TikTok: {e}")
                        except Exception:
                            pass
                        continue

                    if res["type"] == "video":
                        filename = res["file"]
                        try:
                            await bot.send_chat_action(job.chat_id, "upload_video")
                            await bot.send_video(job.chat_id, video=FSInputFile(filename), supports_streaming=True)
                            await bot.send_message(job.chat_id, "✅ Готово!")
                            await increment_download(job.user_id)
                        except Exception as e:
                            logger.exception("Failed to send video")
                            try:
                                await bot.send_message(job.chat_id, f"❌ Ошибка отправки видео: {e}")
                            except Exception:
                                pass
                        finally:
                            # cleanup
                            try:
                                parent = os.path.dirname(filename)
                                if parent and parent.startswith(tempfile.gettempdir()):
                                    shutil.rmtree(parent, ignore_errors=True)
                            except Exception:
                                pass

                    elif res["type"] == "photos":
                        images = res.get("images", [])[:10]  # limit 10
                        audio_file = res.get("audio_file")
                        tmpdir_from = res.get("tmpdir")

                        # Download images to temp files and send as media group
                        media = []
                        local_paths = []
                        try:
                            for idx, img_url in enumerate(images):
                                try:
                                    async with session.get(img_url, timeout=20) as r:
                                        if r.status == 200:
                                            local = os.path.join(tempfile.gettempdir(), f"tt_{uuid.uuid4().hex}.jpg")
                                            with open(local, "wb") as f:
                                                f.write(await r.read())
                                            local_paths.append(local)
                                            media.append(InputMediaPhoto(media=FSInputFile(local)))
                                except Exception as e:
                                    logger.debug("Failed to download image %s : %s", img_url, e)
                            if media:
                                # send_media_group expects list of InputMedia*
                                await bot.send_media_group(job.chat_id, media)
                            else:
                                await bot.send_message(job.chat_id, "📸 Это TikTok-пост с фотографиями, но не удалось скачать превью-изображения.")
                            # send audio if exists
                            if audio_file and os.path.exists(audio_file):
                                try:
                                    await bot.send_message(job.chat_id, "🎵 Музыка из поста:")
                                    await bot.send_audio(job.chat_id, FSInputFile(audio_file))
                                except Exception:
                                    logger.exception("Failed to send audio file")
                            await increment_download(job.user_id)
                        finally:
                            # cleanup local image files and tmpdir_from
                            for p in local_paths:
                                try:
                                    os.remove(p)
                                except Exception:
                                    pass
                            try:
                                if tmpdir_from and os.path.exists(tmpdir_from):
                                    shutil.rmtree(tmpdir_from, ignore_errors=True)
                            except Exception:
                                pass

                    else:
                        # unknown type
                        await bot.send_message(job.chat_id, "❌ Неизвестный формат TikTok-поста.")
                        continue

                # Instagram
                elif is_instagram_url(job.url):
                    try:
                        # download using yt-dlp blocking call
                        filename, info = await loop.run_in_executor(None, run_yt_dlp_blocking, job.url, tmpdir_job, None)
                    except YouTubeNotSupported:
                        await bot.send_message(job.chat_id, "❌ Этот бот не может загружать YouTube видео.")
                        continue
                    except Exception as e:
                        logger.exception("Instagram download error for %s: %s", job.url, e)
                        try:
                            await bot.send_message(job.chat_id, f"❌ Ошибка при скачивании Instagram: {e}")
                        except Exception:
                            pass
                        try:
                            shutil.rmtree(tmpdir_job, ignore_errors=True)
                        except Exception:
                            pass
                        continue

                    # send file (video or image)
                    if filename and os.path.exists(filename):
                        try:
                            # try send as video first
                            await bot.send_chat_action(job.chat_id, "upload_video")
                            await bot.send_video(job.chat_id, video=FSInputFile(filename), supports_streaming=True)
                            await bot.send_message(job.chat_id, "✅ Готово!")
                            await increment_download(job.user_id)
                        except Exception:
                            # try as document
                            try:
                                await bot.send_document(job.chat_id, FSInputFile(filename))
                            except Exception:
                                await bot.send_message(job.chat_id, "❌ Ошибка отправки файла.")
                        finally:
                            try:
                                parent = os.path.dirname(filename)
                                if parent and parent.startswith(tempfile.gettempdir()):
                                    shutil.rmtree(parent, ignore_errors=True)
                            except Exception:
                                pass
                    else:
                        await bot.send_message(job.chat_id, "❌ Файл не найден после скачивания.")
                        continue

                else:
                    # unsupported site
                    try:
                        await bot.send_message(job.chat_id, "❌ Этот бот не может загружать видео с этого сайта.")
                    except Exception:
                        pass
                    continue

            finally:
                try:
                    shutil.rmtree(tmpdir_job, ignore_errors=True)
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
    if is_youtube_url(link):
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

    # detect whether message contains a known link
    is_link = any(x in text for x in ("youtube.com", "youtu.be", "tiktok.com", "vm.tiktok", "instagram.com", "instagr.am"))
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