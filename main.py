# main.py
"""
Полный Telegram-бот:
- скачивает TikTok (включая фото-посты: скачивает картинки + музыку)
- скачивает Instagram (yt-dlp)
- YouTube жёстко заблокирован (чистое сообщение пользователю)
- UI: Профиль, Скачать видео, О боте, Премиум
- Магазин премиума через Telegram Stars (currency="XTR", provider_token="")
- База SQLite: хранит premium и premium_expires
- Аккуратная очистка временных файлов и обработка ошибок
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
from typing import Optional, Dict, Tuple, List

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
TOKEN = os.getenv("TOKEN")  # <- вставь токен бота сюда
if not TOKEN or TOKEN.startswith("PASTE_"):
    raise SystemExit("ERROR: Вставь реальный токен в TOKEN в main.py")

# этот id/список получит уведомление о каждой покупке (и будет 'владельцем' для логики)
ADMIN_IDS = [6705555401]  # <- поставь сюда свой числовой Telegram ID

# БД, воркеры, логирование
DB_PATH = "bot_users.db"
DOWNLOAD_WORKERS = 1
LOG_LEVEL = logging.INFO

# магазин / звёзды
STARS_PROVIDER_TOKEN = ""  # для Telegram Stars provider_token = "" (пустая строка)
STARS_CURRENCY = "XTR"

# yt-dlp / ffmpeg
YDL_FORMAT = "best[ext=mp4]/best"
COOKIES_FILE = "cookies.txt" if os.path.exists("cookies.txt") else None
FFMPEG_LOCATION = None  # можно указать путь к ffmpeg

# лимиты
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
        # safe migration: ensure column exists
        async with db.execute("PRAGMA table_info(users)") as cur:
            cols = await cur.fetchall()
        col_names = [c[1] for c in cols]
        if "premium_expires" not in col_names:
            try:
                await db.execute("ALTER TABLE users ADD COLUMN premium_expires TEXT")
                await db.commit()
            except Exception:
                logger.debug("Couldn't add premium_expires column (may already exist)")

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
    if days is not None:
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
    limit = LIMITS.get(premium, 4)
    return (limit is None) or (downloads_today < limit)

async def is_premium_active(user_id: int) -> Tuple[bool, str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT premium, premium_expires FROM users WHERE id=?", (user_id,)) as cur:
            r = await cur.fetchone()
    if not r:
        return False, "обычный"
    premium, premium_expires = r
    if premium_expires:
        try:
            if datetime.utcnow() < datetime.fromisoformat(premium_expires):
                return True, premium
            else:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("UPDATE users SET premium='обычный', premium_expires=NULL WHERE id=?", (user_id,))
                    await db.commit()
                return False, "обычный"
        except Exception:
            return False, premium or "обычный"
    else:
        if premium and premium != "обычный":
            return True, premium
        return False, "обычный"

# ---------------- UI / commands ----------------
def main_buttons() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🎬 Скачать видео", callback_data="download")],
        [InlineKeyboardButton(text="ℹ️ О боте", callback_data="about")],
        [InlineKeyboardButton(text="💎 Премиум", callback_data="premium")],
    ])

async def register_commands():
    try:
        await bot.set_my_commands([
            BotCommand(command="start", description="Главное меню"),
            BotCommand(command="profile", description="Профиль"),
            BotCommand(command="download", description="Скачать видео"),
            BotCommand(command="about", description="О боте"),
            BotCommand(command="premium", description="Информация о премиум"),
            BotCommand(command="grant_premium", description="(Админ) выдать премиум")
        ])
    except Exception:
        logger.exception("Could not set bot commands")

# ---------------- Helpers & detection ----------------
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
    return "tiktok.com" in u or "vm.tiktok" in u or "vt.tiktok.com" in u

def is_instagram_url(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    return "instagram.com" in u or "instagr.am" in u

def run_yt_dlp_blocking(url: str, outdir: str, ydl_format: Optional[str] = None) -> Tuple[str, dict]:
    if is_youtube_url(url):
        raise YouTubeNotSupported()
    ydl_opts = {
        "format": ydl_format or YDL_FORMAT,
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
        return filename, info

# ---------------- TikTok robust handler (video & photo-posts) ----------------
async def download_tiktok_content(url: str) -> dict:
    tmpdir = tempfile.mkdtemp(prefix="ttjob_")
    loop = asyncio.get_event_loop()

    def ydl_info_no_download():
        opts = {"quiet": True, "no_warnings": True, "noplaylist": True, "skip_download": True, "http_headers": {"User-Agent": "Mozilla/5.0"}}
        if COOKIES_FILE:
            opts["cookiefile"] = COOKIES_FILE
        with YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)

    info = None
    try:
        info = await loop.run_in_executor(None, ydl_info_no_download)
    except Exception as e:
        logger.debug("yt-dlp extract_info failed (fallback to HTML parse): %s", e)
        info = None

    # Video detection
    if isinstance(info, dict) and (info.get("formats") or info.get("ext") == "mp4" or info.get("duration")):
        try:
            def ydl_download():
                opts = {"format": "best[ext=mp4]/best", "outtmpl": os.path.join(tmpdir, "%(id)s.%(ext)s"), "quiet": True, "no_warnings": True, "noplaylist": True, "http_headers": {"User-Agent": "Mozilla/5.0"}}
                if COOKIES_FILE:
                    opts["cookiefile"] = COOKIES_FILE
                if FFMPEG_LOCATION:
                    opts["ffmpeg_location"] = FFMPEG_LOCATION
                with YoutubeDL(opts) as ydl:
                    data = ydl.extract_info(url, download=True)
                    return ydl.prepare_filename(data)
            filename = await loop.run_in_executor(None, ydl_download)
            return {"type": "video", "file": filename, "tmpdir": tmpdir}
        except Exception:
            shutil.rmtree(tmpdir, ignore_errors=True)
            raise

    # Fallback: parse page HTML/JSON for images & audio
    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, timeout=20, allow_redirects=True) as resp:
                html = await resp.text()
        except Exception as e:
            shutil.rmtree(tmpdir, ignore_errors=True)
            raise RuntimeError(f"Не удалось получить страницу TikTok: {e}")

    images_urls: List[str] = []
    audio_url: Optional[str] = None

    # JSON blobs search
    m = re.search(r"window\.__INITIAL_STATE__\s*=\s*({.+?});", html, flags=re.S) or \
        re.search(r"window\['SIGI_STATE'\]\s*=\s*({.+?});", html, flags=re.S) or \
        re.search(r"(\{.+\"ItemModule\":\s*\{.+\}\s*\}.+?)</script>", html, flags=re.S)

    if m:
        try:
            j = json.loads(m.group(1))
            item_module = None
            if "ItemModule" in j:
                item_module = j["ItemModule"]
            else:
                for key in ("props", "initialProps", "appProps"):
                    maybe = j.get(key) or {}
                    if isinstance(maybe, dict) and "ItemModule" in maybe:
                        item_module = maybe["ItemModule"]
                        break
            if item_module and isinstance(item_module, dict):
                first = next(iter(item_module.values()))
                for key in ("images", "imageList", "imageUrls", "image_list", "covers"):
                    val = first.get(key)
                    if val:
                        if isinstance(val, list):
                            for it in val:
                                if isinstance(it, dict):
                                    u = it.get("url") or it.get("uri")
                                    if isinstance(u, str):
                                        images_urls.append(u)
                                elif isinstance(it, str):
                                    images_urls.append(it)
                        elif isinstance(val, str):
                            images_urls.append(val)
                music = first.get("music") or first.get("musicInfo")
                if isinstance(music, dict):
                    audio_url = music.get("playUrl") or music.get("url") or music.get("audioUrl")
        except Exception:
            logger.debug("json parse failed", exc_info=True)

    # regex fallback
    if not images_urls:
        found = re.findall(r"https?://[^\s'\"<>]+?\.(?:jpe?g|png|webp)(?:\?[^\s'\"<>]*)?", html, flags=re.I)
        seen = set()
        for u in found:
            if u not in seen:
                seen.add(u)
                images_urls.append(u)

    if not audio_url:
        audio_matches = re.findall(r"https?://[^\s'\"<>]+?\.(?:mp3|m4a|aac|ogg)(?:\?[^\s'\"<>]*)?", html, flags=re.I)
        if audio_matches:
            audio_url = audio_matches[0]

    if not images_urls and not audio_url:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise RuntimeError("Не удалось найти изображения или аудио в этой странице TikTok (возможно приватный пост).")

    # download images (limit)
    local_images: List[str] = []
    max_images = 20
    async with aiohttp.ClientSession() as session:
        for i, img_u in enumerate(images_urls[:max_images]):
            try:
                async with session.get(img_u, timeout=20) as r:
                    if r.status == 200:
                        ext = ".jpg"
                        ct = r.headers.get("Content-Type", "")
                        if "png" in ct: ext = ".png"
                        elif "webp" in ct: ext = ".webp"
                        local = os.path.join(tmpdir, f"img_{i}_{uuid.uuid4().hex}{ext}")
                        with open(local, "wb") as f:
                            f.write(await r.read())
                        local_images.append(local)
            except Exception as e:
                logger.debug("image download failed %s : %s", img_u, e)

    # download audio
    local_audio = None
    if audio_url:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(audio_url, timeout=30) as r:
                    if r.status == 200:
                        ext = ".mp3"
                        ct = r.headers.get("Content-Type", "")
                        if "mpeg" in ct or "mp3" in ct: ext = ".mp3"
                        elif "m4a" in ct or "aac" in ct: ext = ".m4a"
                        elif "ogg" in ct: ext = ".ogg"
                        local_audio = os.path.join(tmpdir, "audio" + ext)
                        with open(local_audio, "wb") as f:
                            f.write(await r.read())
        except Exception as e:
            logger.debug("audio download failed %s : %s", audio_url, e)
            local_audio = None

    if not local_images and images_urls:
        # couldn't download locally — return URLs
        return {"type": "photos_urls", "images": images_urls, "audio_url": audio_url, "tmpdir": tmpdir}

    return {"type": "photos", "images": local_images, "audio_file": local_audio, "tmpdir": tmpdir}

# ---------------- Download queue worker ----------------
async def enqueue_download(job: DownloadJob):
    async with queue_lock:
        if job.premium_level == "алмазный":
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

            logger.info("Processing job: %s", job)

            if not await can_user_download(job.user_id):
                try:
                    await bot.send_message(job.chat_id, "❌ Лимит скачиваний на сегодня достигнут.")
                except Exception:
                    logger.exception("notify error")
                continue

            # youtube safety
            if is_youtube_url(job.url):
                try:
                    await bot.send_message(job.chat_id, "❌ Этот бот не может загружать YouTube видео.")
                except Exception:
                    pass
                continue

            # TikTok
            if is_tiktok_url(job.url):
                try:
                    res = await download_tiktok_content(job.url)
                except Exception as e:
                    logger.exception("TikTok processing failed for %s: %s", job.url, e)
                    try:
                        await bot.send_message(job.chat_id, f"❌ Ошибка при скачивании TikTok: {e}")
                    except Exception:
                        pass
                    continue

                if res.get("type") == "video":
                    filename = res.get("file")
                    try:
                        await bot.send_chat_action(job.chat_id, "upload_video")
                        await bot.send_video(job.chat_id, video=FSInputFile(filename), supports_streaming=True)
                        await bot.send_message(job.chat_id, "✅ Готово!")
                        await increment_download(job.user_id)
                    except Exception:
                        logger.exception("Failed to send video")
                        try:
                            await bot.send_message(job.chat_id, "❌ Ошибка отправки видео.")
                        except Exception:
                            pass
                    finally:
                        try:
                            parent = os.path.dirname(filename)
                            if parent and parent.startswith(tempfile.gettempdir()):
                                shutil.rmtree(parent, ignore_errors=True)
                        except Exception:
                            pass

                elif res.get("type") == "photos":
                    images = res.get("images", [])
                    audio_file = res.get("audio_file")
                    tmpdir_from = res.get("tmpdir")
                    media = []
                    try:
                        for p in images:
                            media.append(InputMediaPhoto(media=FSInputFile(p)))
                        if media:
                            for i in range(0, len(media), 10):
                                batch = media[i:i+10]
                                try:
                                    await bot.send_media_group(job.chat_id, batch)
                                except Exception:
                                    for mm in batch:
                                        try:
                                            await bot.send_photo(job.chat_id, mm.media)
                                        except Exception:
                                            pass
                        else:
                            await bot.send_message(job.chat_id, "📸 Это TikTok-пост с фотографиями, но не удалось собрать превью.")
                        if audio_file and os.path.exists(audio_file):
                            try:
                                await bot.send_message(job.chat_id, "🎵 Музыка из поста:")
                                await bot.send_audio(job.chat_id, FSInputFile(audio_file))
                            except Exception:
                                logger.exception("Failed to send audio")
                        await increment_download(job.user_id)
                    finally:
                        try:
                            if tmpdir_from and os.path.exists(tmpdir_from):
                                shutil.rmtree(tmpdir_from, ignore_errors=True)
                        except Exception:
                            pass

                elif res.get("type") == "photos_urls":
                    images = res.get("images", [])[:10]
                    audio_url = res.get("audio_url")
                    try:
                        for img in images:
                            try:
                                await bot.send_photo(job.chat_id, img)
                            except Exception:
                                logger.debug("Failed send photo by URL %s", img)
                        if audio_url:
                            try:
                                await bot.send_audio(job.chat_id, audio_url)
                            except Exception:
                                logger.debug("Failed send audio by URL %s", audio_url)
                        await increment_download(job.user_id)
                    finally:
                        try:
                            td = res.get("tmpdir")
                            if td and os.path.exists(td):
                                shutil.rmtree(td, ignore_errors=True)
                        except Exception:
                            pass

                else:
                    try:
                        await bot.send_message(job.chat_id, "❌ Неизвестный формат TikTok-поста.")
                    except Exception:
                        pass
                continue

            # Instagram
            if is_instagram_url(job.url):
                tmpdir_job = tempfile.mkdtemp(prefix="job_")
                try:
                    try:
                        filename, info = await asyncio.get_event_loop().run_in_executor(None, run_yt_dlp_blocking, job.url, tmpdir_job, None)
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

                    if filename and os.path.exists(filename):
                        try:
                            await bot.send_chat_action(job.chat_id, "upload_video")
                            await bot.send_video(job.chat_id, video=FSInputFile(filename), supports_streaming=True)
                            await bot.send_message(job.chat_id, "✅ Готово!")
                            await increment_download(job.user_id)
                        except Exception:
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
                finally:
                    try:
                        shutil.rmtree(tmpdir_job, ignore_errors=True)
                    except Exception:
                        pass
                continue

            # unsupported
            try:
                await bot.send_message(job.chat_id, "❌ Этот бот не может загружать видео с этого сайта.")
            except Exception:
                pass

# ---------------- Payments (Stars shop) ----------------
def build_price(label: str, stars_amount: int) -> List[LabeledPrice]:
    return [LabeledPrice(label=label, amount=stars_amount)]

@dp.callback_query(lambda c: c.data == "premium")
async def cb_premium(cq: CallbackQuery):
    await ensure_user(cq.from_user.id, cq.from_user.username)
    active, level = await is_premium_active(cq.from_user.id)
    if active:
        text = f"У тебя уже активен премиум: {level}."
    else:
        text = "Выбери тариф премиума и оплати звёздами."

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Купить Золотой — 100 ⭐ (30 дней)", callback_data="buy_gold")],
        [InlineKeyboardButton(text="Купить Алмазный — 300 ⭐ (30 дней)", callback_data="buy_diamond")],
        [InlineKeyboardButton(text="Назад", callback_data="menu_back")],
    ])
    await cq.message.answer(text, reply_markup=kb)
    await cq.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("buy_"))
async def cb_buy(cq: CallbackQuery):
    data = cq.data
    if data == "buy_gold":
        label = "Золотой премиум (30 дней)"
        days = 30
        amount = 100
        payload = f"premium:gold:{cq.from_user.id}:{days}:{uuid.uuid4().hex}"
    elif data == "buy_diamond":
        label = "Алмазный премиум (30 дней)"
        days = 30
        amount = 300
        payload = f"premium:diamond:{cq.from_user.id}:{days}:{uuid.uuid4().hex}"
    else:
        await cq.answer("Неизвестный тариф", show_alert=True)
        return

    prices = build_price(label, amount)
    try:
        await bot.send_invoice(
            chat_id=cq.from_user.id,
            title=label,
            description=f"Покупка {label}",
            payload=payload,
            provider_token=STARS_PROVIDER_TOKEN,  # empty string for Stars
            currency=STARS_CURRENCY,
            prices=prices,
            start_parameter="premium-buy"
        )
        await cq.answer()
    except Exception as e:
        logger.exception("Failed to send invoice: %s", e)
        await cq.answer("Не удалось показать счёт. Попробуй позже.", show_alert=True)

@dp.pre_checkout_query()
async def process_pre_checkout(pre: PreCheckoutQuery):
    try:
        await bot.answer_pre_checkout_query(pre.id, ok=True)
    except Exception:
        logger.exception("pre_checkout error")

@dp.message()
async def handle_successful_payment(msg: Message):
    sp = getattr(msg, "successful_payment", None)
    if not sp:
        return  # not a payment message — other handlers will handle
    payload = sp.invoice_payload
    try:
        parts = payload.split(":")
        if parts[0] == "premium" and len(parts) >= 5:
            _, level_key, intended_user_id, days_str, rnd = parts[:5]
            if int(intended_user_id) != msg.from_user.id:
                await msg.answer("Ошибка: ID плательщика не совпадает с получателем премиума.")
                return
            days = int(days_str)
            level_name = "золотой" if level_key == "gold" else ("алмазный" if level_key == "diamond" else level_key)
            await set_premium(msg.from_user.id, level_name, days=days)
            await msg.answer(f"✅ Оплата принята! Тебе выдан премиум: {level_name} на {days} дней.")
            logger.info("User %s bought %s for %s days", msg.from_user.id, level_name, days)
            # notify admin(s)
            for aid in ADMIN_IDS:
                try:
                    await bot.send_message(aid, f"Пользователь @{msg.from_user.username or msg.from_user.id} купил {level_name} на {days} дней.")
                except Exception:
                    pass
        else:
            await msg.answer("Оплата принята. Спасибо!")
    except Exception as e:
        logger.exception("Handling successful payment error: %s", e)
        await msg.answer("Оплата прошла, но возникла ошибка при выдаче премиума. Свяжись с админом.")

# ---------------- Handlers & UI (start / profile / download / about) ----------------
@dp.message(CommandStart())
async def start_handler(msg: Message):
    await ensure_user(msg.from_user.id, msg.from_user.username)
    await msg.answer(
        "Привет! 👋\n\n"
        "Я скачиваю медиа из TikTok и Instagram (Reels / посты / IGTV).\n\n"
        "Отправь ссылку на TikTok или Instagram либо нажми «Скачать видео».",
        reply_markup=main_buttons()
    )

@dp.message(Command("profile"))
async def cmd_profile(msg: Message):
    await ensure_user(msg.from_user.id, msg.from_user.username)
    row = await get_user_row(msg.from_user.id)
    if row:
        _, username, premium, downloads_today, last_reset, premium_expires = row
        exp_text = premium_expires or "нет"
        await msg.answer(f"👤 Профиль\nЮзер: @{username or msg.from_user.id}\nПремиум: {premium}\nИстекает: {exp_text}\nСкачиваний сегодня: {downloads_today}")
    else:
        await msg.answer("Профиль не найден. Нажми /start")

@dp.message(Command("about"))
async def cmd_about(msg: Message):
    await msg.answer("Этот бот скачивает TikTok и Instagram (через yt-dlp + обработка фото-постов).")

@dp.message(Command("premium"))
async def cmd_premium(msg: Message):
    await msg.answer("Открой меню премиума через кнопку 'Премиум' в главном меню.")

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
    if level not in LIMITS:
        await msg.answer("Неверный уровень премиума.")
        return
    await ensure_user(target_id, None)
    await set_premium(target_id, level, days=days)
    await msg.answer(f"✅ Премиум {level} выдан пользователю {target_id}.")
    try:
        await bot.send_message(target_id, f"Тебе выдали премиум: {level} (админ {msg.from_user.id})")
    except Exception:
        pass

@dp.callback_query(lambda c: c.data == "profile")
async def cb_profile(cq: CallbackQuery):
    await cmd_profile(cq.message)
    await cq.answer()

@dp.callback_query(lambda c: c.data == "about")
async def cb_about(cq: CallbackQuery):
    await cmd_about(cq.message)
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

# ---------------- Incoming messages and processing ----------------
async def process_incoming_link(user_id: int, chat_id: int, link: str, msg_obj: Optional[Message] = None):
    last_links[user_id] = link
    await ensure_user(user_id, None)
    row = await get_user_row(user_id)
    premium_level = row[2] if row else "обычный"

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
async def generic_message_handler(msg: Message):
    # first: successful_payment is handled by a specific handler above; if not, we process text links
    user_id = msg.from_user.id
    text = (msg.text or "").strip()

    is_link = any(x in text for x in ("tiktok.com", "vm.tiktok", "vt.tiktok.com", "instagram.com", "instagr.am", "youtube.com", "youtu.be"))
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

    await msg.answer("Нажми «Скачать видео» или отправь ссылку на TikTok / Instagram.", reply_markup=main_buttons())

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