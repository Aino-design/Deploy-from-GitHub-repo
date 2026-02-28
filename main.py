# main.py
"""
Переписанный бот:
- Поддерживает скачивание TikTok и Instagram (через yt-dlp, с резервной попыткой для TikTok).
- Явно блокирует YouTube: при отправке youtube.com / youtu.be бот отвечает чистым сообщением.
- Кнопка «🎲 Случайный TikTok» — скачивает/отправляет трендовое видео (best-effort через публичный API).
- Один /start handler, единый стиль ответов, аккуратная обработка ошибок и очистка временных файлов.
"""

import os
import asyncio
import tempfile
import shutil
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple
from collections import deque
import random

import aiohttp
from yt_dlp import YoutubeDL
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup,
    FSInputFile, BotCommand
)

# ---------------- Config ----------------
TOKEN = os.getenv("TOKEN")  # <- вставь токен
DB_PATH = "bot_users.db"  # (оставлено как есть, не используем БД в минимальной версии)
DOWNLOAD_WORKERS = 1
LOG_LEVEL = logging.INFO
ADMIN_IDS = [6705555401]  # если нужно - добавь id админов

# limits (необязательно, можно расширить)
LIMITS = {"обычный": 4, "золотой": 10, "алмазный": None}

# Опции yt-dlp
YDL_FORMAT = "best[ext=mp4]/best"
COOKIES_FILE = "cookies.txt" if os.path.exists("cookies.txt") else None
FFMPEG_LOCATION = None  # если нужно указать путь к ffmpeg на Windows

# Логирование
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------- Bot init ----------------
bot = Bot(token=TOKEN)
dp = Dispatcher()

# ---------------- Helpers ----------------

class YouTubeNotSupported(Exception):
    """Raised when a YouTube URL is encountered and downloads are blocked."""
    pass

async def send_user_message(chat_id: int, text: str):
    try:
        await bot.send_message(chat_id, text)
    except Exception:
        logger.exception("Failed to send user message")

def is_youtube_url(url: str) -> bool:
    u = (url or "").lower()
    return "youtube.com" in u or "youtu.be" in u

def is_tiktok_url(url: str) -> bool:
    u = (url or "").lower()
    return "tiktok.com" in u or "vm.tiktok" in u

def is_instagram_url(url: str) -> bool:
    u = (url or "").lower()
    return "instagram.com" in u or "instagr.am" in u

def run_yt_dlp_blocking(url: str, outdir: str, ydl_format: Optional[str] = None) -> Tuple[str, dict]:
    """
    Blocking call to yt-dlp that downloads the media to outdir and returns (filename, info).
    Raises YouTubeNotSupported for youtube links.
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

async def download_with_yt_dlp(url: str) -> Tuple[Optional[str], Optional[dict]]:
    """
    Wrapper that runs yt-dlp in executor and returns downloaded filename and info.
    """
    tmpdir = tempfile.mkdtemp(prefix="dl_")
    loop = asyncio.get_event_loop()
    try:
        def blocking():
            return run_yt_dlp_blocking(url, tmpdir, None)
        filename, info = await loop.run_in_executor(None, blocking)
        return filename, info
    except YouTubeNotSupported:
        # bubble up typed exception for caller
        raise
    except Exception as e:
        logger.exception("yt-dlp failed for %s: %s", url, e)
        # cleanup here
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass
        raise
    # caller must cleanup the file and tmpdir after use

async def download_tiktok_fallback(url: str, session: aiohttp.ClientSession) -> str:
    """
    Try a public API fallback to get the direct video url for TikTok (best-effort).
    Returns path to downloaded file.
    """
    tmpdir = tempfile.mkdtemp(prefix="tt_")
    out_file = os.path.join(tmpdir, "video.mp4")
    try:
        api = f"https://www.tikwm.com/api/?url={url}"
        async with session.get(api, timeout=20) as resp:
            if resp.status != 200:
                raise RuntimeError(f"API returned {resp.status}")
            data = await resp.json()
            video_url = (data.get("data") or {}).get("play") or (data.get("data") or {}).get("download")
            if not video_url:
                # try parse text for mp4
                text = await resp.text()
                import re
                urls = re.findall(r'https?://[^\s"\']+', text)
                candidates = [u for u in urls if ".mp4" in u or "v.tiktok" in u or "vm.tiktok" in u]
                video_url = candidates[0] if candidates else None
            if not video_url:
                raise RuntimeError("No video URL found in API response")

            async with session.get(video_url, timeout=60) as vf:
                if vf.status != 200:
                    raise RuntimeError(f"Video URL returned {vf.status}")
                with open(out_file, "wb") as f:
                    while True:
                        chunk = await vf.content.read(1024 * 32)
                        if not chunk:
                            break
                        f.write(chunk)
        if os.path.exists(out_file) and os.path.getsize(out_file) > 1000:
            return out_file
        else:
            raise RuntimeError("Downloaded file missing or too small")
    except Exception as e:
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass
        raise

# ---------------- UI / Keyboards ----------------

def main_buttons() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Скачать видео", callback_data="download")],
        [InlineKeyboardButton(text="🎲 Случайный TikTok", callback_data="random_tiktok")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
    ])
    return kb

# ---------------- Handlers ----------------

@dp.message(CommandStart())
async def start_handler(msg: Message):
    # Единственный обработчик /start
    await msg.answer(
        "Привет! 👋\n\n"
        "Я могу скачивать видео с TikTok и Instagram (Reels / посты / IGTV).\n\n"
        "❌ YouTube не поддерживается — если пришлёшь YouTube-ссылку, я сразу сообщу.\n\n"
        "Отправь ссылку на видео или нажми «Случайный TikTok».",
        reply_markup=main_buttons()
    )

@dp.message(Command("about"))
async def about_handler(msg: Message):
    await msg.answer("Этот бот скачивает видео с TikTok и Instagram. YouTube не поддерживается.")

@dp.callback_query(lambda c: c.data == "profile")
async def cb_profile(cq: CallbackQuery):
    await cq.message.answer("Профиль: временно не настроен. (Можно добавить лимиты/статистику).")
    await cq.answer()

# ---------------- Random TikTok ----------------

async def fetch_random_trending_tiktok(session: aiohttp.ClientSession) -> str:
    """
    Best-effort: получаем список трендовых видео (публично доступный источник) и возвращаем прямой mp4 URL.
    Здесь используем tikwm feed list endpoint if available; если не работает — бросаем.
    """
    # Попытка 1: tikwm feed list (many public bots use it)
    try:
        url = "https://www.tikwm.com/api/feed/list"
        async with session.get(url, timeout=15) as resp:
            if resp.status == 200:
                data = await resp.json()
                items = data.get("data") or []
                if not items:
                    raise RuntimeError("No items")
                choice = random.choice(items)
                # try extract play url
                play = choice.get("play") or choice.get("video") or None
                if play:
                    return play
    except Exception as e:
        logger.debug("tikwm feed/list failed: %s", e)

    # Попытка 2: использовать общий feed endpoint (legacy)
    try:
        url2 = "https://www.tikwm.com/api/feed/list?cursor=0"
        async with session.get(url2, timeout=15) as resp:
            if resp.status == 200:
                data = await resp.json()
                items = data.get("data") or []
                if items:
                    choice = random.choice(items)
                    play = choice.get("play")
                    if play:
                        return play
    except Exception as e:
        logger.debug("tikwm feed/list2 failed: %s", e)

    # Если ничего не удалось — ошибка
    raise RuntimeError("Не удалось получить трендовое видео TikTok (попробуй позже)")

@dp.callback_query(lambda c: c.data == "random_tiktok")
async def cb_random_tiktok(cq: CallbackQuery):
    await cq.answer()  # чтобы убрать крутилку
    await cq.message.answer("⏳ Ищу случайное трендовое видео TikTok...")
    async with aiohttp.ClientSession() as session:
        try:
            video_url_or_mp4 = await fetch_random_trending_tiktok(session)
            # если это прямой mp4, отправляем как видео ссылкой (Telegram поддерживает, но лучше скачать небольшой файл)
            # Пытаемся отправить как video по url
            try:
                await cq.message.answer_video(video_url_or_mp4)
            except Exception:
                # в случае проблем — скачиваем и отправляем как файл
                tmpfile = await download_tiktok_fallback(video_url_or_mp4, session)
                try:
                    await cq.message.answer_video(FSInputFile(tmpfile))
                finally:
                    try:
                        shutil.rmtree(os.path.dirname(tmpfile))
                    except Exception:
                        pass
        except Exception as e:
            logger.exception("random_tiktok error")
            await cq.message.answer(f"❌ Не удалось получить случайное видео: {e}")

# ---------------- Message handler (links) ----------------

# очередь/lock для ожидания ссылок (простая реализация)
awaiting_link: Dict[int, bool] = {}

@dp.callback_query(lambda c: c.data == "download")
async def cb_download_button(cq: CallbackQuery):
    user_id = cq.from_user.id
    awaiting_link[user_id] = True
    await cq.message.answer("📩 Отправь ссылку на TikTok или Instagram.")
    await cq.answer()

@dp.message()
async def handle_message(msg: Message):
    user_id = msg.from_user.id
    text = (msg.text or "").strip()

    # Если пользователь нажал кнопку и теперь присылает ссылку
    if awaiting_link.get(user_id):
        awaiting_link[user_id] = False
        # fallthrough: обработаем текст ниже

    # Detect YouTube first
    if is_youtube_url(text):
        await msg.answer("❌ Этот бот не может загружать YouTube видео.")
        return

    # TikTok
    if is_tiktok_url(text):
        await msg.answer("⏳ Скачивание TikTok (пытаемся через yt-dlp, затем fallback)...")
        # Сначала пытаемся yt-dlp
        tmpdir = tempfile.mkdtemp(prefix="job_")
        try:
            try:
                filename, info = await asyncio.get_event_loop().run_in_executor(None, run_yt_dlp_blocking, text, tmpdir, None)
            except YouTubeNotSupported:
                # на всякий случай - но мы уже проверяли
                await msg.answer("❌ Этот бот не может загружать YouTube видео.")
                return
            except Exception as e:
                logger.debug("yt-dlp failed for tiktok, trying fallback: %s", e)
                # fallback via public API
                async with aiohttp.ClientSession() as session:
                    try:
                        filename = await download_tiktok_fallback(text, session)
                        info = {}
                    except Exception as e2:
                        logger.exception("TikTok fallback failed: %s", e2)
                        await msg.answer("❌ Не удалось скачать TikTok (yt-dlp и fallback не сработали).")
                        return

            # send video
            if filename and os.path.exists(filename):
                try:
                    await msg.answer_video(FSInputFile(filename))
                except Exception:
                    # если отправка как video не сработала, отправим как документ
                    try:
                        await msg.answer_document(FSInputFile(filename))
                    except Exception:
                        await msg.answer("❌ Ошибка отправки файла.")
                finally:
                    # cleanup
                    try:
                        os.remove(filename)
                        parent = os.path.dirname(filename)
                        if parent and parent.startswith(tempfile.gettempdir()):
                            shutil.rmtree(parent, ignore_errors=True)
                    except Exception:
                        pass
        finally:
            # ensure tmpdir removed
            try:
                shutil.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                pass
        return

    # Instagram
    if is_instagram_url(text):
        await msg.answer("⏳ Скачивание Instagram (через yt-dlp)...")
        tmpdir = tempfile.mkdtemp(prefix="job_")
        try:
            try:
                filename, info = await asyncio.get_event_loop().run_in_executor(None, run_yt_dlp_blocking, text, tmpdir, None)
            except YouTubeNotSupported:
                await msg.answer("❌ Этот бот не может загружать YouTube видео.")
                return
            except Exception as e:
                logger.exception("Instagram download failed: %s", e)
                await msg.answer("❌ Не удалось скачать Instagram (ошибка yt-dlp).")
                return

            if filename and os.path.exists(filename):
                try:
                    await msg.answer_video(FSInputFile(filename))
                except Exception:
                    try:
                        await msg.answer_document(FSInputFile(filename))
                    except Exception:
                        await msg.answer("❌ Ошибка отправки файла.")
                finally:
                    try:
                        os.remove(filename)
                        parent = os.path.dirname(filename)
                        if parent and parent.startswith(tempfile.gettempdir()):
                            shutil.rmtree(parent, ignore_errors=True)
                    except Exception:
                        pass
        finally:
            try:
                shutil.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                pass
        return

    # Not a recognized link
    await msg.answer("⚠️ Отправь ссылку на TikTok или Instagram (или нажми «Случайный TikTok»).")

# ---------------- Run ----------------

async def main():
    # register bot commands shown in Telegram UI
    try:
        await bot.set_my_commands([
            BotCommand(command="start", description="Главное меню"),
            BotCommand(command="about", description="О боте"),
        ])
    except Exception:
        logger.exception("Couldn't set bot commands")

    logger.info("Bot starting polling")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down")