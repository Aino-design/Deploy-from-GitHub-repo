import yt_dlp
import instaloader
import requests
import os

def download_youtube(url):
    ydl_opts = {
        'format': 'bestvideo+bestaudio/best',
        'outtmpl': 'downloads/%(title)s.%(ext)s'
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        filename = ydl.prepare_filename(info)
    return filename

def download_instagram(url):
    L = instaloader.Instaloader(download_videos=True, download_comments=False, save_metadata=False)
    post = instaloader.Post.from_shortcode(L.context, url.split("/")[-2])
    L.download_post(post, target='downloads')
    return f'downloads/{post.owner_username}_{post.shortcode}.mp4'

def download_tiktok(url):
    # Простейший вариант через сторонний API
    r = requests.get(f"https://api.tiktokdownloader.com/download?url={url}")
    filename = f"downloads/{url.split('/')[-1]}.mp4"
    with open(filename, "wb") as f:
        f.write(r.content)
    return filename