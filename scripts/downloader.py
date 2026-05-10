#!/usr/bin/env python3
"""
Hamechiz-Down v6 - Core Engine
Universal Media Downloader for GitHub Actions
"""

import os
import sys
import json
import shutil
import subprocess
from pathlib import Path
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import yt_dlp
from tenacity import retry, stop_after_attempt, wait_exponential

# ============================================================
# Configuration
# ============================================================
DOWNLOAD_DIR = Path("downloads")
MAX_WORKERS = 2  # GitHub runner محدود, موازی کامل مناسب نیست
COOKIE_FILE = Path.home() / ".cache/yt-dlp/cookies.txt"
QUEUE_FILE = Path("queue.json")

@dataclass
class DownloadJob:
    url: str
    mode: str
    quality: str = "best"
    subtitles: bool = False
    playlist: bool = False
    output_folder: str = "downloads"

# ============================================================
# Platform Handlers (Modular)
# ============================================================
class BaseHandler:
    def __init__(self, job: DownloadJob):
        self.job = job
        self.ydl_opts = self._base_opts()
    
    def _base_opts(self):
        opts = {
            'quiet': False,
            'no_warnings': False,
            'ignoreerrors': True,
            'retries': 10,
            'fragment_retries': 10,
            'sleep_interval': 2,
            'max_sleep_interval': 5,
            'concurrent_fragments': 4,
            'no_check_certificate': True,
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        }
        if COOKIE_FILE.exists():
            opts['cookiefile'] = str(COOKIE_FILE)
        return opts
    
    def download(self, url: str) -> bool:
        raise NotImplementedError

class YouTubeHandler(BaseHandler):
    def download(self, url: str) -> bool:
        opts = self.ydl_opts.copy()
        if self.job.quality == 'audio-only':
            opts['format'] = 'bestaudio/best'
            opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '0',
            }]
            opts['outtmpl'] = str(DOWNLOAD_DIR / '%(title)s.%(ext)s')
        else:
            if self.job.quality.isdigit():
                opts['format'] = f'bestvideo[height<={self.job.quality}]+bestaudio/best[height<={self.job.quality}]'
            else:
                opts['format'] = 'bestvideo+bestaudio/best'
            opts['merge_output_format'] = 'mp4'
            opts['outtmpl'] = str(DOWNLOAD_DIR / '%(title)s [%(id)s].%(ext)s')
        
        if self.job.subtitles:
            opts['writesubtitles'] = True
            opts['writeautomaticsub'] = True
            opts['subtitleslangs'] = ['en', 'fa']
            opts['embedsubs'] = True
        
        if self.job.playlist:
            pass  # yt-dlp default handles playlist
        else:
            opts['noplaylist'] = True
        
        opts['embedthumbnail'] = True
        opts['embedsubs'] = True
        opts['embedmetadata'] = True
        
        with yt_dlp.YoutubeDL(opts) as ydl:
            try:
                ydl.download([url])
                return True
            except Exception as e:
                print(f"Error downloading {url}: {e}")
                return False

class DirectHandler(BaseHandler):
    def download(self, url: str) -> bool:
        import requests
        filename = url.split('/')[-1].split('?')[0] or 'downloaded_file'
        filepath = DOWNLOAD_DIR / filename
        try:
            r = requests.get(url, stream=True, timeout=60)
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            return True
        except Exception as e:
            print(f"Direct download failed: {e}")
            return False

class InstagramHandler(BaseHandler):
    def download(self, url: str) -> bool:
        opts = self.ydl_opts.copy()
        opts['outtmpl'] = str(DOWNLOAD_DIR / '%(title)s.%(ext)s')
        opts['noplaylist'] = True
        with yt_dlp.YoutubeDL(opts) as ydl:
            try:
                ydl.download([url])
                return True
            except:
                return False

# ----------------------------------------------------------------------
# similar handlers for: Tiktok, Twitter, Reddit, SoundCloud, Telegram, GooglePlay, Webpage
# برای اختصار در اینجا فقط نمونه نوشته شده، اما در کد نهایی همه پیاده‌سازی می‌شوند.
# ----------------------------------------------------------------------

# ============================================================
# Factory
# ============================================================
def get_handler(job: DownloadJob):
    mode = job.mode.lower()
    if mode == 'youtube':
        return YouTubeHandler(job)
    elif mode == 'direct':
        return DirectHandler(job)
    elif mode == 'instagram':
        return InstagramHandler(job)
    # اضافه کردن سایر پلتفرم‌ها
    else:
        # fallback: yt-dlp generic
        return YouTubeHandler(job)

# ============================================================
# Queue Manager (برای repository_dispatch)
# ============================================================
class QueueManager:
    def __init__(self):
        self.queue_file = QUEUE_FILE
        self._init_queue()
    
    def _init_queue(self):
        if not self.queue_file.exists():
            self.queue_file.write_text(json.dumps([]))
    
    def add_job(self, job: Dict):
        queue = json.loads(self.queue_file.read_text())
        queue.append(job)
        self.queue_file.write_text(json.dumps(queue))
    
    def get_next_job(self):
        queue = json.loads(self.queue_file.read_text())
        if queue:
            return queue.pop(0)
        return None

# ============================================================
# Main Orchestrator
# ============================================================
def parse_input():
    """Reads input from environment variables or client_payload"""
    job = DownloadJob(url="", mode="auto")
    
    # Check for repository_dispatch payload
    client_payload = os.getenv("CLIENT_PAYLOAD")
    if client_payload:
        payload = json.loads(client_payload)
        job.url = payload.get("url", "")
        job.mode = payload.get("mode", "auto")
        job.quality = payload.get("quality", "best")
        job.subtitles = payload.get("subtitles", False)
        job.playlist = payload.get("playlist", False)
        output_folder = payload.get("output_folder", "downloads")
        job.output_folder = output_folder
        # support multiple urls
        urls = payload.get("urls", [])
        if urls:
            return [DownloadJob(url=u, mode=job.mode, quality=job.quality,
                                subtitles=job.subtitles, playlist=job.playlist,
                                output_folder=job.output_folder) for u in urls]
        return [job]
    
    # Otherwise from workflow_dispatch or commit
    mode = os.getenv("INPUT_MODE", "auto")
    urls_text = os.getenv("INPUT_URLS", "")
    if urls_text:
        urls = [u.strip() for u in urls_text.split('\n') if u.strip()]
    else:
        # Fallback: read from last commit message
        commit_msg = subprocess.getoutput("git log -1 --pretty=%B")
        import re
        urls = re.findall(r'https?://[^\s]+', commit_msg)
        if not urls:
            print("No URL provided. Exiting.")
            sys.exit(0)
    
    jobs = []
    for url in urls:
        job = DownloadJob(url=url, mode=mode,
                          quality=os.getenv("INPUT_QUALITY", "best"),
                          subtitles=os.getenv("INPUT_SUBTITLES", "false").lower() == "true",
                          playlist=os.getenv("INPUT_PLAYLIST", "false").lower() == "true",
                          output_folder=os.getenv("OUTPUT_MODE", "downloads"))
        jobs.append(job)
    return jobs

def auto_detect_mode(url: str) -> str:
    if 'youtube.com' in url or 'youtu.be' in url:
        return 'youtube'
    if 'instagram.com' in url:
        return 'instagram'
    if 'tiktok.com' in url:
        return 'tiktok'
    if 'twitter.com' in url or 'x.com' in url:
        return 'twitter'
    if 'reddit.com' in url:
        return 'reddit'
    if 'soundcloud.com' in url:
        return 'soundcloud'
    if 't.me' in url:
        return 'telegram'
    if 'play.google.com' in url:
        return 'googleplay'
    return 'direct'

def run_job(job: DownloadJob):
    if job.mode == 'auto':
        job.mode = auto_detect_mode(job.url)
    print(f"🔄 Processing [{job.mode}] {job.url}")
    handler = get_handler(job)
    success = handler.download(job.url)
    if success:
        print(f"✅ Success: {job.url}")
    else:
        print(f"❌ Failed: {job.url}")
    return success

def split_large_files():
    """Split files >90MB into zip chunks (90MB each)"""
    limit = 90 * 1024 * 1024
    for file in DOWNLOAD_DIR.glob("*"):
        if file.is_file() and file.stat().st_size > limit:
            name = file.stem
            safe_name = "".join(c if c.isalnum() else '-' for c in name).strip('-')
            split_dir = DOWNLOAD_DIR / safe_name
            split_dir.mkdir(exist_ok=True)
            shutil.move(str(file), str(split_dir / file.name))
            subprocess.run(["zip", "-s", "90m", "-r", f"{safe_name}.zip", file.name],
                           cwd=split_dir, check=True)
            (split_dir / file.name).unlink()
            print(f"✂️ Split {file.name} into {split_dir}/")

def upload_to_repo():
    """Commit and push files to repository"""
    subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=False)
    subprocess.run(["git", "config", "user.email", "github-actions[bot]@github.com"], check=False)
    subprocess.run(["git", "add", "downloads/"], check=False)
    subprocess.run(["git", "commit", "-m", "🎬 Downloaded via Hamechiz-Down v6 [skip ci]"], check=False)
    subprocess.run(["git", "push"], check=False)

def main():
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    jobs = parse_input()
    if not jobs:
        print("No jobs to process.")
        return
    
    # Queue management: اگر در حال اجرا بودن، job را به صف اضافه کن
    qm = QueueManager()
    # (در این نسخه ساده، بدون صف اضافی مستقیماً اجرا می‌کنیم)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(run_job, job): job for job in jobs}
        for future in as_completed(futures):
            job = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Job failed: {job.url} - {e}")
    
    split_large_files()
    
    output_mode = os.getenv("OUTPUT_MODE", "repo")
    if output_mode == "repo":
        upload_to_repo()
    elif output_mode == "artifact":
        print("Artifact will be uploaded by GitHub Action step.")
    elif output_mode == "release":
        # create release using gh cli
        subprocess.run(["gh", "release", "create", f"download-{os.getenv('RUN_ID')}",
                        "--title", f"Download {os.getenv('RUN_ID')}",
                        "--notes", "Auto-generated download",
                        "downloads/*"], check=False)
    
    # Callback if provided
    callback_url = os.getenv("CALLBACK_URL")
    if callback_url:
        import requests
        try:
            requests.post(callback_url, json={"status": "completed", "run_id": os.getenv("RUN_ID")})
        except:
            pass

if __name__ == "__main__":
    main()
