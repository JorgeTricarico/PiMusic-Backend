#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PiMusic High-Performance Remote API & Media Streamer
Optimizada para Raspberry Pi 5 / 3 y servidores locales.
Soporte completo para SaveFrom, Streaming nativo con Range 206 y Descarga Directa con fMP4.
"""

import os
import sys
import time
import json
import re
import socket
import logging
import logging.handlers
import asyncio
import subprocess
import signal
from typing import Optional, List, Dict, Any, Tuple
from collections import OrderedDict
from threading import Lock
from pathlib import Path

import psutil
import shutil
from datetime import datetime
import requests
import yt_dlp
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Request, Response
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from pydantic import BaseModel

import tempfile

# Cargar variables de entorno desde .env si python-dotenv está disponible
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Variables de Configuración Centralizadas
HOST = os.getenv("PIMUSIC_HOST", os.getenv("HOST", "0.0.0.0"))
PORT = int(os.getenv("PIMUSIC_PORT", os.getenv("PORT", "5000")))

# Directorio de descargas y almacenamiento multimedia
default_downloads = os.path.expanduser("~/pi-music-cache") if os.name != "nt" else os.path.join(os.getcwd(), "downloads")
DOWNLOADS_DIR = Path(os.getenv("PIMUSIC_DOWNLOADS_DIR", os.getenv("DOWNLOADS_DIR", default_downloads))).resolve()
DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

LOGS_DIR = Path(os.getenv("PIMUSIC_LOGS_DIR", str(DOWNLOADS_DIR / "logs"))).resolve()
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS_DIR / "pimusic.log"

SYSTEM_DATA_DIR = Path(os.getenv("PIMUSIC_SYSTEM_DIR", str(DOWNLOADS_DIR / "system"))).resolve()
SYSTEM_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Configuración de CORS
cors_raw = os.getenv("PIMUSIC_CORS_ORIGINS", os.getenv("CORS_ORIGINS", "*")).strip()
CORS_ORIGINS = [o.strip() for o in cors_raw.split(",") if o.strip()] if cors_raw != "*" else ["*"]

ALLOWED_MEDIA_EXTENSIONS = {
    "mp4", "m4v", "mkv", "webm", "mov", "avi", "flv", "wmv",
    "mp3", "m4a", "aac", "ogg", "opus", "wav", "flac", "weba"
}

# Configuración de Logging Persistente con Rotación (10 MB x 5 backups)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

file_handler = logging.handlers.RotatingFileHandler(
    str(LOG_FILE), maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(file_handler)
if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers if not isinstance(h, logging.FileHandler)):
    root_logger.addHandler(console_handler)

for uvicorn_log in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
    u_log = logging.getLogger(uvicorn_log)
    u_log.addHandler(file_handler)

logger = logging.getLogger("PiMusicServer")
logger.info("Iniciando servicio PiMusic Server con registro persistente en: %s", LOG_FILE)

app = FastAPI(title="PiMusic High-Performance API", version="3.0.0")

# CORS configurable
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length", "Content-Type", "X-Media-Duration"],
)

# Compresión Gzip automática para respuestas JSON y texto (> 1000 bytes)
app.add_middleware(GZipMiddleware, minimum_size=1000)

class SimpleRateLimiter:
    """Limitador de tasa por ventana deslizante en memoria para proteger contra abusos."""
    def __init__(self, max_requests: int = 60, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window = window_seconds
        self.clients: Dict[str, List[float]] = {}
        self._lock = Lock()

    def is_allowed(self, client_ip: str) -> bool:
        now = time.time()
        with self._lock:
            timestamps = self.clients.get(client_ip, [])
            valid = [t for t in timestamps if now - t < self.window]
            if len(valid) >= self.max_requests:
                self.clients[client_ip] = valid
                return False
            valid.append(now)
            self.clients[client_ip] = valid
            return True

rate_limiter = SimpleRateLimiter(max_requests=60, window_seconds=60)

def get_client_ip(request: Request) -> str:
    """Obtiene la IP real del cliente respetando cabeceras de proxies inversos y funnels."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "127.0.0.1"

@app.middleware("http")
async def security_and_cache_middleware(request: Request, call_next):
    response = await call_next(request)
    # Cabeceras de seguridad compatibles con funnels y navegadores modernos
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    
    # Políticas de caché para SPA
    path = request.url.path
    if path.startswith("/assets/"):
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    elif path in ("/", "/index.html"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response

CHUNK_SIZE = 256 * 1024  # Buffer de 256 KB para streaming fluido en 720p/1080p
CACHE_TTL_SECONDS = 3600  # 1 hora de TTL para caché de URLs de streaming
CACHE_MAX_ENTRIES = 256   # ~2MB de memoria RAM total

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Runtime JS para resolución de firmas YouTube en yt-dlp (Node.js o QuickJS)
NODE_PATH = shutil.which("node") or ("/usr/bin/node" if os.path.exists("/usr/bin/node") else None)
QJS_PATH = shutil.which("qjs") or ("/usr/local/bin/qjs" if os.path.exists("/usr/local/bin/qjs") else None)

if NODE_PATH:
    JS_OPTS = {"js_runtimes": {"node": {"path": NODE_PATH}}}
elif QJS_PATH:
    JS_OPTS = {"js_runtimes": {"quickjs": {"path": QJS_PATH}}}
else:
    JS_OPTS = {}

MEDIA_MIME_TYPES = {
    "mp4": "video/mp4",
    "m4v": "video/mp4",
    "mkv": "video/x-matroska",
    "webm": "video/webm",
    "mov": "video/quicktime",
    "avi": "video/x-msvideo",
    "flv": "video/x-flv",
    "wmv": "video/x-ms-wmv",
    "mp3": "audio/mpeg",
    "m4a": "audio/mp4",
    "aac": "audio/aac",
    "ogg": "audio/ogg",
    "opus": "audio/opus",
    "wav": "audio/wav",
    "flac": "audio/flac",
    "weba": "audio/webm",
}
VIDEO_EXTENSIONS = {"mp4", "m4v", "mkv", "webm", "mov", "avi", "flv", "wmv"}
AUDIO_EXTENSIONS = {"mp3", "m4a", "aac", "ogg", "opus", "wav", "flac", "weba"}

def get_media_mime_type(ext: str) -> str:
    return MEDIA_MIME_TYPES.get(ext.lower().lstrip("."), "application/octet-stream")



# =====================================================================
# 1. MOTOR DE CACHÉ EN MEMORIA LRU CON TTL (Zero Double-Extraction)
# =====================================================================

def extract_video_id(url_or_id: str) -> str:
    url_or_id = url_or_id.strip()
    if len(url_or_id) == 11 and re.match(r'^[a-zA-Z0-9_-]{11}$', url_or_id):
        return url_or_id
    patterns = [
        r'(?:v=|\/|youtu\.be\/|embed\/|shorts\/)([a-zA-Z0-9_-]{11})',
        r'^[a-zA-Z0-9_-]{11}$'
    ]
    for pattern in patterns:
        m = re.search(pattern, url_or_id)
        if m:
            return m.group(1)
    return url_or_id


class MediaMetadataCache:
    def __init__(self, max_entries: int = CACHE_MAX_ENTRIES, ttl: int = CACHE_TTL_SECONDS):
        self.max_entries = max_entries
        self.ttl = ttl
        self._cache: OrderedDict[str, Tuple[float, Any]] = OrderedDict()
        self._lock = Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._cache:
                return None
            ts, val = self._cache[key]
            if time.time() - ts > self.ttl:
                del self._cache[key]
                return None
            self._cache.move_to_end(key)
            return val

    def set(self, key: str, val: Any):
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = (time.time(), val)
            while len(self._cache) > self.max_entries:
                self._cache.popitem(last=False)


media_cache = MediaMetadataCache()
search_cache = MediaMetadataCache(max_entries=64, ttl=1800)


# =====================================================================
# 2. CONFIGURACIÓN DE YT-DLP Y FORMATOS
# =====================================================================

def get_ydl_base_opts() -> dict:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "extractor_args": {
            "youtube": {
                "player_client": ["android_vr", "android", "web"]
            }
        }
    }
    if COOKIES_FILE.exists() and COOKIES_FILE.stat().st_size > 0:
        opts["cookiefile"] = str(COOKIES_FILE)
    if JS_OPTS:
        opts.update(JS_OPTS)
    return opts


def format_duration(seconds: Optional[float]) -> str:
    if not seconds:
        return "0:00"
    try:
        s = int(float(seconds))
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        if h > 0:
            return f"{h}:{m:02d}:{s:02d}"
        return f"{m}:{s:02d}"
    except Exception:
        return "0:00"


def format_filesize(bytes_val: Optional[int]) -> str:
    if not bytes_val or bytes_val <= 0:
        return "~"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.1f} GB"


def extract_and_cache_info(url_or_id: str) -> dict:
    v_id = extract_video_id(url_or_id)
    cached = media_cache.get(v_id)
    if cached:
        return cached

    url = f"https://www.youtube.com/watch?v={v_id}" if not url_or_id.startswith("http") else url_or_id
    ydl_opts = get_ydl_base_opts()

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except yt_dlp.utils.DownloadError as err:
        msg = str(err).lower()
        if "429" in msg or "too many requests" in msg:
            raise HTTPException(
                status_code=503,
                detail="YouTube ha limitado temporalmente las solicitudes. Puedes cargar cookies en Ajustes."
            )
        elif "age" in msg or "confirm your age" in msg:
            raise HTTPException(
                status_code=403,
                detail="Video con restricción de edad. Requiere inicio de sesión o cookies."
            )
        elif "private" in msg or "unavailable" in msg:
            raise HTTPException(status_code=404, detail="El video es privado o no está disponible.")
        else:
            raise HTTPException(status_code=500, detail=f"Error resolviendo video en YouTube: {err}")

    if not info:
        raise HTTPException(status_code=404, detail="No se pudo obtener información del video")

    if info.get("is_live") or info.get("live_status") == "is_live":
        raise HTTPException(
            status_code=400,
            detail="Las transmisiones en vivo no pueden ser procesadas ni descargadas."
        )

    raw_id = info.get("id") or v_id
    title = info.get("title", "Video")
    uploader = info.get("uploader") or info.get("channel") or "YouTube"
    duration = int(info.get("duration") or 0)
    thumbnail = info.get("thumbnail") or f"https://i.ytimg.com/vi/{raw_id}/hqdefault.jpg"
    views = info.get("view_count", 0)
    raw_formats = info.get("formats", [])

    clean_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()[:60]
    if not clean_title:
        clean_title = f"pimusic_{raw_id}"

    # Clasificación de flujos
    progressive_by_height: Dict[int, dict] = {}
    h264_by_height: Dict[int, dict] = {}
    any_video_by_height: Dict[int, dict] = {}
    best_aac_audio: Optional[dict] = None
    best_audio: Optional[dict] = None

    for f in raw_formats:
        f_url = f.get("url")
        if not f_url or not f_url.startswith("http"):
            continue

        vcodec = f.get("vcodec", "none") or "none"
        acodec = f.get("acodec", "none") or "none"
        ext = f.get("ext", "")
        h = f.get("height")
        abr = f.get("abr") or f.get("tbr") or 0
        protocol = (f.get("protocol") or "").lower()
        is_m3u8 = (
            "m3u8" in protocol 
            or ".m3u8" in f_url.lower() 
            or f.get("is_m3u8") is True
        )

        # Progresivo MP4 genuino (H264 + AAC ya combinados en un único archivo físico)
        # CRÍTICO: Excluir M3U8/HLS porque rompen la reproducción nativa HTML5 en Android y Desktop
        if vcodec != "none" and acodec != "none" and ext == "mp4" and h and not is_m3u8:
            if h not in progressive_by_height or (f.get("tbr") or 0) > (progressive_by_height[h].get("tbr") or 0):
                progressive_by_height[h] = f

        # Video adaptativo H.264 / AVC1 (óptimo para remux directo fMP4 sin transcodificar)
        # Priorizar flujos directos HTTPS sobre playlists HLS m3u8
        if vcodec != "none" and acodec == "none" and h:
            if vcodec.startswith("avc1") or ext == "mp4":
                curr = h264_by_height.get(h)
                curr_is_m3u8 = curr and ("m3u8" in (curr.get("protocol") or "").lower() or ".m3u8" in (curr.get("url") or "").lower())
                if not curr or (curr_is_m3u8 and not is_m3u8) or ((not is_m3u8 or curr_is_m3u8) and (f.get("tbr") or 0) > (curr.get("tbr") or 0)):
                    h264_by_height[h] = f

            # Respaldo con cualquier codec de video si no hay h264
            curr_any = any_video_by_height.get(h)
            curr_any_is_m3u8 = curr_any and ("m3u8" in (curr_any.get("protocol") or "").lower() or ".m3u8" in (curr_any.get("url") or "").lower())
            if not curr_any or (curr_any_is_m3u8 and not is_m3u8) or ((not is_m3u8 or curr_any_is_m3u8) and (f.get("tbr") or 0) > (curr_any.get("tbr") or 0)):
                any_video_by_height[h] = f

        # Audio adaptativo AAC / M4A nativo (preferir flujos directos HTTPS)
        if acodec != "none" and vcodec == "none":
            if acodec.startswith("mp4a") or ext == "m4a" or f.get("format_id") in ("140", "140-0"):
                curr_is_m3u8 = best_aac_audio and ("m3u8" in (best_aac_audio.get("protocol") or "").lower() or ".m3u8" in (best_aac_audio.get("url") or "").lower())
                if not best_aac_audio or (curr_is_m3u8 and not is_m3u8) or ((not is_m3u8 or curr_is_m3u8) and abr > (best_aac_audio.get("abr") or 0)):
                    best_aac_audio = f
            if not best_audio or abr > (best_audio.get("abr") or 0):
                best_audio = f

    if not best_audio and progressive_by_height:
        first_h = next(iter(progressive_by_height))
        best_audio = progressive_by_height[first_h]
    if not best_aac_audio and best_audio:
        best_aac_audio = best_audio

    aac_fmt = best_aac_audio or {}
    best_audio_fmt = best_audio or aac_fmt

    target_heights = [1080, 720, 480, 360]
    video_options = []
    video_plans = {}

    all_heights = set(list(progressive_by_height.keys()) + list(h264_by_height.keys()) + list(any_video_by_height.keys()))
    for f in raw_formats:
        if f.get("height") and f.get("vcodec") != "none":
            all_heights.add(f.get("height"))

    max_h = max(all_heights) if all_heights else 360
    RATE_MB_PER_SEC = {1080: 0.45, 720: 0.22, 480: 0.10, 360: 0.06}

    for th in target_heights:
        # Solo ofrecer calidades menores o iguales a la resolución máxima del video original
        if th <= max_h or (th == 360 and not video_options):
            est_mb = int(duration * RATE_MB_PER_SEC.get(th, 0.15)) if duration else 0
            label = f"{th}p"
            badge = "Full HD" if th == 1080 else ("HD" if th == 720 else "SD")
            video_options.append({
                "quality": label,
                "height": th,
                "ext": "mp4",
                "badge": badge,
                "description": f"Video MP4 ({label}) con audio",
                "approx_size": f"~{est_mb} MB" if est_mb > 0 else "Variable"
            })

            if th in progressive_by_height:
                video_plans[label] = {
                    "mode": "progressive",
                    "url": progressive_by_height[th]["url"],
                    "ua": progressive_by_height[th].get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT)
                }
            elif th in h264_by_height and aac_fmt.get("url"):
                video_plans[label] = {
                    "mode": "remux",
                    "video_url": h264_by_height[th]["url"],
                    "audio_url": aac_fmt["url"],
                    "video_ua": h264_by_height[th].get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT),
                    "audio_ua": aac_fmt.get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT)
                }
            elif th in any_video_by_height and aac_fmt.get("url"):
                video_plans[label] = {
                    "mode": "remux_or_transcode",
                    "video_url": any_video_by_height[th]["url"],
                    "audio_url": aac_fmt["url"],
                    "video_ua": any_video_by_height[th].get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT),
                    "audio_ua": aac_fmt.get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT)
                }
            else:
                candidates = h264_by_height if h264_by_height else any_video_by_height
                closest_h = min(candidates.keys(), key=lambda x: abs(x - th)) if candidates else None
                if closest_h and aac_fmt.get("url"):
                    video_plans[label] = {
                        "mode": "remux",
                        "video_url": candidates[closest_h]["url"],
                        "audio_url": aac_fmt["url"],
                        "video_ua": candidates[closest_h].get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT),
                        "audio_ua": aac_fmt.get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT)
                    }
                else:
                    video_plans[label] = {
                        "mode": "ytdlp",
                        "url": url,
                        "height": th
                    }

    audio_options = [
        {
            "quality": "mp3_320",
            "ext": "mp3",
            "badge": "320 kbps",
            "description": "Audio MP3 (Máxima calidad)",
            "approx_size": f"~{int(duration * 40 / 1024)} MB" if duration else "~8 MB"
        },
        {
            "quality": "mp3_192",
            "ext": "mp3",
            "badge": "192 kbps",
            "description": "Audio MP3 (Estándar recomendado)",
            "approx_size": f"~{int(duration * 24 / 1024)} MB" if duration else "~5 MB"
        },
        {
            "quality": "m4a",
            "ext": "m4a",
            "badge": "AAC / M4A",
            "description": "Audio nativo original (Móviles / iOS)",
            "approx_size": f"~{int(duration * 16 / 1024)} MB" if duration else "~4 MB"
        }
    ]

    audio_plans = {
        "m4a": {
            "url": aac_fmt.get("url"),
            "ua": aac_fmt.get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT),
            "is_aac": True
        },
        "mp3_320": {
            "url": best_audio_fmt.get("url"),
            "ua": best_audio_fmt.get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT),
            "bitrate": "320k"
        },
        "mp3_192": {
            "url": best_audio_fmt.get("url"),
            "ua": best_audio_fmt.get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT),
            "bitrate": "192k"
        }
    }

    # Flujos de preview para reproductor
    preview_audio_url = aac_fmt.get("url") or best_audio_fmt.get("url")
    preview_audio_ua = aac_fmt.get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT)

    preview_prog = progressive_by_height.get(720) or progressive_by_height.get(360) or next(iter(progressive_by_height.values()), None)
    preview_video_url = preview_prog.get("url") if preview_prog else (h264_by_height.get(720, {}).get("url") or preview_audio_url)
    preview_video_ua = preview_prog.get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT) if preview_prog else DEFAULT_USER_AGENT

    progressive_streams = {
        f"{h}p": {
            "url": fmt["url"],
            "ua": fmt.get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT),
            "height": h
        }
        for h, fmt in progressive_by_height.items()
    }

    video_streams = {
        f"{h}p": {
            "url": fmt["url"],
            "ua": fmt.get("http_headers", {}).get("User-Agent", DEFAULT_USER_AGENT),
            "height": h
        }
        for h, fmt in (h264_by_height or any_video_by_height).items()
    }

    cached_data = {
        "id": raw_id,
        "title": title,
        "clean_title": clean_title,
        "uploader": uploader,
        "duration": format_duration(duration),
        "duration_seconds": duration,
        "thumbnail": thumbnail,
        "views": views,
        "url": url,
        "video_options": video_options,
        "audio_options": audio_options,
        "video_plans": video_plans,
        "audio_plans": audio_plans,
        "progressive_streams": progressive_streams,
        "video_streams": video_streams,
        "preview_audio_url": preview_audio_url,
        "preview_audio_ua": preview_audio_ua,
        "preview_video_url": preview_video_url,
        "preview_video_ua": preview_video_ua
    }

    media_cache.set(raw_id, cached_data)
    media_cache.set(url, cached_data)
    return cached_data


# =====================================================================
# 3. GENERADOR STREAMING ANTI-ZOMBIE
# =====================================================================

async def stream_media_process(cmd: list, chunk_size: int = CHUNK_SIZE, request: Optional[Request] = None):
    proc = None
    reader_task = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )

        # Buffer en memoria: 256 chunks de 256KB = 64 MB de pre-buffer continuo.
        # Desacopla la descarga de FFmpeg del consumo del cliente web/móvil para evitar bloqueos del socket.
        queue: asyncio.Queue = asyncio.Queue(maxsize=256)

        async def _reader():
            try:
                while True:
                    chunk = await proc.stdout.read(chunk_size)
                    if not chunk:
                        await queue.put(None)  # Señal EOF
                        break
                    await queue.put(chunk)
            except (asyncio.CancelledError, GeneratorExit):
                pass
            except Exception as e:
                logger.debug(f"Reader task finalizado con excepción: {e}")
                try:
                    await queue.put(None)
                except Exception:
                    pass

        reader_task = asyncio.create_task(_reader())

        while True:
            if request is not None and await request.is_disconnected():
                logger.info("Cliente desconectado detectado mediante request.is_disconnected()")
                break

            chunk = await queue.get()
            if chunk is None:
                break
            yield chunk

        if reader_task and not reader_task.done():
            reader_task.cancel()
        await proc.wait()

    except (asyncio.CancelledError, GeneratorExit, BrokenPipeError, ConnectionResetError):
        logger.info("Cliente desconectado del stream. Limpiando subproceso...")
    except Exception as e:
        logger.error(f"Error en streaming: {e}")
        raise
    finally:
        if reader_task and not reader_task.done():
            reader_task.cancel()
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass


# =====================================================================
# 4. MODELOS & ESTADO LOCAL
# =====================================================================

class SaveServerRequest(BaseModel):
    url: str
    format_type: str = "audio"
    quality: str = "mp3_320"
    title: Optional[str] = None

class PlayRequest(BaseModel):
    url: Optional[str] = ""
    title: Optional[str] = ""
    format: Optional[str] = "video"
    id: Optional[str] = ""

class ControlRequest(BaseModel):
    action: str
    value: Optional[str] = ""

class ActionRequest(BaseModel):
    action: str

class ResolveRequest(BaseModel):
    url: Optional[str] = ""
    id: Optional[str] = ""
    type: Optional[str] = None
    format: Optional[str] = None
    quality: Optional[str] = "480p"

class TerminalRequest(BaseModel):
    command: str

SERVER_PLAYER_STATE = {
    "state": "idle",
    "current_track": "",
    "url": "",
    "format": "AUDIO",
}

MPV_PROCESS: Optional[subprocess.Popen] = None
MPV_LOCK = Lock()

def stop_server_player():
    global MPV_PROCESS
    with MPV_LOCK:
        if MPV_PROCESS:
            try:
                MPV_PROCESS.terminate()
                MPV_PROCESS.wait(timeout=2)
            except Exception:
                try:
                    MPV_PROCESS.kill()
                except Exception:
                    pass
            MPV_PROCESS = None

def cache_played_track(url: str, title: Optional[str] = None, format_type: Optional[str] = "AUDIO"):
    """Guarda automáticamente la pista reproducida en el almacenamiento local para reanudación instantánea."""
    try:
        clean_title = re.sub(r'[\/*?:"<>|]', "", title or "").strip()
        is_audio = (format_type or "").upper() == "AUDIO"
        ext = "mp3" if is_audio else "mp4"
        if clean_title:
            target_file = DOWNLOADS_DIR / f"{clean_title}.{ext}"
            if target_file.exists():
                return
        target_path = DOWNLOADS_DIR / "%(title)s.%(ext)s"
        ydl_opts = {
            "outtmpl": str(target_path),
            "quiet": True,
            "no_warnings": True,
            **get_ydl_base_opts()
        }
        if is_audio:
            ydl_opts.update({
                "format": "bestaudio/best",
                "postprocessors": [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                }],
                "postprocessor_args": ["-threads", "2"]
            })
        else:
            ydl_opts.update({
                "format": "bestvideo[height<=480]+bestaudio/best[height<=480]/best",
                "merge_output_format": "mp4",
                "postprocessor_args": ["-c", "copy"]
            })
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        logger.info(f"Pista reproducida cacheada localmente en servidor: {url}")
    except Exception as e:
        logger.warning(f"Error en caching automático de pista reproducida: {e}")


# =====================================================================
# 5. ENDPOINTS PRINCIPALES
# =====================================================================

STATIC_DIST = Path(__file__).parent / "dist"
if not STATIC_DIST.exists():
    STATIC_DIST = Path(__file__).parent.parent / "dist"

if STATIC_DIST.exists() and (STATIC_DIST / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(STATIC_DIST / "assets")), name="assets")

@app.api_route("/", methods=["GET", "HEAD"])
def read_root():
    if STATIC_DIST.exists() and (STATIC_DIST / "index.html").exists():
        return FileResponse(STATIC_DIST / "index.html")
    return {
        "name": "PiMusic High-Performance API",
        "version": "3.0.0",
        "status": "online"
    }


@app.get("/api/search")
def search_youtube(request: Request, q: str = Query(..., description="Término de búsqueda"), limit: int = Query(16, ge=1, le=30)):
    ip = get_client_ip(request)
    if not rate_limiter.is_allowed(ip):
        raise HTTPException(status_code=429, detail="Demasiadas solicitudes de búsqueda. Espera un momento.")

    if not q.strip():
        return []

    cache_key = f"search:{q.strip().lower()}:{limit}"
    cached = search_cache.get(cache_key)
    if cached:
        return cached

    ydl_opts = get_ydl_base_opts()
    ydl_opts.update({
        "extract_flat": True,
        "default_search": f"ytsearch{limit}",
    })
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            results = ydl.extract_info(f"ytsearch{limit}:{q}", download=False)
            entries = results.get("entries", []) if results else []
            cleaned = []
            for entry in entries:
                if not entry:
                    continue
                v_id = entry.get("id")
                url = entry.get("url") or f"https://www.youtube.com/watch?v={v_id}"
                thumbnail = entry.get("thumbnail") or (f"https://i.ytimg.com/vi/{v_id}/hqdefault.jpg" if v_id else "")
                cleaned.append({
                    "id": v_id,
                    "title": entry.get("title", "Sin título"),
                    "url": url,
                    "duration": format_duration(entry.get("duration")),
                    "duration_seconds": entry.get("duration") or 0,
                    "uploader": entry.get("uploader") or entry.get("channel") or "YouTube",
                    "thumbnail": thumbnail,
                    "view_count": entry.get("view_count")
                })
            search_cache.set(cache_key, cleaned)
            return cleaned
    except Exception as e:
        logger.error(f"Error en búsqueda: {e}")
        raise HTTPException(status_code=500, detail="Error al buscar en YouTube")


@app.get("/api/info")
def get_video_info(request: Request, url: str = Query(..., description="URL o ID de YouTube")):
    ip = get_client_ip(request)
    if not rate_limiter.is_allowed(ip):
        raise HTTPException(status_code=429, detail="Demasiadas solicitudes. Espera un momento.")
    try:
        data = extract_and_cache_info(url)
        return {
            "id": data["id"],
            "title": data["title"],
            "uploader": data["uploader"],
            "duration": data["duration"],
            "duration_seconds": data["duration_seconds"],
            "thumbnail": data["thumbnail"],
            "views": data["views"],
            "url": data["url"],
            "video_options": data["video_options"],
            "audio_options": data["audio_options"]
        }
    except Exception as e:
        logger.error(f"Error en /api/info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# ENDPOINTS DE STREAMING CON PROXY LOCAL Y HTTP RANGE 206
# =====================================================================

@app.get("/api/stream/{video_id}")
def stream_preview(
    video_id: str,
    type: str = Query("audio", pattern="^(audio|video)$"),
    quality: str = Query("720p")
):
    """Devuelve la URL del proxy local con buffering universal sin CORS ni bloqueo de IP."""
    try:
        data = extract_and_cache_info(video_id)
        stream_url = f"/api/stream_media/{video_id}?type={type}"
        if type == "video":
            stream_url += f"&quality={quality}"
        return {
            "video_id": video_id,
            "title": data["title"],
            "stream_url": stream_url,
            "type": type,
            "quality": quality
        }
    except Exception as e:
        logger.error(f"Error en stream preview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.api_route("/api/stream_media/{video_id}", methods=["GET", "HEAD"])
async def stream_media(
    video_id: str,
    request: Request,
    type: str = Query("audio", pattern="^(audio|video)$"),
    quality: str = Query("720p"),
    start: float = Query(0.0, ge=0.0, description="Posición inicial en segundos")
):
    """
    Motor de Streaming Adaptativo Ultra-Rápido para Reproductores Web/Móviles.
    - Progresivo nativo (MP4 único) -> Proxy HTTP Range 206 para scrubbing instantáneo.
    - Video Adaptativo (DASH/HLS) -> Live remuxing a fMP4 con FFmpeg (-c copy) sin recodificar.
    - Audio -> Proxy HTTP Range 206 o remux AAC directo.
    """
    if request.method == "HEAD":
        media_type = "audio/mp4" if type == "audio" or quality == "audio" else "video/mp4"
        return Response(
            status_code=200,
            headers={
                "Content-Type": media_type,
                "Accept-Ranges": "bytes" if type == "audio" else "none",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Range, Content-Type",
            }
        )

    try:
        data = await asyncio.to_thread(extract_and_cache_info, video_id)
    except Exception as e:
        logger.error(f"Error al extraer info de video para stream_media: {e}")
        raise HTTPException(status_code=404, detail=f"No se pudo resolver el stream: {e}")

    duration_sec = str(data.get("duration_seconds") or 0)

    if quality == "audio":
        type = "audio"

    # =========================================================================
    # A) MODO AUDIO
    # =========================================================================
    if type == "audio":
        target_url = data.get("preview_audio_url")
        target_ua = data.get("preview_audio_ua") or DEFAULT_USER_AGENT
        if not target_url:
            raise HTTPException(status_code=404, detail="Flujo de audio no disponible")

        # Si el audio es una playlist m3u8, remuxear a fMP4 con ffmpeg
        if ".m3u8" in target_url.lower():
            cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error"]
            if start > 0:
                cmd.extend(["-ss", str(start)])
            cmd.extend([
                "-user_agent", target_ua,
                "-i", target_url,
                "-c:a", "copy",
                "-movflags", "frag_keyframe+empty_moov+default_base_moof",
                "-f", "mp4", "-"
            ])
            return StreamingResponse(
                stream_media_process(cmd, chunk_size=CHUNK_SIZE),
                status_code=200,
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Cache-Control": "no-cache",
                    "X-Media-Duration": duration_sec,
                    "Access-Control-Expose-Headers": "X-Media-Duration",
                },
                media_type="audio/mp4"
            )

        # Si es un flujo directo HTTP/HTTPS, usar Proxy Range 206
        headers = {"User-Agent": target_ua, "Accept": "*/*"}
        range_header = request.headers.get("range")
        if range_header:
            headers["Range"] = range_header

        try:
            upstream_resp = await asyncio.to_thread(requests.get, target_url, headers=headers, stream=True, timeout=15)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Error conectando con servidor de YouTube: {e}")

        def iter_audio():
            try:
                for chunk in upstream_resp.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        yield chunk
            finally:
                upstream_resp.close()

        response_headers = {
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Range, Content-Range, Accept-Ranges, Content-Type",
            "Access-Control-Expose-Headers": "Content-Range, Accept-Ranges, Content-Length, X-Media-Duration",
            "X-Media-Duration": duration_sec,
            "Cache-Control": "public, max-age=3600"
        }
        for h in ["Content-Range", "Content-Length", "Content-Type"]:
            if h in upstream_resp.headers:
                response_headers[h] = upstream_resp.headers[h]
            elif h.lower() in upstream_resp.headers:
                response_headers[h] = upstream_resp.headers[h.lower()]

        return StreamingResponse(
            iter_audio(),
            status_code=upstream_resp.status_code,
            headers=response_headers,
            media_type=response_headers.get("Content-Type", "audio/mp4")
        )

    # =========================================================================
    # B) MODO VIDEO
    # =========================================================================
    m = re.search(r"(\d+)", quality or "720p")
    req_h = int(m.group(1)) if m else 720
    q_label = f"{req_h}p"

    prog_streams = data.get("progressive_streams", {})

    # 1. ¿Existe un flujo progresivo MP4 físico no-m3u8 en esta resolución?
    if q_label in prog_streams and prog_streams[q_label].get("url") and not (".m3u8" in prog_streams[q_label]["url"].lower()):
        target_url = prog_streams[q_label]["url"]
        target_ua = prog_streams[q_label].get("ua") or DEFAULT_USER_AGENT

        headers = {"User-Agent": target_ua, "Accept": "*/*"}
        range_header = request.headers.get("range")
        if range_header:
            headers["Range"] = range_header

        try:
            upstream_resp = await asyncio.to_thread(requests.get, target_url, headers=headers, stream=True, timeout=15)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Error conectando con servidor de YouTube: {e}")

        def iter_video():
            try:
                for chunk in upstream_resp.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        yield chunk
            finally:
                upstream_resp.close()

        response_headers = {
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Range, Content-Range, Accept-Ranges, Content-Type",
            "Access-Control-Expose-Headers": "Content-Range, Accept-Ranges, Content-Length, X-Media-Duration",
            "X-Media-Duration": duration_sec,
            "Cache-Control": "public, max-age=3600"
        }
        for h in ["Content-Range", "Content-Length", "Content-Type"]:
            if h in upstream_resp.headers:
                response_headers[h] = upstream_resp.headers[h]
            elif h.lower() in upstream_resp.headers:
                response_headers[h] = upstream_resp.headers[h.lower()]

        return StreamingResponse(
            iter_video(),
            status_code=upstream_resp.status_code,
            headers=response_headers,
            media_type="video/mp4"
        )

    # 2. Si no es progresivo directo (DASH separado o HLS): Remuxing Live a fMP4 con FFmpeg
    plans = data.get("video_plans", {})
    plan = plans.get(q_label) or {}

    video_url = plan.get("video_url") or plan.get("url")
    video_ua = plan.get("video_ua") or plan.get("ua") or DEFAULT_USER_AGENT
    audio_url = plan.get("audio_url") or data.get("preview_audio_url")
    audio_ua = plan.get("audio_ua") or data.get("preview_audio_ua") or DEFAULT_USER_AGENT

    if not video_url:
        vid_streams = data.get("video_streams", {})
        if q_label in vid_streams and vid_streams[q_label].get("url"):
            video_url = vid_streams[q_label]["url"]
            video_ua = vid_streams[q_label].get("ua") or DEFAULT_USER_AGENT
        else:
            candidates: Dict[int, dict] = {}
            for k, v in vid_streams.items():
                if v.get("url"):
                    h_m = re.search(r"(\d+)", k)
                    if h_m:
                        candidates[int(h_m.group(1))] = v
            if candidates:
                closest_h = min(candidates.keys(), key=lambda h: abs(h - req_h))
                video_url = candidates[closest_h]["url"]
                video_ua = candidates[closest_h].get("ua") or DEFAULT_USER_AGENT
            else:
                video_url = data.get("preview_video_url")
                video_ua = data.get("preview_video_ua") or DEFAULT_USER_AGENT

    if not video_url:
        raise HTTPException(status_code=404, detail="Flujo de video no disponible")

    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-fflags", "+genpts",
    ]
    if start > 0:
        cmd.extend(["-ss", str(start)])
    cmd.extend([
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
        "-user_agent", video_ua,
        "-i", video_url,
    ])

    if audio_url:
        if start > 0:
            cmd.extend(["-ss", str(start)])
        cmd.extend([
            "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
            "-user_agent", audio_ua,
            "-i", audio_url,
        ])
        cmd.extend([
            "-map", "0:v:0", "-map", "1:a:0",
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "192k",
            "-af", "aresample=async=1:first_pts=0",
            "-fps_mode", "passthrough",
        ])
    else:
        cmd.extend([
            "-map", "0:v:0",
            "-c:v", "copy",
            "-fps_mode", "passthrough",
        ])

    cmd.extend([
        "-max_interleave_delta", "0",
        "-copyts", "-start_at_zero", "-muxdelay", "0",
        "-max_muxing_queue_size", "4096",
        "-movflags", "frag_keyframe+empty_moov+default_base_moof",
        "-f", "mp4", "-"
    ])

    response_headers = {
        "Accept-Ranges": "none",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Range, Content-Type",
        "Access-Control-Expose-Headers": "X-Media-Duration",
        "X-Media-Duration": duration_sec,
        "Cache-Control": "no-cache, no-store, must-revalidate",
    }

    return StreamingResponse(
        stream_media_process(cmd, chunk_size=CHUNK_SIZE, request=request),
        status_code=200,
        headers=response_headers,
        media_type="video/mp4"
    )


# =====================================================================
# ENDPOINT DE DESCARGA DIRECTA (ZERO-TRANSCODE & fMP4)
# =====================================================================

@app.get("/api/download")
async def download_direct(
    url: str = Query(...),
    type: str = Query("video", pattern="^(video|audio)$"),
    quality: str = Query("720p")
):
    try:
        cached = extract_and_cache_info(url)
    except Exception as e:
        logger.error(f"Error resolviendo info para descarga: {e}")
        raise HTTPException(status_code=500, detail="Error analizando el video de YouTube")

    clean_title = cached["clean_title"]

    if type == "audio":
        if "m4a" in quality:
            ext = "m4a"
            media_type = "audio/mp4"
            plan = cached["audio_plans"].get("m4a") or {}
            a_url = plan.get("url") or cached["preview_audio_url"]
            a_ua = plan.get("ua") or DEFAULT_USER_AGENT
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
                "-user_agent", a_ua,
                "-i", a_url,
                "-vn", "-c:a", "copy",
                "-movflags", "frag_keyframe+empty_moov+default_base_moof",
                "-f", "mp4", "-"
            ]
        else:
            ext = "mp3"
            media_type = "audio/mpeg"
            bitrate = "320k" if "320" in quality else "192k"
            plan = cached["audio_plans"].get(quality) or cached["audio_plans"].get("mp3_320") or {}
            a_url = plan.get("url") or cached["preview_audio_url"]
            a_ua = plan.get("ua") or DEFAULT_USER_AGENT
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
                "-user_agent", a_ua,
                "-i", a_url,
                "-vn", "-c:a", "libmp3lame", "-b:a", bitrate,
                "-threads", "4",
                "-f", "mp3", "-"
            ]
    else:
        ext = "mp4"
        media_type = "video/mp4"
        plan = cached["video_plans"].get(quality) or cached["video_plans"].get("720p") or {}

        if plan.get("mode") == "progressive":
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
                "-user_agent", plan.get("ua", DEFAULT_USER_AGENT),
                "-i", plan["url"],
                "-c", "copy",
                "-movflags", "frag_keyframe+empty_moov+default_base_moof",
                "-f", "mp4", "-"
            ]
        elif plan.get("mode") == "remux":
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-avoid_negative_ts", "make_zero",
                "-fflags", "+genpts",
                "-max_muxing_queue_size", "4096",
                "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
                "-user_agent", plan.get("video_ua", DEFAULT_USER_AGENT),
                "-i", plan["video_url"],
                "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
                "-user_agent", plan.get("audio_ua", DEFAULT_USER_AGENT),
                "-i", plan["audio_url"],
                "-c:v", "copy",
                "-c:a", "aac", "-b:a", "192k",
                "-af", "aresample=async=1000:first_pts=0",
                "-movflags", "frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets",
                "-f", "mp4", "-"
            ]
        elif plan.get("mode") == "remux_or_transcode":
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-avoid_negative_ts", "make_zero",
                "-fflags", "+genpts",
                "-max_muxing_queue_size", "4096",
                "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
                "-user_agent", plan.get("video_ua", DEFAULT_USER_AGENT),
                "-i", plan["video_url"],
                "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
                "-user_agent", plan.get("audio_ua", DEFAULT_USER_AGENT),
                "-i", plan["audio_url"],
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "24",
                "-c:a", "aac", "-b:a", "192k",
                "-af", "aresample=async=1000:first_pts=0",
                "-movflags", "frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets",
                "-f", "mp4", "-"
            ]
        else:
            v_url = plan.get("video_url") or plan.get("url") or cached["preview_video_url"]
            a_url = plan.get("audio_url") or cached["preview_audio_url"]
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-avoid_negative_ts", "make_zero",
                "-fflags", "+genpts",
                "-max_muxing_queue_size", "4096",
                "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
                "-user_agent", DEFAULT_USER_AGENT,
                "-i", v_url,
                "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
                "-user_agent", DEFAULT_USER_AGENT,
                "-i", a_url,
                "-c:v", "copy",
                "-c:a", "aac", "-b:a", "192k",
                "-af", "aresample=async=1000:first_pts=0",
                "-movflags", "frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets",
                "-f", "mp4", "-"
            ]

    filename = f"{clean_title}.{ext}"
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Access-Control-Expose-Headers": "Content-Disposition"
    }

    return StreamingResponse(
        stream_media_process(cmd, chunk_size=CHUNK_SIZE),
        media_type=media_type,
        headers=headers
    )


# =====================================================================
# GUARDADO EN SERVIDOR Y OTROS
# =====================================================================

@app.post("/api/save-server")
def save_to_server(req: SaveServerRequest, bg_tasks: BackgroundTasks):
    def run_download():
        target_path = DOWNLOADS_DIR / "%(title)s.%(ext)s"
        ydl_opts = {
            "outtmpl": str(target_path),
            "quiet": True,
            "no_warnings": True,
            **get_ydl_base_opts()
        }
        if req.format_type == "audio":
            if req.quality == "m4a":
                ydl_opts.update({"format": "bestaudio[ext=m4a]/bestaudio/best"})
            else:
                ydl_opts.update({
                    "format": "bestaudio/best",
                    "postprocessors": [{
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "320" if "320" in req.quality else "192",
                    }],
                    "postprocessor_args": ["-threads", "4"]
                })
        else:
            h = req.quality.replace("p", "")
            ydl_opts.update({
                "format": f"bestvideo[height<={h}]+bestaudio/best[height<={h}]/best",
                "merge_output_format": "mp4",
                "postprocessor_args": ["-c", "copy"]
            })
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([req.url])
            logger.info(f"Descarga completada en servidor: {req.url}")
        except Exception as e:
            logger.error(f"Error guardando en servidor: {e}")

    bg_tasks.add_task(run_download)
    return {"status": "queued", "message": f"Descarga iniciada en servidor para: {req.url}"}


@app.get("/api/library")
def get_library():
    """
    Lista los archivos descargados en DOWNLOADS_DIR con metadatos completos de almacenamiento.
    """
    try:
        DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
        files = []
        total_bytes = 0

        if DOWNLOADS_DIR.exists():
            for p in DOWNLOADS_DIR.iterdir():
                if p.is_file() and not p.name.startswith("."):
                    ext = p.suffix.lower().lstrip(".")
                    if ext in ("part", "ytdl", "tmp"):
                        continue
                    if ext in VIDEO_EXTENSIONS or ext in AUDIO_EXTENSIONS or ext in ["mp4", "mkv", "webm", "avi", "mov", "mp3", "m4a", "wav", "flac", "ogg", "opus"]:
                        try:
                            stat = p.stat()
                            size = stat.st_size
                            total_bytes += size
                            mtime = stat.st_mtime
                            created_dt = datetime.fromtimestamp(mtime)
                            created_at_str = created_dt.strftime("%Y-%m-%d %H:%M:%S")
                            date_formatted = created_dt.strftime("%d/%m/%Y %H:%M")
                            media_type = "video" if ext in VIDEO_EXTENSIONS else "audio"

                            files.append({
                                "id": p.name,
                                "filename": p.name,
                                "title": p.stem,
                                "ext": ext,
                                "type": media_type,
                                "size_bytes": size,
                                "size_formatted": format_filesize(size),
                                "mtime": mtime,
                                "modified_at": mtime,
                                "created_at_str": created_at_str,
                                "date_formatted": date_formatted,
                                "stream_url": f"/api/library/stream/{p.name}",
                                "download_url": f"/api/library/download/{p.name}"
                            })
                        except Exception as fe:
                            logger.warning(f"Error procesando archivo {p.name}: {fe}")

        files.sort(key=lambda x: x["mtime"], reverse=True)

        try:
            disk = psutil.disk_usage(str(DOWNLOADS_DIR))
        except Exception:
            disk = psutil.disk_usage("/")

        total_files = len(files)
        total_size_bytes = total_bytes
        total_size_formatted = format_filesize(total_bytes)
        free_disk_percent = round((disk.free / disk.total) * 100, 1) if disk.total > 0 else 0.0
        free_disk_formatted = format_filesize(disk.free)

        stats = {
            "total_files": total_files,
            "used_bytes": total_size_bytes,
            "used_formatted": total_size_formatted,
            "disk_total_gb": round(disk.total / (1024**3), 1),
            "disk_free_gb": round(disk.free / (1024**3), 1),
            "disk_free_formatted": free_disk_formatted,
            "disk_used_percent": round(disk.percent, 1),
            "disk_free_percent": free_disk_percent
        }

        return {
            "files": files,
            "total_files": total_files,
            "total_size_bytes": total_size_bytes,
            "total_size_formatted": total_size_formatted,
            "free_disk_percent": free_disk_percent,
            "free_disk_formatted": free_disk_formatted,
            "stats": stats
        }
    except Exception as e:
        logger.error(f"Error listando biblioteca: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/library/{filename:path}")
def delete_library_file(filename: str):
    """
    Elimina un archivo de la biblioteca de la Raspberry Pi de forma segura con protección anti path-traversal.
    """
    try:
        clean_name = os.path.basename(filename.strip())
        if not clean_name or clean_name in (".", ".."):
            raise HTTPException(status_code=400, detail="Nombre de archivo inválido")

        file_path = (DOWNLOADS_DIR / clean_name).resolve()
        try:
            file_path.relative_to(DOWNLOADS_DIR.resolve())
        except ValueError:
            raise HTTPException(status_code=400, detail="Acceso no permitido fuera del directorio de descargas")

        ext = file_path.suffix.lower().lstrip(".")
        if ext not in ALLOWED_MEDIA_EXTENSIONS:
            raise HTTPException(status_code=403, detail="Acceso denegado: solo se admiten archivos multimedia")

        if not file_path.exists() or not file_path.is_file():
            raise HTTPException(status_code=404, detail="Archivo no encontrado")

        file_path.unlink()
        # Eliminar archivo temporal residual .part si existe
        part_path = file_path.with_name(f"{file_path.name}.part")
        if part_path.exists():
            try:
                part_path.unlink()
            except Exception:
                pass

        logger.info(f"Archivo eliminado de la biblioteca: {clean_name}")
        return {"status": "deleted", "filename": clean_name}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error eliminando archivo: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/library/stream/{filename:path}")
def stream_library_file(filename: str, request: Request):
    """
    Transmite un archivo local con soporte completo para HTTP Range 206 y seeking instantáneo.
    """
    clean_name = os.path.basename(filename.strip())
    if not clean_name or clean_name in (".", ".."):
        raise HTTPException(status_code=400, detail="Nombre de archivo inválido")

    file_path = (DOWNLOADS_DIR / clean_name).resolve()
    try:
        file_path.relative_to(DOWNLOADS_DIR.resolve())
    except ValueError:
        raise HTTPException(status_code=400, detail="Acceso no permitido fuera del directorio de descargas")

    ext = file_path.suffix.lower().lstrip(".")
    if ext not in ALLOWED_MEDIA_EXTENSIONS:
        raise HTTPException(status_code=403, detail="Acceso denegado: solo se admiten archivos multimedia")

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    media_type = get_media_mime_type(ext)

    response_headers = {
        "Accept-Ranges": "bytes",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Range, Content-Range, Accept-Ranges, Content-Type",
        "Access-Control-Expose-Headers": "Content-Range, Accept-Ranges, Content-Length",
        "Cache-Control": "public, max-age=3600"
    }

    return FileResponse(
        path=file_path,
        media_type=media_type,
        headers=response_headers
    )


@app.get("/api/library/download/{filename:path}")
def download_library_file(filename: str):
    """
    Devuelve FileResponse con Content-Disposition: attachment; filename="...".
    """
    clean_name = os.path.basename(filename.strip())
    if not clean_name or clean_name in (".", ".."):
        raise HTTPException(status_code=400, detail="Nombre de archivo inválido")

    file_path = (DOWNLOADS_DIR / clean_name).resolve()
    try:
        file_path.relative_to(DOWNLOADS_DIR.resolve())
    except ValueError:
        raise HTTPException(status_code=400, detail="Acceso no permitido fuera del directorio de descargas")

    ext = file_path.suffix.lower().lstrip(".")
    if ext not in ALLOWED_MEDIA_EXTENSIONS:
        raise HTTPException(status_code=403, detail="Acceso denegado: solo se admiten archivos multimedia")

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    media_type = get_media_mime_type(ext)

    return FileResponse(
        path=file_path,
        filename=clean_name,
        media_type=media_type,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Expose-Headers": "Content-Disposition, Content-Length",
            "Content-Disposition": f'attachment; filename="{clean_name}"'
        }
    )


@app.post("/api/resolve")
def resolve_stream_url(req: ResolveRequest, request: Request):
    raw_url = (req.url or "").strip()
    v_id = req.id or extract_video_id(raw_url)
    target = raw_url if raw_url.startswith("http") else f"https://www.youtube.com/watch?v={v_id}"
    if not v_id:
        v_id = extract_video_id(target)

    media_type = (req.type or req.format or "video").strip().lower()
    is_video = (media_type in ["video", "mp4"])
    quality = (req.quality or "480p").lower()

    # 1. Intentar resolver desde extract_and_cache_info
    try:
        cached = extract_and_cache_info(target)
        title = cached.get("title", "")
        if is_video:
            prog_streams = cached.get("progressive_streams", {})
            # Priorizar flujos progresivos MP4 nativos multiplexados (video H264 + audio AAC en un único contenedor)
            pref_keys = ["720p", "480p", "360p"] if "720" in quality else ["480p", "720p", "360p"]
            for pref_q in pref_keys:
                if pref_q in prog_streams and prog_streams[pref_q].get("url") and not (".m3u8" in prog_streams[pref_q]["url"].lower()):
                    stream_url = prog_streams[pref_q]["url"]
                    return {
                        "status": "ok",
                        "stream_url": stream_url,
                        "direct_url": stream_url,
                        "url": stream_url,
                        "title": title,
                        "format": "mp4",
                        "type": "video"
                    }
            for q, pdata in prog_streams.items():
                if pdata.get("url") and not (".m3u8" in pdata["url"].lower()):
                    stream_url = pdata["url"]
                    return {
                        "status": "ok",
                        "stream_url": stream_url,
                        "direct_url": stream_url,
                        "url": stream_url,
                        "title": title,
                        "format": "mp4",
                        "type": "video"
                    }
        else:
            # AUDIO
            audio_url = cached.get("preview_audio_url")
            if audio_url and not (".m3u8" in audio_url.lower()):
                return {
                    "status": "ok",
                    "stream_url": audio_url,
                    "direct_url": audio_url,
                    "url": audio_url,
                    "title": title,
                    "format": "m4a",
                    "type": "audio"
                }
    except Exception as e:
        logger.warning(f"Advertencia resolviendo stream desde cache en /api/resolve: {e}")

    # 2. Resolución directa con yt-dlp usando los selectores óptimos para Android MediaPlayer
    max_h = 480
    if "720" in quality:
        max_h = 720
    elif "360" in quality:
        max_h = 360
    elif "1080" in quality:
        max_h = 1080

    ydl_opts = get_ydl_base_opts()
    if is_video:
        ydl_opts["format"] = f"18/22/best[ext=mp4][height<={max_h}]/best[height<={max_h}]/best[ext=mp4]/best[vcodec!=none][acodec!=none]/best"
    else:
        ydl_opts["format"] = "140/bestaudio[ext=m4a]/bestaudio/best"

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(target, download=False)
            url = info.get("url") or ""
            title = info.get("title", "")
            if url:
                return {
                    "status": "ok",
                    "stream_url": url,
                    "direct_url": url,
                    "url": url,
                    "title": title,
                    "format": info.get("ext") or ("mp4" if is_video else "m4a"),
                    "type": "video" if is_video else "audio"
                }
    except Exception as e:
        logger.warning(f"yt-dlp directo no encontró stream en /api/resolve: {e}")

    # 3. Fallback de alta resiliencia para Android MediaPlayer / ExoPlayer:
    # Servir a través del endpoint remuxeado por el propio servidor de la Raspberry Pi
    host = request.headers.get("host") or f"{HOST}:{PORT}"
    scheme = request.url.scheme if hasattr(request, "url") and request.url.scheme else "http"
    fallback_type = "video" if is_video else "audio"
    fallback_stream_url = f"{scheme}://{host}/api/stream_media/{v_id}?type={fallback_type}&quality=480p"
    return {
        "status": "ok",
        "stream_url": fallback_stream_url,
        "direct_url": fallback_stream_url,
        "url": fallback_stream_url,
        "title": v_id,
        "format": "mp4" if is_video else "m4a",
        "type": fallback_type
    }


@app.post("/api/terminal")
def execute_terminal(req: TerminalRequest):
    cmd = req.command.strip()
    if not cmd:
        return {"output": "", "exit_code": 0}
    try:
        res = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=15
        )
        output = (res.stdout or "") + (res.stderr or "")
        return {"output": output, "exit_code": res.returncode}
    except subprocess.TimeoutExpired:
        return {"output": "Error: Comando excedió el tiempo límite de ejecución (15s)", "exit_code": 124}
    except Exception as e:
        return {"output": f"Error ejecutando comando: {e}", "exit_code": 1}


@app.get("/api/status")
def get_player_status():
    global SERVER_PLAYER_STATE, MPV_PROCESS
    if MPV_PROCESS and MPV_PROCESS.poll() is not None:
        SERVER_PLAYER_STATE["state"] = "idle"
        MPV_PROCESS = None

    return {
        "state": SERVER_PLAYER_STATE["state"],
        "current_track": SERVER_PLAYER_STATE["current_track"],
        "task_status": "IDLE",
        "download_progress": 0,
        "error": None,
        "position": 0.0,
        "volume": 1.0
    }


@app.post("/api/play")
def play_on_server(req: PlayRequest, bg_tasks: BackgroundTasks):
    global SERVER_PLAYER_STATE, MPV_PROCESS
    raw_url = (req.url or "").strip()
    v_id = req.id or extract_video_id(raw_url)
    target = raw_url if raw_url.startswith("http") else f"https://www.youtube.com/watch?v={v_id}"

    SERVER_PLAYER_STATE["state"] = "playing"
    SERVER_PLAYER_STATE["current_track"] = req.title or target
    SERVER_PLAYER_STATE["url"] = target
    SERVER_PLAYER_STATE["format"] = req.format or "video"

    # Iniciar reproducción por hardware en la Raspberry Pi si MPV está disponible
    if shutil.which("mpv"):
        stop_server_player()
        is_audio = (req.format or "").strip().lower() in ["audio", "mp3"]
        mpv_cmd = ["mpv", "--no-terminal", "--idle=no"]
        if is_audio:
            mpv_cmd.append("--no-video")
        else:
            mpv_cmd.append("--fs")
        mpv_cmd.append(target)
        try:
            with MPV_LOCK:
                MPV_PROCESS = subprocess.Popen(
                    mpv_cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            logger.info(f"MPV iniciado para {target} (audio={is_audio})")
        except Exception as pe:
            logger.warning(f"No se pudo iniciar MPV en Raspberry Pi: {pe}")

    # Caching automático de la pista en segundo plano para reproducciones instantáneas futuras
    bg_tasks.add_task(cache_played_track, target, req.title, req.format)

    return {
        "status": "ok",
        "message": f"Reproduciendo {target}",
        "track": SERVER_PLAYER_STATE["current_track"]
    }


@app.post("/api/control")
@app.post("/api/playback/action")
def control_player(req: ControlRequest):
    global SERVER_PLAYER_STATE, MPV_PROCESS
    action = (req.action or "").lower().strip()
    if action == "pause":
        SERVER_PLAYER_STATE["state"] = "paused"
        if MPV_PROCESS and MPV_PROCESS.poll() is None and os.name != "nt":
            try:
                os.kill(MPV_PROCESS.pid, signal.SIGSTOP)
            except Exception:
                pass
    elif action in ["stop"]:
        SERVER_PLAYER_STATE["state"] = "idle"
        stop_server_player()
    elif action in ["resume", "play"]:
        SERVER_PLAYER_STATE["state"] = "playing"
        if MPV_PROCESS and MPV_PROCESS.poll() is None and os.name != "nt":
            try:
                os.kill(MPV_PROCESS.pid, signal.SIGCONT)
            except Exception:
                pass
    elif action == "toggle":
        if SERVER_PLAYER_STATE["state"] == "playing":
            SERVER_PLAYER_STATE["state"] = "paused"
            if MPV_PROCESS and MPV_PROCESS.poll() is None and os.name != "nt":
                try:
                    os.kill(MPV_PROCESS.pid, signal.SIGSTOP)
                except Exception:
                    pass
        else:
            SERVER_PLAYER_STATE["state"] = "playing"
            if MPV_PROCESS and MPV_PROCESS.poll() is None and os.name != "nt":
                try:
                    os.kill(MPV_PROCESS.pid, signal.SIGCONT)
                except Exception:
                    pass
    return {"status": "ok", "state": SERVER_PLAYER_STATE["state"]}


@app.get("/api/telemetry")
def get_telemetry():
    try:
        cpu_percent = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory()
        disk_path = str(DOWNLOADS_DIR) if DOWNLOADS_DIR.exists() else "/"
        try:
            disk = psutil.disk_usage(disk_path)
        except Exception:
            disk = psutil.disk_usage("/")

        temp_str = "N/A"
        try:
            if os.path.exists("/sys/class/thermal/thermal_zone0/temp"):
                with open("/sys/class/thermal/thermal_zone0/temp", "r") as f:
                    temp_str = f"{int(f.read()) / 1000.0:.1f}°C"
            elif hasattr(psutil, "sensors_temperatures"):
                temps = psutil.sensors_temperatures()
                if "cpu_thermal" in temps and temps["cpu_thermal"]:
                    temp_str = f"{temps['cpu_thermal'][0].current:.1f}°C"
        except Exception:
            pass

        uptime_seconds = int(time.time() - psutil.boot_time())
        uptime_str = format_duration(uptime_seconds)

        model_name = "Raspberry Pi"
        try:
            if os.path.exists("/proc/device-tree/model"):
                with open("/proc/device-tree/model", "r", encoding="utf-8") as f:
                    model_name = f.read().strip("\x00 \n\r")
        except Exception:
            pass

        cpu_val = round(float(cpu_percent), 1)

        return {
            "model": model_name,
            "cpu": f"{cpu_val}%",
            "cpu_percent": cpu_val,
            "temp": temp_str,
            "ram": f"{mem.percent}% | {int(mem.used / (1024*1024))}/{int(mem.total / (1024*1024))}MB",
            "ram_percent": round(mem.percent, 1),
            "disk": f"{disk.percent}%",
            "disk_percent": round(disk.percent, 1),
            "uptime": uptime_str,
            "disk_free_gb": round(disk.free / (1024**3), 1),
            "disk_total_gb": round(disk.total / (1024**3), 1),
            "disk_used_gb": round(disk.used / (1024**3), 1)
        }
    except Exception as e:
        return {"error": str(e)}


# =====================================================================
# 11. SISTEMA HÍBRIDO DE RECOMENDACIONES ("PARA TI") & AUTH SESIONES
# =====================================================================

HISTORY_FILE = SYSTEM_DATA_DIR / "history.json"
USER_SESSION_FILE = SYSTEM_DATA_DIR / "user_session.json"
COOKIES_FILE = Path(os.getenv("PIMUSIC_COOKIES_FILE", str(SYSTEM_DATA_DIR / "cookies.txt"))).resolve()

# Migración automática si existían en la raíz de descargas
for _legacy, _new in [
    (DOWNLOADS_DIR / "cookies.txt", COOKIES_FILE),
    (DOWNLOADS_DIR / "history.json", HISTORY_FILE),
    (DOWNLOADS_DIR / "user_session.json", USER_SESSION_FILE),
]:
    if _legacy.exists() and not _new.exists():
        try:
            shutil.move(str(_legacy), str(_new))
        except Exception:
            pass

def atomic_save_json(file_path: Path, data: Any) -> bool:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile("w", dir=file_path.parent, delete=False, encoding="utf-8") as tf:
            json.dump(data, tf, ensure_ascii=False, indent=2)
            temp_file = tf.name
        os.replace(temp_file, file_path)
        return True
    except Exception as e:
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except Exception:
                pass
        logger.error(f"Error guardando atómicamente {file_path}: {e}")
        return False

class RecordHistoryRequest(BaseModel):
    videoId: str
    title: str
    artist: Optional[str] = ""
    thumbnail: Optional[str] = ""
    duration: Optional[str] = ""

class DeviceCodeRequest(BaseModel):
    client_id: Optional[str] = ""

class DevicePollRequest(BaseModel):
    device_code: str
    client_id: Optional[str] = ""

class UploadSessionRequest(BaseModel):
    cookies_text: str

RECOMMENDATIONS_CACHE = {
    "data": None,
    "timestamp": 0.0
}
RECOMMENDATIONS_CACHE_TTL = 300.0  # 5 minutos de caché

def get_local_history() -> List[Dict[str, Any]]:
    if not HISTORY_FILE.exists():
        return []
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning(f"Error leyendo historial: {e}")
        return []

def save_local_history(history: List[Dict[str, Any]]):
    try:
        atomic_save_json(HISTORY_FILE, history[:50])
    except Exception as e:
        logger.warning(f"Error guardando historial: {e}")

def record_track_history(item: Dict[str, Any]):
    history = get_local_history()
    vid = item.get("videoId")
    if not vid:
        return
    history = [h for h in history if h.get("videoId") != vid]
    item["timestamp"] = time.time()
    history.insert(0, item)
    save_local_history(history)
    RECOMMENDATIONS_CACHE["timestamp"] = 0.0  # Invalidar caché para actualizar

def get_user_session_data() -> Optional[Dict[str, Any]]:
    if not USER_SESSION_FILE.exists():
        return None
    try:
        with open(USER_SESSION_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_user_session_data(session: Dict[str, Any]):
    try:
        atomic_save_json(USER_SESSION_FILE, session)
    except Exception as e:
        logger.warning(f"Error guardando sesión: {e}")

def fetch_feed_search(query: str, max_results: int = 8) -> List[Dict[str, Any]]:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "skip_download": True,
        "noplaylist": True,
    }
    ydl_opts = get_ydl_base_opts()
    ydl_opts.update(opts)
    if COOKIES_FILE.exists():
        ydl_opts["cookiefile"] = str(COOKIES_FILE)

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            res = ydl.extract_info(f"ytsearch{max_results}:{query}", download=False)
            entries = res.get("entries", [])
            results = []
            for e in entries:
                if not e:
                    continue
                vid = e.get("id") or e.get("url")
                if not vid:
                    continue
                vid = extract_video_id(vid)
                dur = format_duration(e.get("duration"))
                results.append({
                    "videoId": vid,
                    "title": e.get("title", "Sin título"),
                    "uploader": e.get("uploader") or e.get("channel") or "YouTube",
                    "thumbnail": e.get("thumbnail") or f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
                    "duration": dur,
                    "badge": "Mix"
                })
            return results
    except Exception as err:
        logger.warning(f"Error buscando recomendaciones para '{query}': {err}")
        return []

@app.get("/api/recommendations/feed")
def get_recommendations_feed():
    now = time.time()
    if RECOMMENDATIONS_CACHE["data"] and (now - RECOMMENDATIONS_CACHE["timestamp"]) < RECOMMENDATIONS_CACHE_TTL:
        return RECOMMENDATIONS_CACHE["data"]

    session = get_user_session_data()
    is_linked = session is not None or COOKIES_FILE.exists()
    account_info = {
        "linked": is_linked,
        "name": session.get("name") if session else ("Cuenta Vinculada" if COOKIES_FILE.exists() else None),
        "avatar": session.get("avatar") if session else None
    }

    history = get_local_history()

    seed_artists = []
    if history:
        for h in history[:6]:
            art = h.get("artist") or ""
            title = h.get("title") or ""
            if not art and " - " in title:
                art = title.split(" - ")[0].strip()
            if art and art.lower() not in [s.lower() for s in seed_artists]:
                seed_artists.append(art)

    sections = []

    # 1. Sección de Mixes Principales
    if seed_artists:
        primary_artist = seed_artists[0]
        mix_query = f"{primary_artist} mix canciones"
        mix_items = fetch_feed_search(mix_query, max_results=8)
        if mix_items:
            sections.append({
                "id": "mix_personal",
                "title": f"Mixes para ti • {primary_artist}",
                "subtitle": "Basado en tu música reciente",
                "items": mix_items
            })

    # 2. Sección Segunda Semilla si existe
    if len(seed_artists) > 1:
        secondary_artist = seed_artists[1]
        sec_items = fetch_feed_search(f"{secondary_artist} canciones", max_results=8)
        if sec_items:
            sections.append({
                "id": "artist_sec",
                "title": f"Porque escuchaste a {secondary_artist}",
                "subtitle": "Canciones y colaboraciones similares",
                "items": sec_items
            })

    # 3. Tendencias Globales y Éxitos del Momento
    trending_items = fetch_feed_search("tendencias musica hits mix", max_results=10)
    if trending_items:
        sections.append({
            "id": "trending_hits",
            "title": "Éxitos y Tendencias",
            "subtitle": "Lo más escuchado ahora",
            "items": trending_items
        })

    # Si el historial estaba vacío, añadir una sección de descubrimiento
    if not seed_artists:
        discovery_items = fetch_feed_search("top exitos mundiales mix musica", max_results=8)
        if discovery_items:
            sections.insert(0, {
                "id": "discovery",
                "title": "Descubrimientos Populares",
                "subtitle": "Comienza a escuchar para personalizar tu feed",
                "items": discovery_items
            })

    hero = None
    all_candidates = []
    for s in sections:
        all_candidates.extend(s.get("items", []))
    if all_candidates:
        first = all_candidates[0]
        hero = {
            "videoId": first["videoId"],
            "title": first["title"],
            "uploader": first["uploader"],
            "thumbnail": first["thumbnail"],
            "duration": first["duration"],
            "reason": "Recomendación Destacada para Ti"
        }

    response_data = {
        "account": account_info,
        "hero": hero,
        "sections": sections,
        "updated_at": datetime.now().strftime("%H:%M")
    }

    RECOMMENDATIONS_CACHE["data"] = response_data
    RECOMMENDATIONS_CACHE["timestamp"] = now
    return response_data

@app.post("/api/history/record")
def record_history(req: RecordHistoryRequest):
    record_track_history({
        "videoId": req.videoId,
        "title": req.title,
        "artist": req.artist or "",
        "thumbnail": req.thumbnail or f"https://i.ytimg.com/vi/{req.videoId}/hqdefault.jpg",
        "duration": req.duration or "3:30"
    })
    return {"status": "success", "message": "Historial registrado"}

@app.get("/api/auth/status")
def get_auth_status():
    session = get_user_session_data()
    cookies_present = COOKIES_FILE.exists()
    return {
        "linked": session is not None or cookies_present,
        "method": "oauth" if session else ("cookies" if cookies_present else "none"),
        "name": session.get("name") if session else ("Sesión por cookies" if cookies_present else None),
        "avatar": session.get("avatar") if session else None
    }

@app.post("/api/auth/device-code")
def request_device_code(req: DeviceCodeRequest):
    client_id = req.client_id.strip() if req.client_id else os.getenv("GOOGLE_DEVICE_CLIENT_ID", "")
    if not client_id:
        return {
            "status": "config_needed",
            "message": "Introduce tu Client ID de Google Cloud (tipo TVs and Limited Input) o importa tu archivo de sesión (cookies.txt)."
        }
    try:
        resp = requests.post(
            "https://oauth2.googleapis.com/device/code",
            data={
                "client_id": client_id,
                "scope": "https://www.googleapis.com/auth/youtube.readonly"
            },
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            return {
                "status": "ready",
                "device_code": data.get("device_code"),
                "user_code": data.get("user_code"),
                "verification_url": data.get("verification_url", "https://www.google.com/device"),
                "expires_in": data.get("expires_in", 1800),
                "interval": data.get("interval", 5)
            }
        else:
            return {
                "status": "error",
                "message": f"Google rechazó la solicitud ({resp.status_code}): {resp.text}"
            }
    except Exception as err:
        return {"status": "error", "message": f"Error de conexión: {str(err)}"}

@app.post("/api/auth/device-poll")
def poll_device_code(req: DevicePollRequest):
    client_id = req.client_id.strip() if req.client_id else os.getenv("GOOGLE_DEVICE_CLIENT_ID", "")
    try:
        resp = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": client_id,
                "device_code": req.device_code,
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code"
            },
            timeout=10
        )
        data = resp.json()
        if resp.status_code == 200 and "access_token" in data:
            save_user_session_data({
                "access_token": data["access_token"],
                "refresh_token": data.get("refresh_token", ""),
                "name": "Usuario de YouTube",
                "avatar": None,
                "linked_at": time.time()
            })
            RECOMMENDATIONS_CACHE["timestamp"] = 0.0
            return {"status": "success", "message": "¡Cuenta vinculada exitosamente!"}
        elif data.get("error") == "authorization_pending":
            return {"status": "pending"}
        else:
            return {"status": "error", "message": data.get("error_description", data.get("error"))}
    except Exception as err:
        return {"status": "error", "message": str(err)}

@app.post("/api/auth/upload-session")
def upload_session(req: UploadSessionRequest):
    content = req.cookies_text.strip()
    if not content:
        raise HTTPException(status_code=400, detail="El contenido de cookies está vacío")
    try:
        with open(COOKIES_FILE, "w", encoding="utf-8") as f:
            f.write(content)
        RECOMMENDATIONS_CACHE["timestamp"] = 0.0
        return {"status": "success", "message": "Archivo de sesión guardado correctamente"}
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Error guardando cookies: {err}")

@app.post("/api/auth/logout")
def logout_auth():
    if USER_SESSION_FILE.exists():
        try:
            os.remove(USER_SESSION_FILE)
        except Exception:
            pass
    if COOKIES_FILE.exists():
        try:
            os.remove(COOKIES_FILE)
        except Exception:
            pass
    RECOMMENDATIONS_CACHE["timestamp"] = 0.0
    return {"status": "success", "message": "Sesión cerrada correctamente"}


# =====================================================================
# 8. DIAGNÓSTICO Y REGISTROS PERSISTENTES DEL SISTEMA
# =====================================================================

@app.get("/api/logs")
def get_system_logs(
    lines: int = Query(150, ge=10, le=1000),
    level: Optional[str] = Query(None, description="Filtro opcional: ERROR, WARNING, INFO")
):
    """Devuelve los últimos registros persistentes del servidor con métricas de errores."""
    if not LOG_FILE.exists():
        return {
            "logs": [],
            "total_lines": 0,
            "error_count": 0,
            "warning_count": 0,
            "file_size": "0 B",
            "file_path": str(LOG_FILE)
        }

    try:
        with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
    except Exception as e:
        logger.error(f"Error al leer archivo de logs {LOG_FILE}: {e}")
        raise HTTPException(status_code=500, detail=f"No se pudo leer el archivo de registros: {e}")

    total_lines = len(all_lines)
    error_count = sum(1 for l in all_lines if "[ERROR]" in l)
    warning_count = sum(1 for l in all_lines if "[WARNING]" in l)
    file_size = format_filesize(LOG_FILE.stat().st_size)

    filtered = all_lines
    if level and level.upper() != "ALL":
        lvl_tag = f"[{level.upper()}]"
        filtered = [l for l in all_lines if lvl_tag in l]

    tail_lines = [l.rstrip("\r\n") for l in filtered[-lines:]]

    return {
        "logs": tail_lines,
        "total_lines": total_lines,
        "error_count": error_count,
        "warning_count": warning_count,
        "file_size": file_size,
        "file_path": str(LOG_FILE)
    }

@app.delete("/api/logs")
def clear_system_logs():
    """Limpia el archivo de registros del servidor de forma segura."""
    try:
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [INFO] PiMusicServer: Registros reiniciados desde la interfaz web.\n")
        logger.info("Registros del sistema reiniciados a petición del usuario.")
        return {"status": "success", "message": "Registros limpiados correctamente."}
    except Exception as e:
        logger.error(f"Error al limpiar archivo de logs: {e}")
        raise HTTPException(status_code=500, detail=f"No se pudieron limpiar los registros: {e}")


if STATIC_DIST.exists():
    @app.api_route("/{full_path:path}", methods=["GET", "HEAD"])
    def serve_frontend_spa(full_path: str):
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Endpoint no encontrado")
        target_file = STATIC_DIST / full_path
        if target_file.is_file():
            return FileResponse(target_file)
        index_file = STATIC_DIST / "index.html"
        if index_file.exists():
            return FileResponse(index_file)
        raise HTTPException(status_code=404, detail="Recurso no encontrado")


if __name__ == "__main__":
    import uvicorn
    logger.info(f"Iniciando PiMusic Server en {HOST}:{PORT}")
    logger.info(f"Directorio de Descargas: {DOWNLOADS_DIR}")
    logger.info(f"CORS Origins permitidos: {CORS_ORIGINS}")
    print(f"[*] PiMusic High-Performance Server escuchando en http://{HOST}:{PORT}")
    print(f"[*] Directorio de almacenamiento: {DOWNLOADS_DIR}")
    uvicorn.run(app, host=HOST, port=PORT)
