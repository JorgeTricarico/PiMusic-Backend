import os
import sys
import shutil
import pytest
from pathlib import Path
from unittest.mock import MagicMock

# Añadir el directorio server al path de Python
server_dir = Path(__file__).parent.parent.resolve()
if str(server_dir) not in sys.path:
    sys.path.insert(0, str(server_dir))

from main import app, media_cache, search_cache, DOWNLOADS_DIR
from fastapi.testclient import TestClient

@pytest.fixture(autouse=True)
def clean_caches():
    """Limpia cachés en memoria antes de cada prueba para evitar efectos secundarios."""
    with media_cache._lock:
        media_cache._cache.clear()
    with search_cache._lock:
        search_cache._cache.clear()
    yield

@pytest.fixture
def tmp_downloads_dir(tmp_path, monkeypatch):
    """Aísla el directorio de descargas a una carpeta temporal protegida."""
    test_downloads = tmp_path / "test_downloads"
    test_downloads.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("main.DOWNLOADS_DIR", test_downloads)
    yield test_downloads
    shutil.rmtree(test_downloads, ignore_errors=True)

@pytest.fixture
def client():
    """Cliente de pruebas sincrónico para FastAPI usando TestClient."""
    with TestClient(app) as test_client:
        yield test_client

@pytest.fixture
def dummy_video_info():
    """Estructura mock idéntica a la emitida por yt-dlp extract_info."""
    return {
        "id": "abc12345678",
        "title": "Prueba de Sonido PiMusic",
        "uploader": "Tester Pi",
        "duration": 180,
        "thumbnail": "https://i.ytimg.com/vi/abc12345678/hqdefault.jpg",
        "view_count": 125000,
        "formats": [
            {
                "format_id": "18",
                "url": "https://googlevideo.com/videoplayback_720p_prog.mp4",
                "vcodec": "avc1.42001E",
                "acodec": "mp4a.40.2",
                "ext": "mp4",
                "height": 720,
                "tbr": 1200,
                "protocol": "https"
            },
            {
                "format_id": "135",
                "url": "https://googlevideo.com/videoplayback_480p_dash.mp4",
                "vcodec": "avc1.4d401e",
                "acodec": "none",
                "ext": "mp4",
                "height": 480,
                "tbr": 800,
                "protocol": "https"
            },
            {
                "format_id": "140",
                "url": "https://googlevideo.com/audioplayback_aac.m4a",
                "vcodec": "none",
                "acodec": "mp4a.40.2",
                "ext": "m4a",
                "abr": 128,
                "protocol": "https"
            }
        ]
    }
