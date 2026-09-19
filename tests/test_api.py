import os
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from fastapi import HTTPException
from main import (
    ALLOWED_MEDIA_EXTENSIONS,
    SYSTEM_DATA_DIR,
    atomic_save_json,
    extract_and_cache_info,
    extract_video_id
)

def test_extract_video_id():
    # ID directo de 11 caracteres
    assert extract_video_id("dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    # URL estándar de YouTube
    assert extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    # URL corta youtu.be
    assert extract_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    # URL con parámetros adicionales
    assert extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=42s") == "dQw4w9WgXcQ"

def test_terminal_access_is_forbidden(client):
    """Verifica que el acceso por terminal remoto esté estrictamente bloqueado con 403."""
    response = client.post("/api/terminal", json={"command": "ls -la"})
    assert response.status_code == 403
    data = response.json()
    assert "deshabilitado" in data["detail"].lower()

def test_library_path_traversal_protection(client, tmp_downloads_dir):
    """Verifica que no sea posible escapar del directorio de descargas."""
    evil_paths = [
        "../etc/passwd",
        "..\\..\\windows\\system32\\calc.exe",
        "/etc/shadow",
        "../../system/cookies.txt",
        "%2e%2e%2fetc%2fpasswd"
    ]
    for path in evil_paths:
        # Delete
        del_resp = client.delete(f"/api/library/{path}")
        assert del_resp.status_code in (400, 403, 404, 405)
        # Stream
        stream_resp = client.get(f"/api/library/stream/{path}")
        assert stream_resp.status_code in (400, 403, 404, 405)
        # Download
        down_resp = client.get(f"/api/library/download/{path}")
        assert down_resp.status_code in (400, 403, 404, 405)

def test_library_disallowed_file_types(client, tmp_downloads_dir):
    """Verifica que no se puedan descargar, transmitir ni borrar archivos que no sean multimedia."""
    system_file = tmp_downloads_dir / "cookies.txt"
    system_file.write_text("sensible_cookie_data=12345", encoding="utf-8")

    # Intentar descargar
    down_resp = client.get("/api/library/download/cookies.txt")
    assert down_resp.status_code == 403

    # Intentar streaming
    stream_resp = client.get("/api/library/stream/cookies.txt")
    assert stream_resp.status_code == 403

    # Intentar borrar
    del_resp = client.delete("/api/library/cookies.txt")
    assert del_resp.status_code == 403
    assert system_file.exists(), "El archivo protegido no debe ser borrado"

def test_library_list_and_delete_valid_media(client, tmp_downloads_dir):
    """Verifica el listado de archivos multimedia válidos y su eliminación segura."""
    # Crear archivo de audio válido
    valid_audio = tmp_downloads_dir / "cancion_prueba.mp3"
    valid_audio.write_bytes(b"ID3" + b"\x00" * 1024)

    # Crear archivo de video válido
    valid_video = tmp_downloads_dir / "video_prueba.mp4"
    valid_video.write_bytes(b"\x00\x00\x00 ftypmp42" + b"\x00" * 2048)

    # Crear archivo de texto (no debe aparecer en el listado)
    hidden_file = tmp_downloads_dir / "notas.txt"
    hidden_file.write_text("No soy un medio", encoding="utf-8")

    # 1. Listar biblioteca
    response = client.get("/api/library")
    assert response.status_code == 200
    data = response.json()
    filenames = [f["filename"] for f in data["files"]]
    
    assert "cancion_prueba.mp3" in filenames
    assert "video_prueba.mp4" in filenames
    assert "notas.txt" not in filenames

    # Verificar metadatos
    audio_entry = next(f for f in data["files"] if f["filename"] == "cancion_prueba.mp3")
    assert audio_entry["type"] == "audio"
    assert audio_entry["ext"] == "mp3"
    assert audio_entry["stream_url"] == "/api/library/stream/cancion_prueba.mp3"

    # 2. Borrar archivo válido
    del_resp = client.delete("/api/library/cancion_prueba.mp3")
    assert del_resp.status_code == 200
    assert del_resp.json()["status"] == "deleted"
    assert not valid_audio.exists()

    # 3. Intentar borrar de nuevo debe dar 404
    del_again = client.delete("/api/library/cancion_prueba.mp3")
    assert del_again.status_code == 404

def test_atomic_save_json(tmp_path):
    """Verifica que atomic_save_json guarde datos de forma atómica y sin corrupción."""
    target_file = tmp_path / "test_data.json"
    data = {"key": "valor_de_prueba", "items": [1, 2, 3]}

    success = atomic_save_json(target_file, data)
    assert success is True
    assert target_file.exists()

    import json
    loaded = json.loads(target_file.read_text(encoding="utf-8"))
    assert loaded == data

def test_info_endpoint_mocked(client, monkeypatch):
    """Verifica que /api/info devuelva la estructura completa de formatos y calidades."""
    mock_info = {
        "id": "abc12345678",
        "title": "Prueba de Sonido PiMusic",
        "uploader": "Tester Pi",
        "duration": "03:00",
        "duration_seconds": 180,
        "thumbnail": "https://i.ytimg.com/vi/abc12345678/hqdefault.jpg",
        "views": 125000,
        "url": "https://www.youtube.com/watch?v=abc12345678",
        "video_options": [
            {"quality": "720p", "height": 720, "ext": "mp4", "badge": "HD", "description": "Video MP4 (720p) con audio", "approx_size": "~39 MB"},
            {"quality": "480p", "height": 480, "ext": "mp4", "badge": "SD", "description": "Video MP4 (480p) con audio", "approx_size": "~18 MB"}
        ],
        "audio_options": [
            {"quality": "mp3_320", "ext": "mp3", "badge": "320 kbps", "description": "Audio MP3 (Máxima calidad)", "approx_size": "~7 MB"}
        ]
    }

    monkeypatch.setattr("main.extract_and_cache_info", lambda url: mock_info)

    response = client.get("/api/info?url=https://www.youtube.com/watch?v=abc12345678")
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "abc12345678"
    assert body["title"] == "Prueba de Sonido PiMusic"
    assert len(body["video_options"]) == 2
    assert body["video_options"][0]["quality"] == "720p"
    assert body["video_options"][1]["quality"] == "480p"
    assert len(body["audio_options"]) == 1

def test_stream_preview_endpoint(client, monkeypatch):
    """Verifica que /api/stream/{video_id} construya URLs de streaming proxy correctas."""
    mock_info = {
        "title": "Video Musical",
        "video_options": [],
        "audio_options": []
    }
    monkeypatch.setattr("main.extract_and_cache_info", lambda v_id: mock_info)

    # Preview de Audio
    resp_audio = client.get("/api/stream/abc12345678?type=audio")
    assert resp_audio.status_code == 200
    assert resp_audio.json()["stream_url"] == "/api/stream_media/abc12345678?type=audio"

    # Preview de Video con calidad 480p
    resp_video = client.get("/api/stream/abc12345678?type=video&quality=480p")
    assert resp_video.status_code == 200
    assert resp_video.json()["stream_url"] == "/api/stream_media/abc12345678?type=video&quality=480p"

def test_stream_media_head_request(client):
    """Verifica que el método HEAD en /api/stream_media/{video_id} devuelva cabeceras de streaming HTTP."""
    resp_audio = client.head("/api/stream_media/abc12345678?type=audio")
    assert resp_audio.status_code == 200
    assert "audio" in resp_audio.headers["content-type"]
    assert resp_audio.headers.get("accept-ranges") == "bytes"

    resp_video = client.head("/api/stream_media/abc12345678?type=video&quality=720p")
    assert resp_video.status_code == 200
    assert "video/mp4" in resp_video.headers["content-type"]

def test_live_stream_rejection(monkeypatch):
    """Verifica que las transmisiones en vivo lancen un error HTTP 400 antes de intentar descargar."""
    live_mock = {
        "id": "live_video_123",
        "is_live": True,
        "title": "Transmisión en directo"
    }

    class MockYDL:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def extract_info(self, url, download=False):
            return live_mock

    monkeypatch.setattr("yt_dlp.YoutubeDL", MockYDL)

    with pytest.raises(HTTPException) as exc_info:
        extract_and_cache_info("live_video_123")
    assert exc_info.value.status_code == 400
    assert "en vivo" in exc_info.value.detail.lower()
