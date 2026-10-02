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

def test_terminal_execution(client):
    """Verifica que el panel de terminal de desarrollo ejecute comandos del sistema."""
    cmd = "echo terminal_ok"
    response = client.post("/api/terminal", json={"command": cmd})
    assert response.status_code == 200
    data = response.json()
    assert "output" in data
    assert "exit_code" in data
    assert "terminal_ok" in data["output"]

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

    # 2. Borrar archivo válido vía DELETE
    del_resp = client.delete("/api/library/cancion_prueba.mp3")
    assert del_resp.status_code == 200
    assert del_resp.json()["status"] in ("ok", "deleted")
    assert del_resp.json()["message"] == "Archivo eliminado con éxito"
    assert not valid_audio.exists()

    # 3. Intentar borrar de nuevo debe dar 404
    del_again = client.delete("/api/library/cancion_prueba.mp3")
    assert del_again.status_code == 404

    # 4. Borrar archivo válido vía POST /api/library/delete
    post_del_resp = client.post("/api/library/delete", json={"filename": "video_prueba.mp4"})
    assert post_del_resp.status_code == 200
    assert post_del_resp.json()["status"] == "ok"
    assert not valid_video.exists()

    # 5. Borrar con nombre inexistente vía POST da 404
    post_del_404 = client.post("/api/library/delete", json={"filename": "inexistente.mp3"})
    assert post_del_404.status_code == 404

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


def test_security_headers_present(client):
    """Verifica que todas las respuestas HTTP contengan cabeceras de seguridad estándar."""
    response = client.get("/")
    assert response.headers.get("X-Content-Type-Options") == "nosniff"
    assert response.headers.get("X-Frame-Options") == "SAMEORIGIN"
    assert "strict-origin-when-cross-origin" in response.headers.get("Referrer-Policy", "")


def test_rate_limiter_triggers_429(client):
    """Verifica que el limitador de tasa responda con HTTP 429 cuando se excede la cuota."""
    from main import rate_limiter
    rate_limiter.clients.clear()
    
    test_ip = "192.168.100.99"
    # Simular saturación de solicitudes
    for _ in range(rate_limiter.max_requests):
        rate_limiter.is_allowed(test_ip)
    
    # La siguiente petición debe ser rechazada
    assert not rate_limiter.is_allowed(test_ip)
    
    # En endpoint real pasando X-Forwarded-For
    response = client.get("/api/search?q=test", headers={"X-Forwarded-For": test_ip})
    assert response.status_code == 429
    assert "Demasiadas solicitudes" in response.json()["detail"]


def test_playback_control_actions(client):
    """Verifica que las acciones de control de reproducción respondan correctamente."""
    # 1. Pause
    resp_pause = client.post("/api/playback/action", json={"action": "pause"})
    assert resp_pause.status_code == 200
    assert resp_pause.json()["state"] == "paused"

    # 2. Play / Resume vía /api/control
    resp_play = client.post("/api/control", json={"action": "play"})
    assert resp_play.status_code == 200
    assert resp_play.json()["state"] == "playing"

    # 3. Toggle
    resp_toggle = client.post("/api/playback/action", json={"action": "toggle"})
    assert resp_toggle.status_code == 200
    assert resp_toggle.json()["state"] == "paused"

    # 4. Volume
    resp_vol = client.post("/api/playback/action", json={"action": "volume", "value": "75"})
    assert resp_vol.status_code == 200
    assert resp_vol.json()["volume"] == 75.0
    assert resp_vol.json()["state"] == "volume_changed"

    # 5. Seek
    resp_seek = client.post("/api/playback/action", json={"action": "seek", "value": "120"})
    assert resp_seek.status_code == 200
    assert resp_seek.json()["position"] == 120.0
    assert resp_seek.json()["state"] == "seeking"

    # 6. Stop
    resp_stop = client.post("/api/control", json={"action": "stop"})
    assert resp_stop.status_code == 200
    assert resp_stop.json()["state"] == "idle"

    # 7. Status endpoint
    resp_status = client.get("/api/status")
    assert resp_status.status_code == 200
    assert resp_status.json()["state"] == "idle"


def test_app_telemetry_logs(client, tmp_path, monkeypatch):
    """Verifica el flujo completo de telemetría y logs de la aplicación Android."""
    import logging
    from main import app_logger
    test_log_file = tmp_path / "test_app_telemetry.log"
    monkeypatch.setattr("main.APP_LOG_FILE", test_log_file)

    test_handler = logging.FileHandler(str(test_log_file), encoding="utf-8")
    test_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    old_handlers = app_logger.handlers[:]
    app_logger.handlers = [test_handler]

    try:
        # 1. Enviar log simple
        resp1 = client.post("/api/logs/app", json={
            "level": "INFO",
            "tag": "PlayerActivity",
            "message": "Pista iniciada con éxito",
            "device": "Pixel 7"
        })
        assert resp1.status_code == 200
        assert resp1.json()["status"] == "ok"
        assert resp1.json()["saved_entries"] == 1

        # 2. Enviar batch de logs con estructura BatchAppLogsRequest
        resp2 = client.post("/api/logs/app", json={
            "device_id": "rpi_tester",
            "app_version": "2.0.0",
            "logs": [
                {"timestamp": 123456, "level": "WARNING", "tag": "Network", "message": "Buffer bajo", "details": "none"},
                {"timestamp": 123457, "level": "ERROR", "tag": "Decoder", "message": "Fallo en frame", "details": "dropped 5 frames"}
            ]
        })
        assert resp2.status_code == 200
        assert resp2.json()["saved_entries"] == 2

        # 3. Consultar logs con GET /api/logs/app
        resp_get = client.get("/api/logs/app?lines=50")
        assert resp_get.status_code == 200
        data = resp_get.json()
        assert data["total_lines"] >= 3
        assert any("PlayerActivity" in l for l in data["logs"])
        assert any("Buffer bajo" in l for l in data["logs"])

        # 4. Limpiar logs con DELETE /api/logs/app
        resp_del = client.delete("/api/logs/app")
        assert resp_del.status_code == 200
        assert resp_del.json()["status"] == "success"

        # 5. Comprobar que los registros se reiniciaron
        resp_check = client.get("/api/logs/app")
        assert resp_check.status_code == 200
        assert len(resp_check.json()["logs"]) == 1
        assert "reiniciados" in resp_check.json()["logs"][0].lower()
    finally:
        test_handler.close()
        app_logger.handlers = old_handlers



def test_play_device_profiles(client):
    """Verifica que el endpoint /api/play configure adecuadamente los perfiles RPi3 y RPi5."""
    # Perfil Raspberry Pi 3 explícito
    resp_rpi3 = client.post("/api/play", json={
        "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "title": "Never Gonna Give You Up",
        "target_device": "rpi3",
        "quality": "480p"
    })
    assert resp_rpi3.status_code == 200
    assert resp_rpi3.json()["status"] == "ok"
    assert resp_rpi3.json()["device_profile"] == "rpi3"

    # Perfil Raspberry Pi 5 / estándar
    resp_rpi5 = client.post("/api/play", json={
        "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "title": "Never Gonna Give You Up",
        "target_device": "rpi5",
        "quality": "1080p"
    })
    assert resp_rpi5.status_code == 200
    assert resp_rpi5.json()["status"] == "ok"
    assert resp_rpi5.json()["device_profile"] == "rpi5"


def test_ydl_opts_player_client_avoids_403():
    """Verifica que get_ydl_base_opts configure clientes que no requieran PO-Token restrictivo."""
    from main import get_ydl_base_opts
    opts = get_ydl_base_opts()
    clients = opts.get("extractor_args", {}).get("youtube", {}).get("player_client", [])
    assert "android_vr" not in clients, "android_vr genera HTTP 403 Forbidden por exigir GVS PO Token"
    assert "android" in clients
    assert "visionos" in clients or "web" in clients


def test_save_server_and_download_to_server_aliases(client):
    """Verifica que los endpoints /api/save-server y /api/download_to_server encolen la descarga."""
    # POST /api/save-server
    resp1 = client.post("/api/save-server", json={
        "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "format_type": "audio",
        "quality": "mp3_320"
    })
    assert resp1.status_code == 200
    assert resp1.json()["status"] == "queued"

    # POST /api/download_to_server (alias documentado en README)
    resp2 = client.post("/api/download_to_server", json={
        "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "format_type": "video",
        "quality": "720p"
    })
    assert resp2.status_code == 200
    assert resp2.json()["status"] == "queued"


def test_library_download_with_query_params_and_auto_extension(client, tmp_downloads_dir):
    """Verifica que /api/library/download sanitice query params y resuelva archivos sin extensión explícita."""
    test_file = tmp_downloads_dir / "pista_musical.mp3"
    test_file.write_bytes(b"ID3" + b"\x00" * 512)

    # 1. Petición con query params (ej. ?download=1&auth=xyz) que antes causaba 403
    resp_query = client.get("/api/library/download/pista_musical.mp3?download=1&auth=xyz")
    assert resp_query.status_code == 200
    assert "pista_musical.mp3" in resp_query.headers.get("Content-Disposition", "")

    # 2. Petición sin extensión en la URL -> debe resolver al archivo físico existente
    resp_no_ext = client.get("/api/library/download/pista_musical")
    assert resp_no_ext.status_code == 200
    assert "pista_musical.mp3" in resp_no_ext.headers.get("Content-Disposition", "")

    # 3. Streaming con query params
    resp_stream = client.get("/api/library/stream/pista_musical.mp3?token=456")
    assert resp_stream.status_code == 200


@pytest.mark.anyio
async def test_stream_media_process_emits_chunks_without_deadlock():
    """Verifica que stream_media_process consuma un subproceso y devuelva chunks sin bloquearse en ASGI."""
    import sys
    from main import stream_media_process
    # Subproceso que emite datos binarios y termina
    cmd = [sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'HELLO_STREAM_CHUNK' * 10)"]
    chunks = []
    async for chunk in stream_media_process(cmd, chunk_size=32):
        chunks.append(chunk)
    assert len(chunks) > 0
    assert b"".join(chunks) == b"HELLO_STREAM_CHUNK" * 10


def test_api_resolve_avoids_googlevideo_403_and_routes_to_server_proxy(client, monkeypatch, tmp_downloads_dir):
    """Verifica que /api/resolve enrute a través del proxy del backend en lugar de dar URLs directas de googlevideo propensas a 403."""
    mock_info = {
        "title": "Video de Putin",
        "duration": "19:40",
        "duration_seconds": 1180,
        "progressive_streams": {
            "360p": {"url": "https://rr3---sn-uxaxjxougv-x1xll.googlevideo.com/videoplayback?itag=18&ip=2800:810:..."}
        }
    }
    monkeypatch.setattr("main.extract_and_cache_info", lambda v_id: mock_info)

    # 1. Petición para video de YouTube -> DEBE devolver URL apuntando al proxy local (/api/stream_media/)
    resp = client.post("/api/resolve", json={
        "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "type": "video",
        "quality": "480p"
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "/api/stream_media/dQw4w9WgXcQ?type=video&quality=480p" in data["stream_url"]
    assert "googlevideo.com" not in data["stream_url"], "La URL no debe exponer googlevideo.com directo por el IP-lock"
    assert data["title"] == "Video de Putin"
    assert data["type"] == "video"

    # 2. Petición para audio
    resp_audio = client.post("/api/resolve", json={
        "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "type": "audio"
    })
    assert resp_audio.status_code == 200
    assert "/api/stream_media/dQw4w9WgXcQ?type=audio" in resp_audio.json()["stream_url"]

    # 3. Petición para archivo local existente
    local_song = tmp_downloads_dir / "cancion_local.mp3"
    local_song.write_bytes(b"MP3DATA")
    resp_local = client.post("/api/resolve", json={"url": "cancion_local.mp3"})
    assert resp_local.status_code == 200
    assert "/api/library/stream/cancion_local.mp3" in resp_local.json()["stream_url"]
    assert resp_local.json().get("is_local") is True




