# Instrucciones para el Backend Python (Raspberry Pi)

Este documento describe la API HTTP que la aplicación Android y Android TV espera encontrar en el servidor Raspberry Pi para controlar la reproducción de música y video (vía MPV) y para resolver flujos directos progresivos compatibles con MediaPlayer y ExoPlayer.

## Requisitos en la Raspberry Pi
- **Python 3.9+**
- **yt-dlp**: `pip install --upgrade yt-dlp`
- **mpv**: `sudo apt install mpv`
- **fastapi & uvicorn**: `pip install fastapi uvicorn psutil`

---

## Endpoints Principales

### 1. Reproducción en la Raspberry Pi (Salida HDMI / Jack de audio vía MPV)
`POST /api/play`
- **Body**: 
```json
{
  "url": "https://www.youtube.com/watch?v=...",
  "title": "Nombre de la pista",
  "id": "video_id",
  "format": "video|audio",
  "target_device": "rpi3|rpi5|auto",
  "quality": "low|360p|480p|720p|1080p|best"
}
```
- **Descripción**: Inicia la reproducción local en la Raspberry Pi usando mpv conectado al socket IPC (`/tmp/mpvsocket`). 
  - **Exportación Automática de Pantalla (HDMI / TV):** Configura automáticamente las variables de entorno de pantalla para asegurar que el reproductor se dibuje a pantalla completa en la TV/HDMI aunque el servidor corra bajo `systemd` o como demonio:
    ```python
    run_env = os.environ.copy()
    if "DISPLAY" not in run_env:
        run_env["DISPLAY"] = ":0"
    if "XDG_RUNTIME_DIR" not in run_env and os.path.exists("/run/user/1000"):
        run_env["XDG_RUNTIME_DIR"] = "/run/user/1000"
    if "WAYLAND_DISPLAY" not in run_env and os.path.exists("/run/user/1000/wayland-0"):
        run_env["WAYLAND_DISPLAY"] = "wayland-0"
    ```
  - **Con `mpv --fs` (Recomendado):** La Pi abre la ventana de video directamente a pantalla completa en la TV conectada por HDMI con aceleración por hardware.
    - **Raspberry Pi 5**: Perfil de alto rendimiento con decodificación 1080p/4K 60fps y búfer amplio (128MB).
    - **Raspberry Pi 3 / Hardware Legacy**: Perfil ligero que limita la resolución a un máximo de 720p/480p, activa `--hwdec=auto-safe`, `--framedrop=vo` y limita el búfer a 32MB para evitar congelamientos o sobrecalentamiento.
  - **Alternativa con Navegador (Chromium Kiosk):** Si prefieres abrir el navegador web de la Pi con YouTube en lugar de mpv, puedes sustituir la llamada en el backend por:
    ```bash
    chromium-browser --kiosk --autoplay-policy=no-user-gesture-required "https://www.youtube.com/watch?v=ID"
    ```
- **Respuesta**:
```json
{
  "status": "ok",
  "message": "Playback started",
  "track": "Nombre de la pista",
  "device": "rpi3|rpi5",
  "device_profile": "rpi3|rpi5",
  "video_mode": true
}
```

`POST /api/playback/action` y `POST /api/control`
- **Body**:
```json
{
  "action": "pause|resume|play|toggle|stop|seek|volume|next|prev",
  "value": "120"
}
```
- **Descripción**: Control unificado de reproducción. Utiliza comandos JSON IPC ultra-rápidos mediante el socket `/tmp/mpvsocket` (con fallback transparente a señales de proceso `SIGSTOP`/`SIGCONT`/`SIGTERM`).
  - `action="pause"`: Pausa el reproductor (`{"status": "ok", "state": "paused"}`).
  - `action="play"` o `"resume"`: Reanuda la reproducción (`{"status": "ok", "state": "playing"}`).
  - `action="toggle"`: Alterna el estado entre pausa y reproducción.
  - `action="stop"`: Detiene y cierra limpiamente el reproductor (`{"status": "ok", "state": "idle"}`).
  - `action="seek"` con `value="segundos"`: Salta instantáneamente a la posición indicada.
  - `action="volume"` con `value="0-100"`: Ajusta el nivel de volumen.
- **Respuesta**:
```json
{
  "status": "ok",
  "state": "playing|paused|idle"
}
```

### 2. Estado de Reproducción
`GET /api/status`
- **Descripción**: Devuelve el estado actual del reproductor y tareas.
- **Respuesta**:
```json
{
  "state": "playing|paused|idle",
  "current_track": "Nombre del video",
  "task_status": "IDLE",
  "download_progress": 0,
  "error": null,
  "position": 0.0,
  "volume": 1.0
}
```

### 3. Resolución de Streaming y URLs (Para Celular y Android TV)
`POST /api/resolve` (Recomendado - Extracción de CDN Directo Multiplexado)
- **Body**:
```json
{
  "url": "https://www.youtube.com/watch?v=...",
  "id": "video_id",
  "type": "video|audio",
  "quality": "480p"
}
```
- **Descripción**: Resuelve el flujo multimedia garantizando máxima compatibilidad con `MediaPlayer` y `ExoPlayer` en Android y Android TV. Enruta la reproducción a través del proxy del servidor (`/api/stream_media/{videoId}?type={type}&quality={quality}`) para **anular completamente los errores `HTTP 403 Forbidden`** causados por:
  1. El amarre criptográfico de dirección IP de YouTube (`&ip=...` en las URLs de `googlevideo.com`), que deniega el acceso cuando el celular se conecta desde otra IP o red móvil.
  2. La exigencia de GVS PO-Token en clientes restringidos (como `android_vr`).
  3. Desajustes en encabezados `User-Agent`.
  Además, incluye soporte completo para archivos locales de la biblioteca y metadatos (título, duración).
- **Respuesta Esperada**:
```json
{
  "status": "ok",
  "stream_url": "http://192.168.1.50:5000/api/stream_media/ID?type=video&quality=480p",
  "direct_url": "http://192.168.1.50:5000/api/stream_media/ID?type=video&quality=480p",
  "url": "http://192.168.1.50:5000/api/stream_media/ID?type=video&quality=480p",
  "server_url": "http://192.168.1.50:5000/api/stream_media/ID?type=video&quality=480p",
  "video_id": "ID",
  "title": "Título del video",
  "duration": "19:40",
  "duration_seconds": 1180,
  "format": "mp4",
  "type": "video"
}
```

`GET /api/stream_media/{videoId}?type={video|audio}&quality={quality}` (Fallback Stream con HTTP Range 206)
- **Descripción**: Streaming con proxy directo por chunks y soporte de HTTP Range 206 para seeking instantáneo y remuxing en vivo a fMP4 sin recodificar. Cuenta con detección automática de 403/404/410 upstream para transicionar limpiamente al remuxer local FFmpeg.

`GET /api/download?url={url}&quality={calidad}&type={tipo}` y `GET /api/download_stream`
- **Descripción**: Descarga directa de archivos de YouTube en MP4 o MP3/M4A con multiplexado al vuelo vía FFmpeg y cabecera `Content-Disposition: attachment`.

`POST /api/save-server` y `POST /api/download_to_server`
- **Body**:
```json
{
  "url": "https://www.youtube.com/watch?v=...",
  "format_type": "video|audio",
  "quality": "720p|mp3_320"
}
```
- **Descripción**: Encola la descarga física del archivo en el almacenamiento local de la Raspberry Pi en segundo plano.

### 4. Telemetría y Terminal
`GET /api/telemetry`
- **Descripción**: Devuelve métricas de hardware de la Raspberry Pi en formato dual (compatible tanto con modelos numéricos como string de Retrofit).
- **Respuesta**:
```json
{
  "model": "Raspberry Pi 5 Model B Rev 1.0",
  "cpu": "15.4%",
  "cpu_percent": 15.4,
  "temp": "48.2°C",
  "ram": "34.0% | 1566/3991MB",
  "ram_percent": 34.0,
  "disk": "62.1%",
  "disk_percent": 62.1,
  "uptime": "37:13:26",
  "disk_free_gb": 13.0,
  "disk_total_gb": 56.8,
  "disk_used_gb": 41.4
}
```

`POST /api/terminal`
- **Body**:
```json
{
  "command": "uname -a"
}
```
- **Descripción**: Ejecuta un comando del sistema para el panel de desarrollo y devuelve la salida.
- **Respuesta**:
```json
{
  "output": "Linux raspberrypi5 ...\n",
  "exit_code": 0
}
```

### 5. Recepción de Telemetría y Logs desde la App Android
`POST /api/logs/app`
- **Body**:
```json
{
  "level": "INFO|WARNING|ERROR|DEBUG",
  "tag": "PlayerActivity",
  "message": "Mensaje de diagnóstico o evento",
  "device": "Pixel 7 / Android TV"
}
```
*(También soporta batch con `{"logs": [...]}` o array `[...]`)*
- **Descripción**: Registra eventos y diagnósticos enviados desde la aplicación Android en el archivo `app_telemetry.log` del servidor de la Raspberry Pi.
- **Respuesta**:
```json
{
  "status": "ok",
  "message": "Log(s) registrado(s) correctamente"
}
```

`GET /api/logs/app?lines=150&level=ERROR`
- **Descripción**: Permite inspeccionar los últimos registros de telemetría de la aplicación Android.

`DELETE /api/logs/app`
- **Descripción**: Limpia de forma segura el archivo `app_telemetry.log`.

### 6. Biblioteca Multimedia y Eliminación de Archivos (Raspberry Pi 5 & 3)
La biblioteca busca automáticamente archivos de video y audio decodificando nombres con espacios y caracteres URL en múltiples ubicaciones estándar:
`downloads`, `music`, `~/Music`, `~/downloads`, `~/pi-music-cache` y `.`.

`GET /api/library`
- **Descripción**: Lista todos los archivos descargados y multimedia con metadatos completos y espacio de disco.

`DELETE /api/library/{filename}`
- **Descripción**: Elimina el archivo multimedia físico de forma segura con protección anti path-traversal y decodificación de espacios (`+` o `%20`).
- **Respuesta**:
```json
{
  "status": "ok",
  "message": "Archivo eliminado con éxito",
  "filename": "tu_cancion.mp3",
  "deleted": true
}
```

`POST /api/library/delete`
- **Body**:
```json
{
  "filename": "tu_cancion.mp3"
}
```
- **Descripción**: Endpoint alternativo vía POST para clientes que no soportan DELETE con payload o rutas complejas.
- **Respuesta**:
```json
{
  "status": "ok",
  "message": "Archivo eliminado con éxito",
  "filename": "tu_cancion.mp3",
  "deleted": true
}
```


