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
  "format": "video|audio"
}
```
- **Descripción**: Inicia la reproducción local en la Raspberry Pi usando mpv. Si el formato es audio añade `--no-video`, y si es video `--fs`. Además realiza caching automático en segundo plano para reproducciones instantáneas futuras.
- **Respuesta**:
```json
{
  "status": "ok",
  "message": "Reproduciendo https://...",
  "track": "Nombre de la pista"
}
```

`POST /api/playback/action` y `POST /api/control`
- **Body**:
```json
{
  "action": "pause|resume|play|toggle|stop|next|prev"
}
```
- **Descripción**: Envía comandos al proceso de reproducción (`SIGSTOP` para pause, `SIGCONT` para resume, terminación limpia para stop).
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
- **Descripción**: Utiliza `yt_dlp.YoutubeDL` nativo con `player_client: ["android_vr", "android", "web"]` para extraer streams multiplexados MP4 progresivos (video H.264 + audio AAC en un único contenedor) 100% compatibles con `MediaPlayer` y `ExoPlayer`, evitando los errores `MEDIA_ERROR_MALFORMED (-1007)` y `MEDIA_ERROR_UNSUPPORTED (-1010)`. Si YouTube no entrega stream progresivo directo, realiza fallback transparente al streaming fMP4 remuxeado en tiempo real por el servidor PiMusic.
- **Respuesta Esperada**:
```json
{
  "status": "ok",
  "stream_url": "https://rr3---sn-...",
  "direct_url": "https://rr3---sn-...",
  "url": "https://rr3---sn-...",
  "title": "Título del video",
  "format": "mp4",
  "type": "video"
}
```

`GET /api/stream_media/{videoId}?type={video|audio}&quality={quality}` (Fallback Stream con HTTP Range 206)
- **Descripción**: Streaming con proxy directo por chunks y soporte de HTTP Range 206 para seeking instantáneo y remuxing en vivo a fMP4 sin recodificar.

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
