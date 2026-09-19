# 🚀 PiMusic Backend

<div align="center">

[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Python](https://img.shields.io/badge/Python-3.10%20%7C%203.11%20%7C%203.12-3776ab?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![yt-dlp](https://img.shields.io/badge/yt--dlp-Active-red?style=for-the-badge&logo=youtube&logoColor=white)](https://github.com/yt-dlp/yt-dlp)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ed?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Pytest](https://img.shields.io/badge/Pytest-100%25%20Passing-success?style=for-the-badge&logo=pytest&logoColor=white)](https://docs.pytest.org/)
[![License MIT](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)](LICENSE)

**Servidor API de alto rendimiento para streaming multimedia sin publicidad, descargas directas y gestión de biblioteca para Raspberry Pi, Linux, Docker y PCs locales.**

[Características](#-características) •
[Inicio Rápido](#-inicio-rápido) •
[Despliegue con Docker](#-despliegue-con-docker) •
[Despliegue en Raspberry Pi](#-despliegue-en-raspberry-pi-systemd) •
[Variables de Entorno](#-variables-de-entorno) •
[Referencia de la API](#-referencia-de-la-api) •
[Frontend](#-frontend)

</div>

---

## ✨ Características

* ⚡ **Streaming en Tiempo Real con Soporte HTTP Range 206**:
  - Proxy de streaming transparente que permite saltar a cualquier punto de la línea de tiempo (*seeking*) inmediatamente en navegadores web.
  - Resolución inteligente de formatos: 1080p, 720p, 480p, 360p y solo audio MP3.
  - Sin anuncios ni rastreadores intermediarios.

* 📥 **Motor de Descargas Directas & Segmentadas**:
  - Descarga a través del navegador del cliente o directamente al disco del servidor / Raspberry Pi.
  - Conversión optimizada con FFmpeg a MP3 de alta fidelidad (hasta 320 kbps) o remux a MP4 fMP4.

* 📚 **Gestor de Biblioteca Local**:
  - Endpoints para listar (`/api/library`), reproducir localmente (`/api/library/stream/{filename}`), descargar (`/api/library/download/{filename}`) y eliminar (`DELETE /api/library/{filename}`) archivos con protección contra *Path Traversal*.
  - Telemetría en tiempo real: espacio usado, espacio libre en disco, temperatura del SoC, uso de CPU y memoria RAM.

* 🧠 **Caché LRU en Memoria (Zero Double-Extraction)**:
  - Almacena metadatos y URLs de streaming con TTL para reducir peticiones a YouTube y evitar bloqueos por tasa de límite.

* 🔐 **Autenticación Opcional & Sesiones YouTube**:
  - Soporte para importar cookies de YouTube (`cookies.txt`) para contenido restringido por edad.
  - Soporte para flujo OAuth de vinculación de dispositivos para TVs (*Device Code Flow*).

---

## 🚀 Inicio Rápido

### Requisitos Previos
* **Python 3.10+** (recomendado Python 3.11 o 3.12).
* **FFmpeg**: Necesario para la extracción y remuxing de audio/video.
* **Node.js** (opcional pero recomendado): Permite a `yt-dlp` ejecutar desafíos JavaScript de YouTube sin límite de velocidad.

### Instalación Local

```bash
# 1. Clonar el repositorio
git clone https://github.com/JorgeTricarico/PiMusic-Backend.git
cd PiMusic-Backend

# 2. Crear y activar entorno virtual
python -m venv venv
# En Linux / macOS:
source venv/bin/activate
# En Windows:
venv\Scripts\activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar variables de entorno (opcional)
cp .env.example .env

# 5. Iniciar el servidor
python main.py
```

El servidor estará escuchando en `http://localhost:5000` con documentación Swagger interactiva disponible en `http://localhost:5000/docs`.

### Ejecutar Pruebas Automatizadas

```bash
pip install -r requirements-dev.txt
pytest tests/
```

---

## 🐳 Despliegue con Docker

PiMusic Backend cuenta con una imagen basada en `python:3.11-slim` con FFmpeg y Node.js preinstalados:

### Usando Docker Compose

```bash
# Iniciar contenedor
docker compose up -d --build
```

El servidor estará accesible en el puerto `5000`. Los archivos descargados persistirán en `./downloads`.

### Usando Docker directamente

```bash
# Construir imagen
docker build -t pimusic-backend .

# Ejecutar contenedor
docker run -d \
  --name pimusic-backend \
  -p 5000:5000 \
  -v $(pwd)/downloads:/app/downloads \
  --restart unless-stopped \
  pimusic-backend
```

---

## 🍓 Despliegue en Raspberry Pi (Systemd)

Para ejecutar PiMusic Backend de forma desatendida y automática al encender tu Raspberry Pi:

1. Clona el repositorio en `/home/jorge/pimusic-backend` (o tu usuario correspondiente):
   ```bash
   git clone https://github.com/JorgeTricarico/PiMusic-Backend.git /home/jorge/pimusic-backend
   cd /home/jorge/pimusic-backend
   python3 -m venv venv
   ./venv/bin/pip install -r requirements.txt
   ```

2. Instala el servicio Systemd:
   ```bash
   sudo cp systemd/pimusic.service /etc/systemd/system/pimusic-backend.service
   sudo systemctl daemon-reload
   sudo systemctl enable --now pimusic-backend
   ```

3. Comprobar el estado y ver registros:
   ```bash
   sudo systemctl status pimusic-backend
   journalctl -u pimusic-backend -f
   ```

---

## ⚙️ Variables de Entorno

Configurables en tu archivo `.env` o en las variables del contenedor:

| Variable | Descripción | Valor por defecto |
|---|---|---|
| `PIMUSIC_HOST` | Dirección IP de enlace del servidor | `0.0.0.0` |
| `PIMUSIC_PORT` | Puerto de escucha | `5000` |
| `PIMUSIC_DOWNLOADS_DIR` | Directorio de descargas y biblioteca multimedia | `./downloads` *(o `~/pi-music-cache`)* |
| `PIMUSIC_CORS_ORIGINS` | Orígenes permitidos (separados por coma, o `*` para LAN abierta) | `*` |
| `PIMUSIC_COOKIES_FILE` | Ruta a archivo de cookies de YouTube para eludir restricciones | `./downloads/system/cookies.txt` |
| `GOOGLE_DEVICE_CLIENT_ID` | Client ID de Google OAuth para Device Code Flow (opcional) | `""` |
| `GOOGLE_DEVICE_CLIENT_SECRET` | Client Secret de Google OAuth (opcional) | `""` |

---

## 📖 Referencia de la API

| Método | Endpoint | Descripción |
|---|---|---|
| `GET` | `/api/search?q={termino}` | Búsqueda rápida de videos en YouTube con miniaturas y metadatos. |
| `GET` | `/api/info?url={url_o_id}` | Obtiene opciones de formato disponibles (1080p, 720p, 480p, 360p, MP3). |
| `GET` | `/api/stream_media/{id}?type={video\|audio}&quality={calidad}` | Stream directo en tiempo real con soporte HTTP Range 206 (seeking instantáneo). |
| `GET` | `/api/download_stream?url={url}&quality={calidad}&type={tipo}` | Descarga directa al navegador como archivo adjunto (`Content-Disposition: attachment`). |
| `POST` | `/api/download_to_server` | Descarga asíncrona a la biblioteca del servidor en segundo plano. |
| `GET` | `/api/library` | Lista archivos multimedia guardados en el almacenamiento con telemetría de disco. |
| `GET` | `/api/library/stream/{filename}` | Streaming con soporte HTTP Range de un archivo ya almacenado en el servidor. |
| `DELETE` | `/api/library/{filename}` | Elimina un archivo de la biblioteca con verificación anti path-traversal. |
| `GET` | `/api/telemetry` | Métricas del sistema en tiempo real: CPU, RAM, temperatura y disco. |

---

## 💻 Frontend

Este servidor está diseñado para emparejarse con **[PiMusic-Frontend](https://github.com/JorgeTricarico/PiMusicFront)**, la interfaz web moderna en React 19, TypeScript, Tailwind CSS, PWA, soporte para gestos táctiles y atajos de teclado completos estilo YouTube.

---

## 📄 Licencia

Distribuido bajo la Licencia MIT. Consulta `LICENSE` para más detalles.
