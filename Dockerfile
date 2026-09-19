# ==============================================================================
# Dockerfile para PiMusic Backend (FastAPI + yt-dlp + FFmpeg)
# Soporta arquitecturas: amd64, arm64 (Raspberry Pi 4 / 5)
# ==============================================================================

FROM python:3.11-slim

# Evitar prompts interactivos y buffering de salida
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIMUSIC_HOST=0.0.0.0 \
    PIMUSIC_PORT=5000 \
    PIMUSIC_DOWNLOADS_DIR=/app/downloads

WORKDIR /app

# Instalar FFmpeg, curl y Node.js (necesario para la resolución de firmas n-sig de YouTube en yt-dlp)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    nodejs \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY . .

# Crear directorio de almacenamiento de descargas y permisos adecuados
RUN mkdir -p /app/downloads && \
    useradd -u 1000 -m -s /bin/bash pimusic && \
    chown -R pimusic:pimusic /app

USER pimusic

EXPOSE 5000

# Verificación de salud del contenedor
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:5000/api/telemetry || exit 1

CMD ["python", "main.py"]
