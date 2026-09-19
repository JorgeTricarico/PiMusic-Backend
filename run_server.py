#!/usr/bin/env python3
"""
Lanzador del servidor PiMusic en local o Raspberry Pi.
Ejecución: python run_server.py
"""
import os
import sys

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"==================================================")
    print(f"🎵 PiMusic YouTube API & Downloader Server")
    print(f"📡 Escuchando en: http://localhost:{port}")
    print(f"🌐 Accesible en la red local: http://<IP-DE-TU-PC-O-PI>:{port}")
    print(f"==================================================")
    uvicorn.run("main:app", host=host, port=port, reload=True)
