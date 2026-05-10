from __future__ import annotations

from contextlib import asynccontextmanager
import logging
import os
import socket
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi import Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api.routes import api_router, router
from core.database import ensure_schema
from services.orchestration_service import orchestration_service


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

FRONTEND_DIR = PROJECT_ROOT / "frontend"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup: ensuring database schema")
    ensure_schema()
    logger.info("Application startup: initializing realtime orchestration")
    await orchestration_service.initialize()
    try:
        yield
    finally:
        logger.info("Application shutdown: stopping realtime orchestration")
        await orchestration_service.shutdown()


app = FastAPI(title="EV Charging Coordination Console", lifespan=lifespan)
app.include_router(router)
app.include_router(api_router)
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


@app.get("/", include_in_schema=False)
def frontend_index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


def find_available_port(host: str, preferred_port: int) -> int:
    for port in range(preferred_port, preferred_port + 100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
            except OSError:
                continue
            return port

    raise RuntimeError(f"No available port found from {preferred_port} to {preferred_port + 99}")


def main() -> None:
    host = os.getenv("APP_HOST", DEFAULT_HOST)
    preferred_port = int(os.getenv("APP_PORT", str(DEFAULT_PORT)))
    port = find_available_port(host, preferred_port)

    if port != preferred_port:
        print(f"Port {preferred_port} is already in use. Starting on http://{host}:{port}")

    uvicorn.run("app.main:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
