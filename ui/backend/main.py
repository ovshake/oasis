"""FastAPI entrypoint for the OASIS Crypto Sim backend API."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="OASIS Crypto Sim API", version="0.1.0")

# CORS for frontend dev (localhost:3000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import and register routers
from ui.backend.routes import scenarios, runs, telemetry_ws, eval_, personas  # noqa: E402

app.include_router(scenarios.router)
app.include_router(runs.router)
app.include_router(telemetry_ws.router)
app.include_router(eval_.router)
app.include_router(personas.router)


@app.get("/health")
async def health():
    return {"status": "ok"}
