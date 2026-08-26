"""
ETL Observability App API
Pipeline attach stored in MySQL; webhook Sync loads active or named pipeline. Dashboard UI APIs under /api/v1/*.
"""

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.db import query
from app.routers.dashboard_v1 import router as dashboard_v1_router

app = FastAPI(
    title="ETL Observability App API",
    description="Pipeline attach stored in MySQL; webhook Sync loads active or named pipeline. Dashboard UI APIs under /api/v1/*.",
    version="0.5.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", summary="Root")
def root():
    return {
        "message": "ETL Observability App API is running",
        "docs": "/docs",
        "version": "0.5.0"
    }

@app.get("/health", summary="Health")
def health():
    res = query("SELECT 1 as alive")
    is_alive = len(res) > 0 and res[0]["alive"] == 1
    return {
        "status": "healthy" if is_alive else "degraded",
        "database": "connected" if is_alive else "disconnected",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

# Mount /api/v1 router
app.include_router(dashboard_v1_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
