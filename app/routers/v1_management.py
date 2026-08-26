import json
import uuid
from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field
from fastapi import APIRouter, Query, Path, Body, HTTPException
from app.core.db import query
from app.core.envelope import clean_val

router = APIRouter()

class CreatePipelineRequest(BaseModel):
    pipeline_id: Optional[str] = Field(None, title="Pipeline Id", description="Omit to create a new UUID (stored in DB)")
    pipeline_name: Optional[str] = Field("stock_etl", title="Pipeline Name", description="Template name: stock_etl | ecommerce_etl (or custom label)")
    make_active: bool = Field(True, title="Make Active", description="If true, this pipeline becomes the Sync default")

class SyncRequest(BaseModel):
    pipeline_id: Optional[str] = Field(None, title="Pipeline Id")
    pipeline_name: Optional[str] = Field(None, title="Pipeline Name")
    dbt_run_id: Optional[str] = Field(None, title="Dbt Run Id")

@router.get("/v1/pipelines/templates", summary="Pipelines Templates")
def get_pipeline_templates():
    return {
        "ok": True,
        "templates": [
            "ecommerce_etl",
            "hr_etl",
            "stock_etl"
        ]
    }

@router.get("/v1/pipelines", summary="Pipelines List")
def get_v1_pipelines():
    rows = query("SELECT pipeline_id, pipeline_name, source_tool, source_schema, etl_tool, target_tool, target_schema, is_active, updated_at FROM obs_pipelines ORDER BY updated_at DESC")
    return {
        "ok": True,
        "pipelines": clean_val(rows)
    }

@router.post("/v1/pipelines", summary="Create Pipeline")
def create_pipeline(body: Optional[CreatePipelineRequest] = None):
    req = body or CreatePipelineRequest()
    p_id = req.pipeline_id or str(uuid.uuid4())
    p_name = req.pipeline_name or "stock_etl"
    
    if req.make_active:
        query("UPDATE obs_pipelines SET is_active = 0")
        
    query("""
        INSERT INTO obs_pipelines (pipeline_id, pipeline_name, tenant_id, description, source_tool, source_schema, etl_tool, target_tool, target_schema, is_active, is_operational, created_at, updated_at)
        VALUES (%s, %s, 'demo', %s, 'snowflake', 'RAW', 'dbt', 'snowflake', 'STAGING_STAGING', %s, 0, NOW(), NOW())
        ON DUPLICATE KEY UPDATE pipeline_name = VALUES(pipeline_name), is_active = VALUES(is_active), updated_at = NOW()
    """, (p_id, p_name, f"Pipeline {p_name}", 1 if req.make_active else 0))

    created = query("SELECT * FROM obs_pipelines WHERE pipeline_id = %s", (p_id,))
    return {"ok": True, "pipeline": clean_val(created[0]) if created else {"pipeline_id": p_id, "pipeline_name": p_name}}

@router.get("/v1/pipelines/current", summary="Get Current Pipeline")
def get_current_pipeline():
    rows = query("SELECT * FROM obs_pipelines WHERE is_active = 1 LIMIT 1")
    if not rows:
        rows = query("SELECT * FROM obs_pipelines ORDER BY updated_at DESC LIMIT 1")
    if not rows:
        raise HTTPException(status_code=404, detail="No pipeline found")
    
    p = rows[0]
    config = {}
    if p.get("config_json"):
        try:
            config = json.loads(p["config_json"])
        except Exception:
            pass

    return {
        "pipeline_id": p["pipeline_id"],
        "pipeline_name": p["pipeline_name"],
        "tenant_id": p.get("tenant_id", "demo"),
        "description": p.get("description", ""),
        "source": config.get("source", {
            "tool": p.get("source_tool", "snowflake"),
            "connector_instance_id": "sf-source-raw",
            "schema": p.get("source_schema", "RAW")
        }),
        "etl": config.get("etl", {
            "tool": p.get("etl_tool", "dbt"),
            "connector_instance_id": "dbt-job"
        }),
        "target": config.get("target", {
            "tool": p.get("target_tool", "snowflake"),
            "connector_instance_id": "sf-target-staging",
            "schema": p.get("target_schema", "STAGING_STAGING")
        }),
        "created_at": p.get("created_at"),
        "updated_at": p.get("updated_at"),
        "is_active": bool(p.get("is_active")),
        "is_operational": bool(p.get("is_operational"))
    }

@router.post("/v1/sync", summary="Sync Manual")
def sync_manual(body: Optional[SyncRequest] = None):
    req = body or SyncRequest()
    target_pipe = req.pipeline_name or "ecommerce_etl"
    return {
        "ok": True,
        "message": f"Sync completed for pipeline '{target_pipe}'",
        "pipeline_name": target_pipe,
        "synced_at": datetime.utcnow().isoformat() + "Z"
    }

@router.get("/v1/dashboard/overview", summary="Dashboard Overview")
def get_v1_dashboard_overview(range: str = Query("24h", description="Time range")):
    tot_kpis = query("SELECT total_runs, success_runs, failed_runs, success_rate_pct, (SELECT ROUND(AVG(duration),1) FROM obs_pipeline_runs) as avg_duration FROM vw_kpi_totals LIMIT 1")[0]
    
    daily_rows = query("""
        SELECT DATE(start_time) as dt, COUNT(*) as tot, SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as suc, SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as fld
        FROM obs_pipeline_runs
        GROUP BY dt
        ORDER BY dt
    """)

    daily = []
    for r in daily_rows:
        tot = int(r["tot"] or 0)
        suc = int(r["suc"] or 0)
        rate = round(suc * 100.0 / max(tot, 1), 1)
        daily.append({
            "date": str(r["dt"]),
            "total_runs": tot,
            "success_runs": suc,
            "failed_runs": int(r["fld"] or 0),
            "success_rate_pct": rate
        })

    recent_runs = query("""
        SELECT id, pipeline_name, status, start_time, duration
        FROM obs_pipeline_runs
        ORDER BY start_time DESC
        LIMIT 5
    """)

    kpi_defs = [
        {"id": "pipelines", "title": "Pipelines", "meaning": "How many ETL pipelines are registered in metadata.", "formula": "COUNT(*) FROM obs_pipelines", "tables": "obs_pipelines"},
        {"id": "success_rate", "title": "Success Rate", "meaning": "Share of pipeline runs that finished successfully in the selected range.", "formula": "100 * success_runs / total_runs", "tables": "obs_pipeline_runs"},
        {"id": "failed_runs", "title": "Failed Runs", "meaning": "Count of pipeline runs with failed status.", "formula": "COUNT(*) WHERE status='failed'", "tables": "obs_pipeline_runs"},
        {"id": "avg_duration", "title": "Avg Duration", "meaning": "Mean execution duration in seconds across runs.", "formula": "AVG(duration)", "tables": "obs_pipeline_runs"}
    ]

    return {
        "ok": True,
        "range": range,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "kpi_defs": kpi_defs,
        "kpis": {
            "total_runs": int(tot_kpis["total_runs"]),
            "success_runs": int(tot_kpis["success_runs"]),
            "failed_runs": int(tot_kpis["failed_runs"]),
            "success_rate_pct": float(tot_kpis["success_rate_pct"]),
            "avg_duration_sec": float(tot_kpis["avg_duration"] or 12.0)
        },
        "charts": {
            "daily": daily
        },
        "recent_runs": clean_val(recent_runs)
    }

@router.post("/grafana/dashboard", summary="Generate Grafana Dashboard")
def generate_grafana_dashboard():
    return {
        "ok": True,
        "status": "success",
        "dashboard_uid": "etl-obs-dash-01",
        "url": "https://grafana.example.com/d/etl-obs-dash-01/etl-observability",
        "message": "Grafana dashboard generated successfully"
    }

@router.post("/webhooks/dbt", summary="Dbt Webhook")
def dbt_webhook(
    pipeline_name: Optional[str] = Query(None),
    payload: Dict[str, Any] = Body(..., title="Payload")
):
    pipe = pipeline_name or payload.get("data", {}).get("jobName", "dbt_job")
    return {
        "ok": True,
        "status": "received",
        "pipeline_name": pipe,
        "received_at": datetime.utcnow().isoformat() + "Z",
        "eventType": payload.get("eventType", "job.run.completed")
    }

@router.post("/webhooks/dbt/{pipeline_name}", summary="Dbt Webhook For Pipeline")
def dbt_webhook_for_pipeline(
    pipeline_name: str = Path(...),
    payload: Dict[str, Any] = Body(..., title="Payload")
):
    return {
        "ok": True,
        "status": "received",
        "pipeline_name": pipeline_name,
        "received_at": datetime.utcnow().isoformat() + "Z",
        "eventType": payload.get("eventType", "job.run.completed")
    }
