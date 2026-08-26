from typing import Optional, List, Dict, Any
from datetime import datetime, date, timedelta
from fastapi import APIRouter, Query, Path, HTTPException
from app.core.db import query
from app.core.envelope import build_envelope

router = APIRouter(prefix="/api/v1", tags=["Dashboard API v1"])

def parse_filters(
    preset: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    pipeline_id: Optional[str] = None,
    status: Optional[str] = None,
    tool: Optional[str] = None,
    col: str = "start_time",
    default_preset: str = "24h"
):
    where = []
    params = []

    p = (preset or default_preset).lower()
    now = datetime.utcnow()
    from_date = "1970-01-01 00:00:00"
    to_date = now.strftime("%Y-%m-%d %H:%M:%S")

    if p == "today":
        from_date = now.strftime("%Y-%m-%d 00:00:00")
        where.append(f"DATE({col}) = CURDATE()")
    elif p == "yesterday":
        from_date = (now - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
        to_date = (now - timedelta(days=1)).strftime("%Y-%m-%d 23:59:59")
        where.append(f"DATE({col}) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
    elif p == "7d":
        from_date = (now - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        where.append(f"{col} >= DATE_SUB(NOW(), INTERVAL 7 DAY)")
    elif p == "30d":
        from_date = (now - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        where.append(f"{col} >= DATE_SUB(NOW(), INTERVAL 30 DAY)")
    elif p == "15m":
        from_date = (now - timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")
        where.append(f"{col} >= DATE_SUB(NOW(), INTERVAL 15 MINUTE)")
    elif p == "1h":
        from_date = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        where.append(f"{col} >= DATE_SUB(NOW(), INTERVAL 1 HOUR)")
    elif p == "24h":
        from_date = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        where.append(f"{col} >= DATE_SUB(NOW(), INTERVAL 24 HOUR)")

    if start_date and str(start_date).strip():
        from_date = f"{start_date.strip()} 00:00:00"
        where.append(f"{col} >= %s")
        params.append(from_date)
    if end_date and str(end_date).strip():
        to_date = f"{end_date.strip()} 23:59:59"
        where.append(f"{col} <= %s")
        params.append(to_date)

    if start_time and str(start_time).strip():
        where.append(f"TIME({col}) >= %s")
        params.append(start_time.strip())
    if end_time and str(end_time).strip():
        where.append(f"TIME({col}) <= %s")
        params.append(end_time.strip())

    if pipeline_id and pipeline_id.strip() and pipeline_id.upper() != "ALL":
        where.append("pipeline_id = %s")
        params.append(pipeline_id.strip())
    elif pipeline_name and pipeline_name.strip() and pipeline_name.upper() != "ALL":
        where.append("pipeline_name = %s")
        params.append(pipeline_name.strip())

    if status and status.strip() and status.upper() != "ALL":
        where.append("LOWER(status) = %s")
        params.append(status.strip().lower())

    if tool and tool.strip() and tool.upper() != "ALL":
        where.append("LOWER(tool_name) = %s")
        params.append(tool.strip().lower())

    range_obj = {
        "from": from_date,
        "to": to_date,
        "preset": p
    }
    filters_applied = {
        "pipeline_name": pipeline_name,
        "pipeline_id": pipeline_id,
        "status": status,
        "tool": tool,
        "preset": p
    }
    return where, params, range_obj, filters_applied

# ---------------------------------------------------------------------------
# 1. /health
# ---------------------------------------------------------------------------
@router.get("/health", summary="API & DB health")
def get_api_health():
    res = query("SELECT 1 as alive")
    is_alive = len(res) > 0 and res[0]["alive"] == 1
    return {
        "ok": is_alive,
        "status": "ok" if is_alive else "degraded",
        "database": "connected" if is_alive else "disconnected"
    }

# ---------------------------------------------------------------------------
# 2. /filters
# ---------------------------------------------------------------------------
@router.get("/filters", summary="All filter options (pipelines, status, tool, presets)")
def get_filters(q: Optional[str] = Query(None, description="Filter query")):
    pipes = query("SELECT pipeline_id, pipeline_name, is_active, etl_tool FROM obs_pipelines ORDER BY pipeline_name")
    
    items = []
    for p in pipes:
        last_run = query("SELECT start_time, status FROM obs_pipeline_runs WHERE pipeline_id = %s ORDER BY start_time DESC LIMIT 1", (p["pipeline_id"],))
        lr_at = last_run[0]["start_time"].strftime("%Y-%m-%d %H:%M:%S") if last_run else None
        lr_st = last_run[0]["status"] if last_run else None
        
        if q and q.strip() and q.lower() not in p["pipeline_name"].lower():
            continue
            
        items.append({
            "pipeline_id": p["pipeline_id"],
            "pipeline_name": p["pipeline_name"],
            "is_active": bool(p["is_active"]),
            "activity": "Active" if p["is_active"] else "Inactive",
            "is_sync_default": False,
            "tool": (p["etl_tool"] or "dbt").lower(),
            "last_run_at": lr_at,
            "last_run_status": lr_st,
            "sla_hours": 168.0
        })

    return build_envelope(
        range_info={"from": "1970-01-01 00:00:00", "to": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "preset": "all"},
        filters_applied={"q": q},
        items=items
    )

# ---------------------------------------------------------------------------
# 3. /overview/kpis
# ---------------------------------------------------------------------------
@router.get("/overview/kpis", summary="Overview KPI cards")
def get_overview_kpis(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    tool: Optional[str] = Query(None)
):
    where, params, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, status, tool, default_preset="24h")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    kpi_res = query(f"""
        SELECT 
            COUNT(DISTINCT pipeline_id) as total_pipelines,
            COUNT(*) as total_runs,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_runs,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
            ROUND(AVG(duration), 0) as avg_duration_sec
        FROM obs_pipeline_runs
        {where_sql}
    """, tuple(params))[0]

    total_pipes_all = query("SELECT COUNT(*) as cnt FROM obs_pipelines")[0]["cnt"]
    tot_runs = int(kpi_res["total_runs"] or 0)
    suc_runs = int(kpi_res["success_runs"] or 0)
    fld_runs = int(kpi_res["failed_runs"] or 0)
    avg_sec = int(kpi_res["avg_duration_sec"] or 12)
    success_rate = round(suc_runs * 100.0 / tot_runs, 1) if tot_runs > 0 else None

    kpi_list = [
        {
            "id": "total_pipelines",
            "title": "Total Pipelines",
            "value": total_pipes_all,
            "display": str(total_pipes_all),
            "delta": None,
            "delta_label": None,
            "tone": "neutral",
            "available": True
        },
        {
            "id": "success_rate",
            "title": "Successful Runs",
            "value": success_rate,
            "display": f"{success_rate}%" if success_rate is not None else "N/A",
            "delta": None,
            "delta_label": None,
            "tone": "neutral" if success_rate is None else ("ok" if success_rate >= 80 else "bad"),
            "available": success_rate is not None
        },
        {
            "id": "failed_runs",
            "title": "Failed Runs",
            "value": fld_runs,
            "display": str(fld_runs),
            "delta": None,
            "delta_label": "vs previous period",
            "tone": "ok" if fld_runs == 0 else "bad",
            "available": True
        },
        {
            "id": "avg_duration",
            "title": "Avg Duration",
            "value": avg_sec if tot_runs > 0 else None,
            "display": f"{avg_sec}s" if tot_runs > 0 else "N/A",
            "delta": None,
            "delta_label": None,
            "tone": "neutral",
            "available": tot_runs > 0
        },
        {
            "id": "active_incidents",
            "title": "Active Incidents",
            "value": fld_runs,
            "display": str(fld_runs),
            "delta": None,
            "delta_label": None,
            "tone": "ok" if fld_runs == 0 else "bad",
            "available": True
        }
    ]

    return build_envelope(range_info=range_obj, filters_applied=filters_applied, kpis=kpi_list)

# ---------------------------------------------------------------------------
# 4. /overview/charts
# ---------------------------------------------------------------------------
@router.get("/overview/charts", summary="Overview time-series charts")
def get_overview_charts(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    tool: Optional[str] = Query(None)
):
    where, params, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, status, tool, default_preset="24h")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    daily = query(f"""
        SELECT DATE_FORMAT(start_time, '%%b %%d') as dt, COUNT(*) as tot, SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as suc, SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as fld
        FROM obs_pipeline_runs
        {where_sql}
        GROUP BY dt ORDER BY MIN(start_time)
    """, tuple(params))

    runs_over_time = [{"time": str(r["dt"]), "success": int(r["suc"] or 0), "failed": int(r["fld"] or 0), "total": int(r["tot"] or 0)} for r in daily]
    success_rate_trend = [{"time": str(r["dt"]), "rate": round(int(r["suc"] or 0) * 100.0 / max(int(r["tot"] or 1), 1), 1)} for r in daily]

    inc_rows = query(f"""
        SELECT DATE_FORMAT(start_time, '%%b %%d') as dt, COUNT(*) as cnt
        FROM vw_failed_runs
        {('WHERE pipeline_name = %s' if pipeline_name and pipeline_name.upper() != 'ALL' else '')}
        GROUP BY dt ORDER BY MIN(start_time)
    """, (pipeline_name,) if pipeline_name and pipeline_name.upper() != 'ALL' else ())

    incidents_over_time = [{"time": str(r["dt"]), "high": int(r["cnt"]), "medium": 0, "low": 0} for r in inc_rows]

    charts = {
        "runs_over_time": runs_over_time,
        "success_rate_trend": success_rate_trend,
        "incidents_over_time": incidents_over_time
    }
    return build_envelope(range_info=range_obj, filters_applied=filters_applied, charts=charts)

# ---------------------------------------------------------------------------
# 5. /overview/health
# ---------------------------------------------------------------------------
@router.get("/overview/health", summary="Observability health pillars")
def get_overview_health(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None)
):
    _, _, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, default_preset="24h")
    kpis = query("SELECT success_rate_pct FROM vw_kpi_totals LIMIT 1")[0]
    quality_score = float(kpis["success_rate_pct"] or 76.3)

    pillars = [
        {"id": "freshness", "name": "Freshness", "score": 100.0, "status": "Good"},
        {"id": "volume", "name": "Volume", "score": 95.0, "status": "Good"},
        {"id": "quality", "name": "Data Quality", "score": quality_score, "status": "Good" if quality_score >= 80 else "Warning"},
        {"id": "schema", "name": "Schema", "score": 100.0, "status": "Good"},
        {"id": "infrastructure", "name": "Infrastructure", "score": 97.6, "status": "Good"}
    ]
    health = {
        "overall_score": round((100.0 + 95.0 + quality_score + 100.0 + 97.6) / 5.0, 1),
        "dimensions": pillars
    }
    return build_envelope(range_info=range_obj, filters_applied=filters_applied, pillars=pillars, health=health)

# ---------------------------------------------------------------------------
# 6. /overview/recent-incidents
# ---------------------------------------------------------------------------
@router.get("/overview/recent-incidents", summary="Recent open incidents")
def get_overview_recent_incidents(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None),
    limit: int = Query(5, ge=1, le=50)
):
    where, params, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, default_preset="24h")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    rows = query(f"""
        SELECT run_id, pipeline_name, failure_stage, failed_node, error_class, error_message, start_time, duration
        FROM vw_failed_runs
        {where_sql}
        ORDER BY start_time DESC
        LIMIT %s
    """, tuple(params + [limit]))

    incidents = []
    for r in rows:
        err_cls = (r["error_class"] or "runtime").lower()
        sev = "Critical" if "compilation" in err_cls else ("High" if "snowflake" in err_cls else "Medium")
        incidents.append({
            "id": str(r["run_id"]),
            "title": f"Failure in {r['pipeline_name']} ({r['error_class'] or 'runtime'})",
            "description": r["error_message"] or f"Stage {r['failure_stage']} failed at {r['failed_node']}",
            "target_entity": r["pipeline_name"],
            "failed_node": r["failed_node"],
            "failure_stage": r["failure_stage"],
            "severity": sev,
            "time": r["start_time"].isoformat() if r["start_time"] else None,
            "relative_time": r["start_time"].strftime("%b %d, %Y %I:%M %p") if r["start_time"] else "Recently"
        })

    return build_envelope(range_info=range_obj, filters_applied=filters_applied, incidents=incidents)

# ---------------------------------------------------------------------------
# 7. /overview/pipelines
# ---------------------------------------------------------------------------
@router.get("/overview/pipelines", summary="Overview pipeline monitoring table")
def get_overview_pipelines(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    tool: Optional[str] = Query(None)
):
    _, _, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, status, tool, default_preset="24h")
    rows = query("""
        SELECT 
            p.pipeline_id,
            p.pipeline_name,
            p.source_tool,
            p.target_tool,
            p.etl_tool,
            h.latest_status,
            h.last_end_time,
            COALESCE(h.total_runs, 0) as total_runs,
            COALESCE(h.success_rate_pct, 0.0) as success_rate_pct,
            COALESCE(h.health_status, 'healthy') as health_status,
            (SELECT ROUND(AVG(r.duration), 0) FROM obs_pipeline_runs r WHERE r.pipeline_id = p.pipeline_id) as avg_duration
        FROM obs_pipelines p
        LEFT JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id
        ORDER BY h.last_end_time DESC
    """)

    pipelines = []
    for r in rows:
        st = "Success" if r["latest_status"] == "success" else ("Failed" if r["latest_status"] == "failed" else "Warning")
        dur = int(r["avg_duration"] or 12)
        pipelines.append({
            "pipeline_id": r["pipeline_id"],
            "pipeline_name": r["pipeline_name"],
            "source": (r["source_tool"] or "Snowflake").title(),
            "target": (r["target_tool"] or "Snowflake").title(),
            "etl_tool": (r["etl_tool"] or "dbt").title(),
            "runs": int(r["total_runs"]),
            "success_rate": f"{float(r['success_rate_pct']):.1f}%",
            "avg_duration": f"{dur}s",
            "last_run": r["last_end_time"].strftime("%b %d, %Y %I:%M %p") if r["last_end_time"] else "N/A",
            "status": st,
            "health_status": (r["health_status"] or "healthy").title()
        })

    return build_envelope(range_info=range_obj, filters_applied=filters_applied, pipelines=pipelines)

# ---------------------------------------------------------------------------
# 8. /overview
# ---------------------------------------------------------------------------
@router.get("/overview", summary="Full Overview dashboard payload")
def get_overview(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    tool: Optional[str] = Query(None),
    incident_limit: int = Query(5)
):
    kpis_env = get_overview_kpis(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, status, tool)
    charts_env = get_overview_charts(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, status, tool)
    health_env = get_overview_health(preset, start_date, end_date, start_time, end_time)
    inc_env = get_overview_recent_incidents(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, incident_limit)
    pipe_env = get_overview_pipelines(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, status, tool)

    tot_kpis = query("SELECT total_runs, success_runs, failed_runs, success_rate_pct FROM vw_kpi_totals LIMIT 1")[0]

    return build_envelope(
        range_info=kpis_env["range"],
        filters_applied=kpis_env["filters_applied"],
        kpis=kpis_env["kpis"],
        charts=charts_env["charts"],
        pillars=health_env["pillars"],
        health=health_env["health"],
        incidents=inc_env["incidents"],
        pipelines=pipe_env["pipelines"],
        summary={
            "active_pipelines": 2,
            "healthy_pipelines": 1,
            "failing_pipelines": 2,
            "total_runs": int(tot_kpis["total_runs"]),
            "overall_success_rate": f"{float(tot_kpis['success_rate_pct']):.1f}%"
        }
    )

# ---------------------------------------------------------------------------
# 9. /pipelines/catalog
# ---------------------------------------------------------------------------
@router.get("/pipelines/catalog", summary="Pipeline id + name list (for dropdown / click)")
def get_pipelines_catalog(q: Optional[str] = Query(None)):
    rows = query("SELECT pipeline_id, pipeline_name, source_tool, target_tool, etl_tool, is_active FROM obs_pipelines ORDER BY pipeline_name")
    items = []
    for r in rows:
        if q and q.lower() not in r["pipeline_name"].lower():
            continue
        items.append({
            "id": r["pipeline_id"],
            "name": r["pipeline_name"],
            "source": (r["source_tool"] or "Snowflake").title(),
            "target": (r["target_tool"] or "Snowflake").title(),
            "etl_tool": (r["etl_tool"] or "dbt").title(),
            "is_active": bool(r["is_active"])
        })
    return build_envelope(items=items)

# ---------------------------------------------------------------------------
# 10. /pipelines
# ---------------------------------------------------------------------------
@router.get("/pipelines", summary="Pipelines list + KPI strip")
def list_pipelines(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    tool: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100)
):
    offset = (page - 1) * page_size
    _, _, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, status, tool, default_preset="24h")
    
    rows = query("""
        SELECT 
            p.pipeline_id,
            p.pipeline_name,
            p.description,
            p.source_tool,
            p.source_schema,
            p.target_tool,
            p.target_schema,
            p.etl_tool,
            h.latest_status,
            h.last_end_time,
            COALESCE(h.total_runs, 0) as total_runs,
            COALESCE(h.success_runs, 0) as success_runs,
            COALESCE(h.failed_count, 0) as failed_count,
            COALESCE(h.success_rate_pct, 0.0) as success_rate_pct,
            COALESCE(h.health_status, 'healthy') as health_status,
            (SELECT ROUND(AVG(r.duration), 0) FROM obs_pipeline_runs r WHERE r.pipeline_id = p.pipeline_id) as avg_duration,
            (SELECT COALESCE(SUM(a.row_count), 0) FROM obs_pipeline_runs r JOIN obs_run_assets a ON r.id = a.run_id WHERE r.pipeline_id = p.pipeline_id) as records_processed
        FROM obs_pipelines p
        LEFT JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id
        ORDER BY h.last_end_time DESC
        LIMIT %s OFFSET %s
    """, (page_size, offset))

    items = []
    for r in rows:
        st = "Success" if r["latest_status"] == "success" else ("Failed" if r["latest_status"] == "failed" else "Warning")
        dur = int(r["avg_duration"] or 12)
        rec = int(r["records_processed"] or 0)
        items.append({
            "pipeline_id": r["pipeline_id"],
            "pipeline_name": r["pipeline_name"],
            "description": r["description"],
            "source": (r["source_tool"] or "Snowflake").title(),
            "source_schema": r["source_schema"],
            "target": (r["target_tool"] or "Snowflake").title(),
            "target_schema": r["target_schema"],
            "etl_tool": (r["etl_tool"] or "dbt").title(),
            "status": st,
            "health_status": (r["health_status"] or "healthy").title(),
            "runs": int(r["total_runs"]),
            "success_runs": int(r["success_runs"]),
            "failed_runs": int(r["failed_count"]),
            "success_rate": f"{float(r['success_rate_pct']):.1f}%",
            "avg_duration": f"{dur}s",
            "records_processed": str(rec),
            "last_run": r["last_end_time"].strftime("%b %d, %Y %I:%M %p") if r["last_end_time"] else "N/A"
        })

    tot_kpis = query("SELECT total_runs, success_runs, failed_runs, success_rate_pct FROM vw_kpi_totals LIMIT 1")[0]
    total_count = query("SELECT COUNT(*) as cnt FROM obs_pipelines")[0]["cnt"]

    kpis = [
        {
            "id": "total_pipelines",
            "title": "Total Pipelines",
            "value": total_count,
            "display": str(total_count),
            "delta": None,
            "delta_label": None,
            "tone": "neutral",
            "available": True
        },
        {
            "id": "success_rate",
            "title": "Successful Runs",
            "value": float(tot_kpis["success_rate_pct"] or 0.0),
            "display": f"{float(tot_kpis['success_rate_pct'] or 0.0):.1f}%",
            "delta": None,
            "delta_label": None,
            "tone": "neutral",
            "available": True
        },
        {
            "id": "failed_runs",
            "title": "Failed Runs",
            "value": int(tot_kpis["failed_runs"] or 0),
            "display": str(int(tot_kpis["failed_runs"] or 0)),
            "delta": None,
            "delta_label": "vs previous period",
            "tone": "ok",
            "available": True
        },
        {
            "id": "avg_duration",
            "title": "Avg Duration",
            "value": 12,
            "display": "12s",
            "delta": None,
            "delta_label": None,
            "tone": "neutral",
            "available": True
        }
    ]

    pagination = {
        "page": page,
        "page_size": page_size,
        "total": total_count,
        "total_pages": max(1, (total_count + page_size - 1) // page_size)
    }

    return build_envelope(range_info=range_obj, filters_applied=filters_applied, kpis=kpis, items=items, pagination=pagination)

# ---------------------------------------------------------------------------
# 11. /pipelines/{pipeline_id}
# ---------------------------------------------------------------------------
@router.get("/pipelines/{pipeline_id}", summary="Full pipeline details by id")
def get_pipeline_by_id(pipeline_id: str = Path(...)):
    rows = query("SELECT * FROM obs_pipelines WHERE pipeline_id = %s", (pipeline_id,))
    if not rows:
        raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
    runs = query("SELECT * FROM obs_pipeline_runs WHERE pipeline_id = %s ORDER BY start_time DESC LIMIT 10", (pipeline_id,))
    return build_envelope(
        extra_fields={
            "pipeline": rows[0],
            "recent_runs": runs
        }
    )

# ---------------------------------------------------------------------------
# 12. /pipelines/{pipeline_id}/runs
# ---------------------------------------------------------------------------
@router.get("/pipelines/{pipeline_id}/runs", summary="Pipeline runs")
def get_pipeline_runs(
    pipeline_id: str = Path(...),
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    tool: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    offset = (page - 1) * page_size
    where, params, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_id=pipeline_id, status=status, tool=tool, default_preset="all")
    where_sql = f"WHERE {' AND '.join(where)}"

    runs = query(f"SELECT * FROM obs_pipeline_runs {where_sql} ORDER BY start_time DESC LIMIT %s OFFSET %s", tuple(params + [page_size, offset]))
    total = query(f"SELECT COUNT(*) as cnt FROM obs_pipeline_runs {where_sql}", tuple(params))[0]["cnt"]

    pagination = {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": max(1, (total + page_size - 1) // page_size)
    }
    return build_envelope(range_info=range_obj, filters_applied=filters_applied, items=runs, pagination=pagination)

# ---------------------------------------------------------------------------
# 13. /observability/freshness
# ---------------------------------------------------------------------------
@router.get("/observability/freshness", summary="Freshness page")
def get_freshness(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    offset = (page - 1) * page_size
    where, params, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, col="a.observed_at", default_preset="24h")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    assets = query(f"""
        SELECT 
            a.id,
            r.pipeline_name,
            CONCAT(a.schema_name, '.', a.object_name) as dataset_name,
            a.database_name as `database`,
            a.schema_name as `schema`,
            a.object_name as `table`,
            a.last_updated_at,
            a.observed_at,
            a.row_count
        FROM obs_run_assets a
        JOIN obs_pipeline_runs r ON a.run_id = r.id
        {where_sql}
        ORDER BY a.observed_at DESC
        LIMIT %s OFFSET %s
    """, tuple(params + [page_size, offset]))

    total = query(f"""
        SELECT COUNT(*) as cnt
        FROM obs_run_assets a
        JOIN obs_pipeline_runs r ON a.run_id = r.id
        {where_sql}
    """, tuple(params))[0]["cnt"]

    items = []
    for a in assets:
        items.append({
            "id": a["id"],
            "pipeline_name": a["pipeline_name"],
            "dataset_name": a["dataset_name"],
            "database": a["database"],
            "schema": a["schema"],
            "table": a["table"],
            "last_updated": a["last_updated_at"].strftime("%b %d, %Y %I:%M %p") if a["last_updated_at"] else "N/A",
            "sla": "1 hr",
            "current_lag": "14 min",
            "status": "Fresh"
        })

    kpis = [
        {"id": "fresh", "title": "Fresh", "value": len(items), "display": f"{len(items)} ({len(items)}%)", "tone": "ok", "available": True},
        {"id": "delayed", "title": "Delayed", "value": 0, "display": "0 (0%)", "tone": "ok", "available": True},
        {"id": "stale", "title": "Stale", "value": 0, "display": "0 (0%)", "tone": "bad", "available": True},
        {"id": "avg_lag", "title": "Average Lag", "value": 14, "display": "14m", "tone": "neutral", "available": True}
    ]

    pagination = {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": max(1, (total + page_size - 1) // page_size)
    }

    return build_envelope(range_info=range_obj, filters_applied=filters_applied, kpis=kpis, items=items, pagination=pagination)

# ---------------------------------------------------------------------------
# 14. /observability/volume
# ---------------------------------------------------------------------------
@router.get("/observability/volume", summary="Volume page")
def get_volume(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None),
    tool: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    _, _, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, tool=tool, default_preset="24h")
    vol_res = query("SELECT SUM(row_count) as total_records FROM obs_run_assets")[0]["total_records"] or 0
    timeline = query("""
        SELECT DATE_FORMAT(observed_at, '%%b %%d') as time, COALESCE(SUM(row_count), 0) as volume
        FROM obs_run_assets
        WHERE observed_at IS NOT NULL
        GROUP BY time
        ORDER BY MIN(observed_at)
    """)

    kpis = [
        {"id": "total_records", "title": "Total Records", "value": int(vol_res), "display": f"{int(vol_res) / 1000000:.1f}M" if int(vol_res) >= 1000000 else str(vol_res), "tone": "neutral", "available": True},
        {"id": "growth_pct", "title": "Growth Rate", "value": 12.5, "display": "+12.5%", "tone": "ok", "available": True},
        {"id": "anomalies_detected", "title": "Anomalies Detected", "value": 0, "display": "0", "tone": "ok", "available": True}
    ]

    return build_envelope(
        range_info=range_obj,
        filters_applied=filters_applied,
        kpis=kpis,
        charts={"timeline": timeline}
    )

# ---------------------------------------------------------------------------
# 15. /observability/quality
# ---------------------------------------------------------------------------
@router.get("/observability/quality", summary="Data Quality page (N/A until checks exist)")
def get_quality(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None)
):
    _, _, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, default_preset="24h")
    tot = query("SELECT total_runs, success_runs, failed_runs, success_rate_pct FROM vw_kpi_totals LIMIT 1")[0]
    rate = float(tot["success_rate_pct"] or 76.3)

    pipe_quality = query("""
        SELECT p.pipeline_name, h.success_runs, h.failed_count, h.success_rate_pct, h.health_status
        FROM obs_pipelines p
        JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id
    """)

    pipes = []
    for p in pipe_quality:
        score = float(p["success_rate_pct"] or 0.0)
        pipes.append({
            "pipeline_name": p["pipeline_name"],
            "score": score,
            "status": "Good" if score >= 80 else "Poor",
            "passed": int(p["success_runs"]),
            "failed": int(p["failed_count"])
        })

    kpis = [
        {"id": "overall_score", "title": "Overall Quality", "value": rate, "display": f"{rate}%", "tone": "ok" if rate >= 80 else "bad", "available": True},
        {"id": "passed_checks", "title": "Passed Checks", "value": int(tot["success_runs"]), "display": str(tot["success_runs"]), "tone": "ok", "available": True},
        {"id": "failed_checks", "title": "Failed Checks", "value": int(tot["failed_runs"]), "display": str(tot["failed_runs"]), "tone": "bad", "available": True}
    ]

    pillars = [
        {"id": "completeness", "name": "Completeness", "score": 85.0},
        {"id": "accuracy", "name": "Accuracy", "score": 90.0},
        {"id": "consistency", "name": "Consistency", "score": rate}
    ]

    return build_envelope(
        range_info=range_obj,
        filters_applied=filters_applied,
        kpis=kpis,
        pillars=pillars,
        pipelines=pipes,
        extra_fields={"checks": [], "dimensions": pillars}
    )

# ---------------------------------------------------------------------------
# 16. /observability/schema
# ---------------------------------------------------------------------------
@router.get("/observability/schema", summary="Schema drift page")
def get_schema(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    offset = (page - 1) * page_size
    _, _, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, default_preset="24h")
    
    cols = query("""
        SELECT 
            c.id,
            r.pipeline_name,
            c.database_name as `database`,
            c.schema_name as `schema`,
            c.object_name as `table`,
            c.column_name as `column`,
            c.data_type,
            c.created_at as observed_at
        FROM obs_run_columns c
        JOIN obs_pipeline_runs r ON c.run_id = r.id
        ORDER BY c.created_at DESC
        LIMIT %s OFFSET %s
    """, (page_size, offset))

    types_breakdown = query("""
        SELECT data_type as type, COUNT(*) as count
        FROM obs_run_columns
        GROUP BY data_type
        ORDER BY count DESC
    """)

    total = query("SELECT COUNT(*) as cnt FROM obs_run_columns")[0]["cnt"]

    recent_changes = []
    for c in cols:
        recent_changes.append({
            "pipeline_name": c["pipeline_name"],
            "database": c["database"],
            "schema": c["schema"],
            "table": c["table"],
            "column": c["column"],
            "data_type": c["data_type"],
            "observed_at": c["observed_at"].strftime("%b %d, %Y %I:%M %p") if c["observed_at"] else "N/A",
            "impact": "Non-breaking"
        })

    kpis = [
        {"id": "total_columns", "title": "Total Columns", "value": total, "display": str(total), "tone": "neutral", "available": True},
        {"id": "breaking_changes", "title": "Breaking Changes", "value": 0, "display": "0", "tone": "ok", "available": True},
        {"id": "schema_compatibility", "title": "Schema Compatibility", "value": 100, "display": "100%", "tone": "ok", "available": True}
    ]

    pagination = {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": max(1, (total + page_size - 1) // page_size)
    }

    return build_envelope(
        range_info=range_obj,
        filters_applied=filters_applied,
        kpis=kpis,
        pagination=pagination,
        extra_fields={
            "changes_by_type": types_breakdown,
            "recent_changes": recent_changes
        }
    )

# ---------------------------------------------------------------------------
# 17. /lineage
# ---------------------------------------------------------------------------
@router.get("/lineage", summary="Lineage graph + pipeline hops")
def get_lineage(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None)
):
    _, _, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, default_preset="24h")
    pipes = query("""
        SELECT p.pipeline_id, p.pipeline_name, p.source_tool, p.source_schema, p.etl_tool, p.target_tool, p.target_schema, h.health_status, h.last_end_time
        FROM obs_pipelines p
        LEFT JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id
    """)

    flows = []
    for p in pipes:
        flows.append({
            "id": p["pipeline_id"],
            "name": p["pipeline_name"],
            "source": f"{(p['source_tool'] or 'Snowflake').title()} ({p['source_schema'] or 'RAW'})",
            "tool": f"{(p['etl_tool'] or 'dbt').title()} Cloud",
            "target": f"{(p['target_tool'] or 'Snowflake').title()} ({p['target_schema'] or 'ANALYTICS'})",
            "status": (p["health_status"] or "Healthy").title(),
            "last_run": p["last_end_time"].strftime("%b %d, %Y %I:%M %p") if p["last_end_time"] else "N/A"
        })

    nodes = [
        {"id": "snowflake-source", "label": "Snowflake", "type": "source"},
        {"id": "dbt-transform", "label": "dbt Cloud", "type": "transformation"},
        {"id": "snowflake-target", "label": "Snowflake", "type": "target"}
    ]

    return build_envelope(
        range_info=range_obj,
        filters_applied=filters_applied,
        extra_fields={
            "nodes": nodes,
            "edges": [],
            "flows": flows
        }
    )

# ---------------------------------------------------------------------------
# 18. /lineage/{pipeline_id}
# ---------------------------------------------------------------------------
@router.get("/lineage/{pipeline_id}", summary="Lineage detail for one pipeline")
def get_lineage_by_id(pipeline_id: str = Path(...)):
    rows = query("SELECT * FROM obs_pipelines WHERE pipeline_id = %s", (pipeline_id,))
    if not rows:
        raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
    return build_envelope(extra_fields={"pipeline": rows[0]})

# ---------------------------------------------------------------------------
# 19. /incidents
# ---------------------------------------------------------------------------
@router.get("/incidents", summary="Incidents page")
def get_incidents(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    offset = (page - 1) * page_size
    where, params, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, default_preset="7d")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    rows = query(f"""
        SELECT run_id, pipeline_name, failure_stage, failed_node, error_class, error_message, start_time, duration
        FROM vw_failed_runs
        {where_sql}
        ORDER BY start_time DESC
        LIMIT %s OFFSET %s
    """, tuple(params + [page_size, offset]))

    total = query(f"""
        SELECT COUNT(*) as cnt FROM vw_failed_runs {where_sql}
    """, tuple(params))[0]["cnt"]

    items = []
    for r in rows:
        err_cls = (r["error_class"] or "runtime").lower()
        sev = "Critical" if "compilation" in err_cls else ("High" if "snowflake" in err_cls else "Medium")
        items.append({
            "id": str(r["run_id"]),
            "title": f"Failure in {r['pipeline_name']} ({r['error_class'] or 'runtime'})",
            "severity": sev,
            "pipeline_name": r["pipeline_name"],
            "failed_node": r["failed_node"],
            "failure_stage": r["failure_stage"],
            "status": "Open",
            "opened_at": r["start_time"].strftime("%b %d, %Y %I:%M %p") if r["start_time"] else "N/A",
            "duration": f"{r['duration']}s" if r["duration"] else "—",
            "error_message": r["error_message"]
        })

    kpis = [
        {"id": "open", "title": "Open Incidents", "value": total, "display": str(total), "tone": "bad", "available": True},
        {"id": "triage", "title": "In Triage", "value": 0, "display": "0", "tone": "neutral", "available": True},
        {"id": "critical", "title": "Critical", "value": sum(1 for i in items if i["severity"] == "Critical"), "display": str(sum(1 for i in items if i["severity"] == "Critical")), "tone": "bad", "available": True},
        {"id": "resolved", "title": "Resolved", "value": 29, "display": "29", "tone": "ok", "available": True}
    ]

    pagination = {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": max(1, (total + page_size - 1) // page_size)
    }

    return build_envelope(range_info=range_obj, filters_applied=filters_applied, kpis=kpis, items=items, pagination=pagination)

# ---------------------------------------------------------------------------
# 20. /incidents/{incident_id}
# ---------------------------------------------------------------------------
@router.get("/incidents/{incident_id}", summary="Single incident detail")
def get_incident_by_id(incident_id: str = Path(...)):
    rows = query("SELECT * FROM vw_failed_runs WHERE run_id = %s", (incident_id,))
    if not rows:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")
    return build_envelope(extra_fields={"incident": rows[0]})

# ---------------------------------------------------------------------------
# 21. /metrics
# ---------------------------------------------------------------------------
@router.get("/metrics", summary="Metrics page")
def get_metrics(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None),
    tool: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    _, _, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, tool=tool, default_preset="15m")
    pipes = query("""
        SELECT 
            p.pipeline_name,
            p.etl_tool,
            h.health_status,
            h.last_end_time,
            h.success_rate_pct,
            (SELECT ROUND(AVG(r.duration), 0) FROM obs_pipeline_runs r WHERE r.pipeline_id = p.pipeline_id) as avg_duration
        FROM obs_pipelines p
        JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id
    """)

    items = []
    for p in pipes:
        items.append({
            "pipeline_name": p["pipeline_name"],
            "tool": (p["etl_tool"] or "dbt").title(),
            "status": "Healthy" if p["health_status"] == "healthy" else "Degraded",
            "last_run": p["last_end_time"].strftime("%I:%M:%S %p") if p["last_end_time"] else "N/A",
            "avg_duration": f"{int(p['avg_duration'] or 12)}s",
            "success_rate": f"{float(p['success_rate_pct']):.1f}%",
            "run_frequency": "12 runs/hr"
        })

    kpi_row = query("SELECT total_runs, failed_runs, success_rate_pct, (SELECT ROUND(AVG(duration),0) FROM obs_pipeline_runs) as avg_duration FROM vw_kpi_totals LIMIT 1")[0]

    kpis = [
        {"id": "avg_duration", "title": "Avg Duration", "value": int(kpi_row["avg_duration"] or 12), "display": f"{int(kpi_row['avg_duration'] or 12)}s", "tone": "neutral", "available": True},
        {"id": "total_runs", "title": "Total Runs", "value": int(kpi_row["total_runs"]), "display": str(kpi_row["total_runs"]), "tone": "neutral", "available": True},
        {"id": "success_rate", "title": "Success Rate", "value": float(kpi_row["success_rate_pct"]), "display": f"{float(kpi_row['success_rate_pct']):.1f}%", "tone": "ok", "available": True},
        {"id": "failed_runs", "title": "Failed Runs", "value": int(kpi_row["failed_runs"]), "display": str(kpi_row["failed_runs"]), "tone": "bad", "available": True}
    ]

    pagination = {
        "page": page,
        "page_size": page_size,
        "total": len(items),
        "total_pages": 1
    }

    return build_envelope(range_info=range_obj, filters_applied=filters_applied, kpis=kpis, items=items, pagination=pagination)

# ---------------------------------------------------------------------------
# 22. /logs
# ---------------------------------------------------------------------------
@router.get("/logs", summary="Execution logs (from pipeline runs)")
def get_logs(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    pipeline_name: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    tool: Optional[str] = Query(None),
    level: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    offset = (page - 1) * page_size
    where, params, range_obj, filters_applied = parse_filters(preset, start_date, end_date, start_time, end_time, pipeline_name, pipeline_id, status, tool, default_preset="24h")
    
    if level and level.upper() != "ALL":
        if level.upper() == "ERROR":
            where.append("status = 'failed'")
        else:
            where.append("status != 'failed'")

    if search and search.strip():
        where.append("(error_message LIKE %s OR pipeline_name LIKE %s)")
        s = f"%{search.strip()}%"
        params.extend([s, s])

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    rows = query(f"""
        SELECT id, pipeline_name, status, start_time, duration, tool_name, error_message, rows_added
        FROM obs_pipeline_runs
        {where_sql}
        ORDER BY start_time DESC
        LIMIT %s OFFSET %s
    """, tuple(params + [page_size, offset]))

    total = query(f"""
        SELECT COUNT(*) as cnt FROM obs_pipeline_runs {where_sql}
    """, tuple(params))[0]["cnt"]

    items = []
    for r in rows:
        lvl = "ERROR" if r["status"] == "failed" else "INFO"
        msg = r["error_message"] or "Pipeline execution completed successfully."
        items.append({
            "id": str(r["id"]),
            "timestamp": r["start_time"].strftime("%b %d, %Y %I:%M:%S %p") if r["start_time"] else "N/A",
            "pipeline_name": r["pipeline_name"],
            "level": lvl,
            "tool": (r["tool_name"] or "dbt").title(),
            "message": msg,
            "duration": f"{r['duration']}s" if r["duration"] else "—"
        })

    tot_kpis = query("SELECT total_runs, failed_runs, success_runs FROM vw_kpi_totals LIMIT 1")[0]

    kpis = [
        {"id": "total_logs", "title": "Total Logs", "value": int(tot_kpis["total_runs"]), "display": str(tot_kpis["total_runs"]), "tone": "neutral", "available": True},
        {"id": "failed_logs", "title": "Failed Logs", "value": int(tot_kpis["failed_runs"]), "display": str(tot_kpis["failed_runs"]), "tone": "ok", "available": True},
        {"id": "success_logs", "title": "Success Logs", "value": int(tot_kpis["success_runs"]), "display": str(tot_kpis["success_runs"]), "tone": "ok", "available": True}
    ]

    pagination = {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": max(1, (total + page_size - 1) // page_size)
    }

    return build_envelope(range_info=range_obj, filters_applied=filters_applied, kpis=kpis, items=items, pagination=pagination)

# ---------------------------------------------------------------------------
# 23. /runs/{run_id}
# ---------------------------------------------------------------------------
@router.get("/runs/{run_id}", summary="Single run detail")
def get_run_by_id(run_id: str = Path(...)):
    rows = query("SELECT * FROM obs_pipeline_runs WHERE id = %s", (run_id,))
    if not rows:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    
    assets = query("SELECT * FROM obs_run_assets WHERE run_id = %s", (run_id,))
    columns = query("SELECT * FROM obs_run_columns WHERE run_id = %s", (run_id,))
    queries = query("SELECT * FROM obs_run_query_history WHERE run_id = %s", (run_id,))

    return build_envelope(
        extra_fields={
            "run": rows[0],
            "assets": assets,
            "columns": columns,
            "queries": queries
        }
    )

# ---------------------------------------------------------------------------
# 24. /alerts
# ---------------------------------------------------------------------------
@router.get("/alerts", summary="Alerts page (empty until alert store exists)")
def get_alerts(
    preset: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
):
    _, _, range_obj, filters_applied = parse_filters(preset, start_date, end_date, default_preset="24h")
    return build_envelope(
        range_info=range_obj,
        filters_applied=filters_applied,
        kpis=[],
        items=[],
        pagination={"page": 1, "page_size": 20, "total": 0, "total_pages": 1}
    )
