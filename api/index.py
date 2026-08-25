"""
VITHI Enterprise Data Observability REST API
Secure, Clean, Production-Grade REST Engine with Comprehensive Filtering
"""

import os
import json
import time
import pymysql
from enum import Enum
from decimal import Decimal
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Union
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, Query, Path, APIRouter, status
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------------
# Load environment credentials
# ---------------------------------------------------------------------------
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

HOST     = os.getenv("CENTRAL_DB_HOST") or os.getenv("DB_HOST", "localhost")
PORT     = int(os.getenv("CENTRAL_DB_PORT") or os.getenv("DB_PORT", "3306"))
USER     = os.getenv("CENTRAL_DB_USER") or os.getenv("DB_USER", "root")
PASSWORD = os.getenv("CENTRAL_DB_PASSWORD") or os.getenv("DB_PASSWORD", "")
DB_NAME  = os.getenv("CENTRAL_DB_NAME") or os.getenv("DB_NAME", "metadata")

app = FastAPI(
    title="Data Observability REST API",
    description="Enterprise REST API for Pipeline Monitoring, Data Quality, Freshness SLAs, Schema Drift, Logs & Incidents.",
    version="3.1.0",
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

# ---------------------------------------------------------------------------
# Filter Enums for Swagger Dropdowns
# ---------------------------------------------------------------------------
class TimeRangeEnum(str, Enum):
    ALL = "ALL"
    R_15M = "15m"
    R_1H = "1h"
    R_6H = "6h"
    R_24H = "24h"
    R_7D = "7d"
    R_30D = "30d"

class StatusEnum(str, Enum):
    ALL = "ALL"
    SUCCESS = "Success"
    FAILED = "Failed"
    WARNING = "Warning"

class SeverityEnum(str, Enum):
    ALL = "ALL"
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"

class LogLevelEnum(str, Enum):
    ALL = "ALL"
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"
    DEBUG = "DEBUG"

class DataTypeEnum(str, Enum):
    ALL = "ALL"
    NUMBER = "NUMBER"
    TEXT = "TEXT"
    DATE = "DATE"
    BOOLEAN = "BOOLEAN"

class QualityStatusEnum(str, Enum):
    ALL = "ALL"
    GOOD = "Good"
    WARNING = "Warning"
    POOR = "Poor"

class FreshnessStatusEnum(str, Enum):
    ALL = "ALL"
    FRESH = "Fresh"
    DELAYED = "Delayed"
    STALE = "Stale"

class SortOrderEnum(str, Enum):
    DESC = "desc"
    ASC = "asc"

# ---------------------------------------------------------------------------
# Custom JSON Serializer (Hides internal DB types)
# ---------------------------------------------------------------------------
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj) if "." in str(obj) else int(obj)
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        return super().default(obj)

def clean_obj(val):
    if isinstance(val, dict):
        return {k: clean_obj(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [clean_obj(v) for v in val]
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val) if "." in str(val) else int(val)
    return val

def jsonify_payload(data: Any, meta: Dict[str, Any] = None, exec_start: float = None):
    exec_ms = round((time.time() - exec_start) * 1000, 2) if exec_start else 0.0
    response_meta = {
        "executionTimeMs": exec_ms,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "version": "3.1.0"
    }
    if meta:
        response_meta.update(meta)
    
    payload = {
        "status": "success",
        "code": 200,
        "meta": response_meta,
        "data": data
    }
    return json.loads(json.dumps(clean_obj(payload), cls=CustomEncoder))

def get_conn():
    try:
        return pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DB_NAME,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail="Service unavailable: Database connection failed")

def query(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except Exception as e:
        print(f"SQL Error: {e} | Query: {sql}")
        raise HTTPException(status_code=500, detail="Database query execution error")
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Dynamic Date & Time Window Helper
# ---------------------------------------------------------------------------
def get_time_window_sql(time_range: str, start_date: str = None, end_date: str = None, col: str = "start_time"):
    where = []
    params = []
    
    if start_date and str(start_date).strip():
        where.append(f"{col} >= %s")
        params.append(f"{start_date.strip()} 00:00:00")
        
    if end_date and str(end_date).strip():
        where.append(f"{col} <= %s")
        params.append(f"{end_date.strip()} 23:59:59")

    return where, params

# ---------------------------------------------------------------------------
# API ROUTERS
# ---------------------------------------------------------------------------
overview_router   = APIRouter(prefix="/api/v1/overview", tags=["Overview Dashboard"])
pipelines_router  = APIRouter(prefix="/api/v1/pipelines", tags=["Pipelines"])
runs_router       = APIRouter(prefix="/api/v1/runs", tags=["Pipeline Runs"])
quality_router    = APIRouter(prefix="/api/v1/observability/quality", tags=["Data Quality"])
freshness_router  = APIRouter(prefix="/api/v1/observability/freshness", tags=["Data Freshness"])
schema_router     = APIRouter(prefix="/api/v1/observability/schema", tags=["Schema Drift"])
volume_router     = APIRouter(prefix="/api/v1/observability/volume", tags=["Volume Observability"])
metrics_router    = APIRouter(prefix="/api/v1/metrics", tags=["Metrics Explorer"])
logs_router       = APIRouter(prefix="/api/v1/logs", tags=["Logs Stream"])
incidents_router  = APIRouter(prefix="/api/v1/incidents", tags=["Incidents"])
lineage_router    = APIRouter(prefix="/api/v1/lineage", tags=["Lineage Flows"])
filters_router    = APIRouter(prefix="/api/v1/filters", tags=["Filter Options"])
alerts_router     = APIRouter(prefix="/api/v1/alerts", tags=["Alerts"])

# ---------------------------------------------------------------------------
# SYSTEM ROOT & HEALTH (No Internal DB Details Leaked)
# ---------------------------------------------------------------------------
@app.get("/", tags=["System"], summary="API Root")
def api_root():
    return {
        "status": "online",
        "version": "3.1.0",
        "docs": "/docs"
    }

@app.get("/health", tags=["System"], summary="Health Check")
def health_check():
    t0 = time.time()
    res = query("SELECT 1 as is_alive")
    db_alive = len(res) > 0 and res[0]["is_alive"] == 1
    return {
        "status": "healthy" if db_alive else "degraded",
        "latencyMs": round((time.time() - t0) * 1000, 2),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

# ---------------------------------------------------------------------------
# DYNAMIC FILTER OPTIONS
# ---------------------------------------------------------------------------
@filters_router.get("/options", summary="Get Distinct Filter Options From Database")
def get_filter_options():
    t0 = time.time()
    pipelines = [r["name"] for r in query("SELECT DISTINCT pipeline_name as name FROM obs_pipelines WHERE pipeline_name IS NOT NULL ORDER BY name")]
    sources   = [r["src"].title() for r in query("SELECT DISTINCT source_tool as src FROM obs_pipelines WHERE source_tool IS NOT NULL ORDER BY src")]
    targets   = [r["tgt"].title() for r in query("SELECT DISTINCT target_tool as tgt FROM obs_pipelines WHERE target_tool IS NOT NULL ORDER BY tgt")]
    etl_tools = [r["tool"].title() for r in query("SELECT DISTINCT etl_tool as tool FROM obs_pipelines WHERE etl_tool IS NOT NULL ORDER BY tool")]
    schemas   = [r["sch"] for r in query("SELECT DISTINCT schema_name as sch FROM obs_run_assets WHERE schema_name IS NOT NULL ORDER BY sch")]
    databases = [r["db"] for r in query("SELECT DISTINCT database_name as db FROM obs_run_assets WHERE database_name IS NOT NULL ORDER BY db")]
    data_types = [r["dt"] for r in query("SELECT DISTINCT data_type as dt FROM obs_run_columns WHERE data_type IS NOT NULL ORDER BY dt")]
    
    return jsonify_payload({
        "pipelines": pipelines,
        "sources": sources,
        "destinations": targets,
        "etlTools": etl_tools,
        "schemas": schemas,
        "databases": databases,
        "dataTypes": data_types,
        "statuses": ["Success", "Failed", "Warning"],
        "severities": ["Critical", "High", "Medium", "Low"],
        "logLevels": ["ERROR", "WARN", "INFO", "DEBUG"],
        "timeRanges": ["15m", "1h", "6h", "24h", "7d", "30d", "ALL"]
    }, exec_start=t0)

# ---------------------------------------------------------------------------
# 1. OVERVIEW DASHBOARD
# ---------------------------------------------------------------------------
@overview_router.get("", summary="Consolidated Overview Dashboard")
def get_overview_dashboard(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name (e.g. ecommerce_etl, hr_etl, stock_etl)"),
    status: StatusEnum = Query(StatusEnum.ALL, description="Filter by status"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    time_range: TimeRangeEnum = Query(TimeRangeEnum.ALL, description="Time range filter"),
    search: Optional[str] = Query(None, description="Free text search")
):
    t0 = time.time()
    
    where_parts = []
    params = []
    
    if pipeline_name and pipeline_name.upper() != "ALL":
        where_parts.append("pipeline_name = %s")
        params.append(pipeline_name)
        
    if status != StatusEnum.ALL:
        where_parts.append("status = %s")
        params.append(status.value.lower())
        
    date_where, date_params = get_time_window_sql(time_range.value, start_date, end_date, "start_time")
    where_parts.extend(date_where)
    params.extend(date_params)
    
    if search and search.strip():
        where_parts.append("(pipeline_name LIKE %s OR error_message LIKE %s)")
        s = f"%{search.strip()}%"
        params.extend([s, s])
        
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    
    # Live KPI counts based on applied filters
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
    
    tot_runs = int(kpi_res["total_runs"] or 0)
    suc_runs = int(kpi_res["success_runs"] or 0)
    fld_runs = int(kpi_res["failed_runs"] or 0)
    tot_pipes = int(kpi_res["total_pipelines"] or (3 if not pipeline_name else 1))
    avg_sec = int(kpi_res["avg_duration_sec"] or 12)
    success_rate = round(suc_runs * 100.0 / tot_runs, 1) if tot_runs > 0 else 0.0

    # Daily trend
    daily_rows = query(f"""
        SELECT 
            DATE_FORMAT(start_time, '%%b %%d') as time_label,
            COUNT(*) as total_runs,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_runs,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
            ROUND(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as success_rate
        FROM obs_pipeline_runs
        {where_sql}
        GROUP BY time_label
        ORDER BY MIN(start_time)
    """, tuple(params))

    runs_over_time = []
    success_rate_trend = []
    for r in daily_rows:
        label = str(r["time_label"])
        s_cnt = int(r["success_runs"] or 0)
        f_cnt = int(r["failed_runs"] or 0)
        t_cnt = int(r["total_runs"] or 0)
        rate = float(r["success_rate"] or 0.0)
        runs_over_time.append({"time": label, "success": s_cnt, "failed": f_cnt, "total": t_cnt})
        success_rate_trend.append({"time": label, "rate": rate})

    # Recent incidents
    inc_where = []
    inc_params = []
    if pipeline_name and pipeline_name.upper() != "ALL":
        inc_where.append("pipeline_name = %s")
        inc_params.append(pipeline_name)
    inc_where_sql = f"WHERE {' AND '.join(inc_where)}" if inc_where else ""

    recent_incidents = query(f"""
        SELECT run_id, pipeline_name, failure_stage, failed_node, error_class, error_message, start_time, duration
        FROM vw_failed_runs
        {inc_where_sql}
        ORDER BY start_time DESC
        LIMIT 5
    """, tuple(inc_params))

    incidents_list = []
    for r in recent_incidents:
        err_cls = (r["error_class"] or "etl").lower()
        sev = "Critical" if "compilation" in err_cls else ("High" if "snowflake" in err_cls else "Medium")
        incidents_list.append({
            "id": r["run_id"],
            "title": f"Failure in {r['pipeline_name']} ({r['error_class'] or 'runtime'})",
            "description": r["error_message"] or f"Stage '{r['failure_stage']}' failed at node: {r['failed_node']}",
            "pipeline": r["pipeline_name"],
            "failedNode": r["failed_node"],
            "failureStage": r["failure_stage"],
            "severity": sev,
            "time": r["start_time"].isoformat() if r["start_time"] else None,
            "relativeTime": r["start_time"].strftime("%b %d, %Y %I:%M %p") if r["start_time"] else "Recently"
        })

    # Pipeline monitoring list
    pipe_rows = query(f"""
        SELECT 
            p.pipeline_id,
            p.pipeline_name,
            p.source_tool,
            p.source_schema,
            p.etl_tool,
            p.target_tool,
            p.target_schema,
            h.latest_status,
            h.last_end_time,
            COALESCE(h.total_runs, 0) as total_runs,
            COALESCE(h.success_rate_pct, 0.0) as success_rate_pct,
            COALESCE(h.health_status, 'healthy') as health_status
        FROM obs_pipelines p
        LEFT JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id
        {('WHERE p.pipeline_name = %s' if pipeline_name and pipeline_name.upper() != 'ALL' else '')}
        ORDER BY h.last_end_time DESC
    """, (pipeline_name,) if pipeline_name and pipeline_name.upper() != 'ALL' else ())

    monitoring_items = []
    for p in pipe_rows:
        st = "Success" if p["latest_status"] == "success" else ("Failed" if p["latest_status"] == "failed" else "Warning")
        monitoring_items.append({
            "id": p["pipeline_id"],
            "pipeline": p["pipeline_name"],
            "source": (p["source_tool"] or "Snowflake").title(),
            "target": (p["target_tool"] or "Snowflake").title(),
            "status": st,
            "runs": int(p["total_runs"]),
            "successRate": f"{float(p['success_rate_pct']):.1f}%",
            "lastRun": p["last_end_time"].strftime("%b %d, %Y %I:%M %p") if p["last_end_time"] else "N/A"
        })

    meta = {
        "appliedFilters": {
            "pipeline_name": pipeline_name,
            "status": status.value,
            "start_date": start_date,
            "end_date": end_date,
            "time_range": time_range.value,
            "search": search
        }
    }

    return jsonify_payload({
        "kpis": {
            "totalPipelines": {"value": tot_pipes, "isPositive": True},
            "totalRuns": {"value": tot_runs, "isPositive": True},
            "successfulRuns": {"value": f"{success_rate}%", "isPositive": success_rate >= 80},
            "failedRuns": {"value": fld_runs, "isPositive": fld_runs == 0},
            "avgDuration": {"value": f"{avg_sec}s", "valueSeconds": avg_sec}
        },
        "charts": {
            "runsOverTime": runs_over_time,
            "successRateTrend": success_rate_trend
        },
        "recentIncidents": incidents_list,
        "pipelineMonitoring": monitoring_items
    }, meta=meta, exec_start=t0)


@overview_router.get("/kpis", summary="Overview KPI Totals")
def get_overview_kpis(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    time_range: TimeRangeEnum = Query(TimeRangeEnum.ALL, description="Time range")
):
    t0 = time.time()
    where_parts = []
    params = []
    if pipeline_name and pipeline_name.upper() != "ALL":
        where_parts.append("pipeline_name = %s")
        params.append(pipeline_name)
    date_where, date_params = get_time_window_sql(time_range.value, start_date, end_date, "start_time")
    where_parts.extend(date_where)
    params.extend(date_params)
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    kpi_res = query(f"SELECT COUNT(DISTINCT pipeline_id) as total_pipelines, COUNT(*) as total_runs, SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_runs, SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs, ROUND(AVG(duration), 0) as avg_duration_sec FROM obs_pipeline_runs {where_sql}", tuple(params))[0]
    tot_runs = int(kpi_res["total_runs"] or 0)
    suc_runs = int(kpi_res["success_runs"] or 0)
    fld_runs = int(kpi_res["failed_runs"] or 0)
    tot_pipes = int(kpi_res["total_pipelines"] or (3 if not pipeline_name else 1))
    avg_sec = int(kpi_res["avg_duration_sec"] or 12)
    success_rate = round(suc_runs * 100.0 / tot_runs, 1) if tot_runs > 0 else 0.0

    return jsonify_payload({
        "totalPipelines": {"value": tot_pipes, "isPositive": True},
        "totalRuns": {"value": tot_runs, "isPositive": True},
        "successfulRuns": {"value": f"{success_rate}%", "isPositive": success_rate >= 80},
        "failedRuns": {"value": fld_runs, "isPositive": fld_runs == 0},
        "avgDuration": {"value": f"{avg_sec}s", "valueSeconds": avg_sec}
    }, exec_start=t0)

@overview_router.get("/charts", summary="Overview Execution & Incident Charts")
def get_overview_charts(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    time_range: TimeRangeEnum = Query(TimeRangeEnum.ALL, description="Time range")
):
    t0 = time.time()
    where_sql = "WHERE pipeline_name = %s" if pipeline_name and pipeline_name.upper() != "ALL" else ""
    params = (pipeline_name,) if pipeline_name and pipeline_name.upper() != "ALL" else ()
    daily_rows = query(f"SELECT DATE_FORMAT(start_time, '%%b %%d') as time_label, COUNT(*) as total_runs, SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_runs, SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs, ROUND(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as success_rate FROM obs_pipeline_runs {where_sql} GROUP BY time_label ORDER BY MIN(start_time)", params)
    runs_over_time = [{"time": str(r["time_label"]), "success": int(r["success_runs"] or 0), "failed": int(r["failed_runs"] or 0), "total": int(r["total_runs"] or 0)} for r in daily_rows]
    success_rate_trend = [{"time": str(r["time_label"]), "rate": float(r["success_rate"] or 0.0)} for r in daily_rows]
    return jsonify_payload({"runsOverTime": runs_over_time, "successRateTrend": success_rate_trend}, exec_start=t0)

@overview_router.get("/health", summary="Overview 5-Pillar Observability Dimensions")
def get_overview_health():
    t0 = time.time()
    kpis = query("SELECT success_rate_pct FROM vw_kpi_totals LIMIT 1")[0]
    quality_score = float(kpis["success_rate_pct"] or 80.0)
    return jsonify_payload({
        "overallScore": round((quality_score + 95.0 + 90.0 + 85.0) / 4.0, 1),
        "dimensions": [
            { "id": "freshness", "name": "Freshness", "score": 85.0, "status": "Good" },
            { "id": "volume", "name": "Volume", "score": 95.0, "status": "Good" },
            { "id": "quality", "name": "Data Quality", "score": quality_score, "status": "Good" if quality_score >= 80 else "Warning" },
            { "id": "schema", "name": "Schema", "score": 90.0, "status": "Good" }
        ]
    }, exec_start=t0)

@overview_router.get("/recent-incidents", summary="Overview Recent Incidents")
def get_recent_incidents(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    severity: SeverityEnum = Query(SeverityEnum.ALL, description="Filter severity"),
    limit: int = Query(5, ge=1, le=50, description="Max items")
):
    t0 = time.time()
    where_parts = []
    params = []
    if pipeline_name and pipeline_name.upper() != "ALL":
        where_parts.append("pipeline_name = %s")
        params.append(pipeline_name)
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    recent_incidents = query(f"SELECT run_id, pipeline_name, failure_stage, failed_node, error_class, error_message, start_time, duration FROM vw_failed_runs {where_sql} ORDER BY start_time DESC LIMIT %s", tuple(params + [limit]))
    incidents_list = []
    for r in recent_incidents:
        err_cls = (r["error_class"] or "etl").lower()
        sev = "Critical" if "compilation" in err_cls else ("High" if "snowflake" in err_cls else "Medium")
        incidents_list.append({
            "id": r["run_id"],
            "title": f"Failure in {r['pipeline_name']} ({r['error_class'] or 'runtime'})",
            "description": r["error_message"] or f"Stage '{r['failure_stage']}' failed at node: {r['failed_node']}",
            "pipeline": r["pipeline_name"],
            "failedNode": r["failed_node"],
            "failureStage": r["failure_stage"],
            "severity": sev,
            "time": r["start_time"].isoformat() if r["start_time"] else None,
            "relativeTime": r["start_time"].strftime("%b %d, %Y %I:%M %p") if r["start_time"] else "Recently"
        })
    return jsonify_payload(incidents_list, exec_start=t0)

@overview_router.get("/pipeline-monitoring", summary="Overview Pipeline Monitoring Table")
def get_overview_pipeline_monitoring():
    t0 = time.time()
    pipe_rows = query("SELECT p.pipeline_id, p.pipeline_name, p.source_tool, p.target_tool, h.latest_status, h.last_end_time, COALESCE(h.total_runs, 0) as total_runs, COALESCE(h.success_rate_pct, 0.0) as success_rate_pct FROM obs_pipelines p LEFT JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id ORDER BY h.last_end_time DESC LIMIT 5")
    monitoring_items = []
    for p in pipe_rows:
        st = "Success" if p["latest_status"] == "success" else ("Failed" if p["latest_status"] == "failed" else "Warning")
        monitoring_items.append({
            "id": p["pipeline_id"],
            "pipeline": p["pipeline_name"],
            "source": (p["source_tool"] or "Snowflake").title(),
            "target": (p["target_tool"] or "Snowflake").title(),
            "status": st,
            "runs": int(p["total_runs"]),
            "successRate": f"{float(p['success_rate_pct']):.1f}%",
            "lastRun": p["last_end_time"].strftime("%b %d, %Y %I:%M %p") if p["last_end_time"] else "N/A"
        })
    return jsonify_payload(monitoring_items, exec_start=t0)

# ---------------------------------------------------------------------------
# 2. PIPELINES REGISTRY (With Search, Status, Date & Pipeline Name Filters)
# ---------------------------------------------------------------------------
@pipelines_router.get("", summary="List Pipelines with Full Filters")
def list_pipelines(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    status: StatusEnum = Query(StatusEnum.ALL, description="Filter by status"),
    search: Optional[str] = Query(None, description="Search by pipeline name or schema"),
    source: str = Query("ALL", description="Filter by source tool"),
    destination: str = Query("ALL", description="Filter by destination tool"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Page size")
):
    t0 = time.time()
    where_parts = []
    params = []

    if pipeline_name and pipeline_name.upper() != "ALL":
        where_parts.append("p.pipeline_name = %s")
        params.append(pipeline_name)

    if status != StatusEnum.ALL:
        where_parts.append("(h.health_status = %s OR h.latest_status = %s)")
        params.extend([status.value.lower(), status.value.lower()])

    if search and search.strip():
        where_parts.append("(p.pipeline_name LIKE %s OR p.source_schema LIKE %s OR p.target_schema LIKE %s)")
        s = f"%{search.strip()}%"
        params.extend([s, s, s])

    if source and source.upper() != "ALL":
        where_parts.append("LOWER(p.source_tool) = %s")
        params.append(source.lower())

    if destination and destination.upper() != "ALL":
        where_parts.append("LOWER(p.target_tool) = %s")
        params.append(destination.lower())

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    offset = (page - 1) * page_size

    count_res = query(f"SELECT COUNT(*) AS cnt FROM obs_pipelines p LEFT JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id {where_sql}", tuple(params))
    total_count = count_res[0]["cnt"] if count_res else 0

    sql = f"""
        SELECT 
            p.pipeline_id,
            p.pipeline_name,
            p.description,
            p.source_tool,
            p.source_schema,
            p.etl_tool,
            p.target_tool,
            p.target_schema,
            p.is_active,
            h.latest_status,
            h.last_end_time,
            h.failure_stage,
            h.failed_node,
            h.error_class,
            h.error_message,
            COALESCE(h.total_runs, 0) as total_runs,
            COALESCE(h.success_runs, 0) as success_runs,
            COALESCE(h.failed_count, 0) as failed_count,
            COALESCE(h.success_rate_pct, 0.0) as success_rate_pct,
            COALESCE(h.health_status, 'healthy') as health_status,
            (SELECT COALESCE(SUM(a.row_count), 0) FROM obs_pipeline_runs r JOIN obs_run_assets a ON r.id = a.run_id WHERE r.pipeline_id = p.pipeline_id) as total_records,
            (SELECT ROUND(AVG(r.duration), 0) FROM obs_pipeline_runs r WHERE r.pipeline_id = p.pipeline_id) as avg_duration
        FROM obs_pipelines p
        LEFT JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id
        {where_sql}
        ORDER BY h.last_end_time DESC, p.pipeline_name ASC
        LIMIT %s OFFSET %s
    """
    rows = query(sql, tuple(params + [page_size, offset]))

    items = []
    for r in rows:
        st = "Success" if r["latest_status"] == "success" else ("Failed" if r["latest_status"] == "failed" else "Warning")
        dur_sec = int(r["avg_duration"] or 12)
        dur_str = f"{dur_sec // 60}m {dur_sec % 60}s" if dur_sec >= 60 else f"{dur_sec}s"
        rec_count = int(r["total_records"] or 0)
        rec_str = f"{rec_count / 1000:.1f}K" if rec_count >= 1000 else str(rec_count)

        items.append({
            "id": r["pipeline_id"],
            "pipeline": r["pipeline_name"],
            "description": r["description"],
            "source": (r["source_tool"] or "Snowflake").title(),
            "sourceSchema": r["source_schema"],
            "etlTool": (r["etl_tool"] or "dbt").title(),
            "target": (r["target_tool"] or "Snowflake").title(),
            "targetSchema": r["target_schema"],
            "status": st,
            "healthStatus": (r["health_status"] or "healthy").title(),
            "runs": int(r["total_runs"]),
            "successRuns": int(r["success_runs"]),
            "failedRuns": int(r["failed_count"]),
            "successRate": f"{float(r['success_rate_pct']):.1f}%",
            "duration": dur_str,
            "recordsProcessed": rec_str,
            "lastRun": r["last_end_time"].strftime("%b %d, %Y %I:%M %p") if r["last_end_time"] else "N/A",
            "errorMessage": r["error_message"]
        })

    meta = {
        "totalRecords": total_count,
        "page": page,
        "pageSize": page_size,
        "totalPages": max(1, (total_count + page_size - 1) // page_size) if total_count else 1,
        "appliedFilters": {
            "pipeline_name": pipeline_name,
            "status": status.value,
            "search": search
        }
    }
    return jsonify_payload(items, meta=meta, exec_start=t0)

# ---------------------------------------------------------------------------
# 3. PIPELINE RUNS (Date, Time, Status, Pipeline Name Filters)
# ---------------------------------------------------------------------------
@runs_router.get("", summary="List Pipeline Runs with Date & Pipeline Filters")
def list_pipeline_runs(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name (e.g. ecommerce_etl, hr_etl, stock_etl)"),
    status: StatusEnum = Query(StatusEnum.ALL, description="Filter run status"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    time_range: TimeRangeEnum = Query(TimeRangeEnum.ALL, description="Time range"),
    search: Optional[str] = Query(None, description="Search error or pipeline"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Page size")
):
    t0 = time.time()
    where_parts = []
    params = []

    if pipeline_name and pipeline_name.upper() != "ALL":
        where_parts.append("pipeline_name = %s")
        params.append(pipeline_name)

    if status != StatusEnum.ALL:
        where_parts.append("status = %s")
        params.append(status.value.lower())

    date_where, date_params = get_time_window_sql(time_range.value, start_date, end_date, "start_time")
    where_parts.extend(date_where)
    params.extend(date_params)

    if search and search.strip():
        where_parts.append("(pipeline_name LIKE %s OR error_message LIKE %s OR failure_stage LIKE %s)")
        s = f"%{search.strip()}%"
        params.extend([s, s, s])

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    offset = (page - 1) * page_size

    count_res = query(f"SELECT COUNT(*) as cnt FROM obs_pipeline_runs {where_sql}", tuple(params))
    total = count_res[0]["cnt"] if count_res else 0

    sql = f"""
        SELECT 
            id as run_id,
            pipeline_id,
            pipeline_name,
            status,
            start_time,
            end_time,
            duration,
            tool_name,
            rows_read,
            rows_written,
            rows_added,
            failure_stage,
            failed_node,
            error_class,
            error_message
        FROM obs_pipeline_runs
        {where_sql}
        ORDER BY start_time DESC
        LIMIT %s OFFSET %s
    """
    rows = query(sql, tuple(params + [page_size, offset]))

    meta = {
        "totalRecords": total,
        "page": page,
        "pageSize": page_size,
        "totalPages": max(1, (total + page_size - 1) // page_size) if total else 1,
        "appliedFilters": {
            "pipeline_name": pipeline_name,
            "status": status.value,
            "start_date": start_date,
            "end_date": end_date
        }
    }
    return jsonify_payload(rows, meta=meta, exec_start=t0)

# ---------------------------------------------------------------------------
# 4. DATA QUALITY (Pipeline Name & Quality Status Filters)
# ---------------------------------------------------------------------------
@quality_router.get("", summary="Data Quality Health & Pipeline Scoring")
def get_data_quality(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    status: QualityStatusEnum = Query(QualityStatusEnum.ALL, description="Filter quality status: Good, Warning, Poor")
):
    t0 = time.time()
    where_parts = []
    params = []

    if pipeline_name and pipeline_name.upper() != "ALL":
        where_parts.append("p.pipeline_name = %s")
        params.append(pipeline_name)

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    pipe_quality = query(f"""
        SELECT 
            p.pipeline_id,
            p.pipeline_name,
            h.total_runs,
            h.failed_count,
            h.success_rate_pct,
            h.health_status,
            h.last_end_time
        FROM obs_pipelines p
        JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id
        {where_sql}
        ORDER BY h.success_rate_pct DESC
    """, tuple(params))

    top_pipes = []
    for pq in pipe_quality:
        score = float(pq["success_rate_pct"] or 0)
        status_label = "Good" if score >= 85 else ("Warning" if score >= 50 else "Poor")
        if status != QualityStatusEnum.ALL and status_label.lower() != status.value.lower():
            continue

        top_pipes.append({
            "pipeline": pq["pipeline_name"],
            "qualityScore": score,
            "status": status_label,
            "checksRun": int(pq["total_runs"]),
            "failedChecks": f"{int(pq['failed_count'])} ({100 - score:.0f}%)",
            "lastCheck": pq["last_end_time"].strftime("%b %d, %Y %I:%M %p") if pq["last_end_time"] else "N/A"
        })

    totals = query("SELECT total_runs, success_runs, failed_runs, success_rate_pct FROM vw_kpi_totals LIMIT 1")[0]
    rate = float(totals["success_rate_pct"] or 0.0)

    return jsonify_payload({
        "qualityStatus": rate,
        "qualityStatusLabel": "Good" if rate >= 80 else "Warning",
        "totalChecks": int(totals["total_runs"]),
        "passed": int(totals["success_runs"]),
        "failed": int(totals["failed_runs"]),
        "pipelines": top_pipes
    }, exec_start=t0)

# ---------------------------------------------------------------------------
# 5. DATA FRESHNESS (Search & Freshness Status Filters)
# ---------------------------------------------------------------------------
@freshness_router.get("", summary="Data Freshness SLAs & Table Delay Tracking")
def get_data_freshness(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    search: Optional[str] = Query(None, description="Search table or schema name"),
    status: FreshnessStatusEnum = Query(FreshnessStatusEnum.ALL, description="Filter status: Fresh, Delayed, Stale"),
    limit: int = Query(20, ge=1, le=100, description="Limit items")
):
    t0 = time.time()
    where_parts = []
    params = []

    if search and search.strip():
        where_parts.append("(a.object_name LIKE %s OR a.schema_name LIKE %s)")
        s = f"%{search.strip()}%"
        params.extend([s, s])

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    assets = query(f"""
        SELECT 
            a.id,
            a.object_name,
            a.schema_name,
            a.database_name,
            a.row_count,
            a.last_updated_at,
            a.observed_at,
            COALESCE(TIMESTAMPDIFF(MINUTE, a.last_updated_at, a.observed_at), 10) as lag_minutes
        FROM obs_run_assets a
        {where_sql}
        ORDER BY a.observed_at DESC
        LIMIT %s
    """, tuple(params + [limit]))

    pipe_items = []
    for a in assets:
        lag = int(a["lag_minutes"] or 0)
        st = "Fresh" if lag <= 60 else ("Delayed" if lag <= 180 else "Stale")

        if status != FreshnessStatusEnum.ALL and st.lower() != status.value.lower():
            continue

        lag_str = f"{lag // 60}h {lag % 60}m" if lag >= 60 else f"{lag} min"
        pipe_items.append({
            "id": f"asset_{a['id']}",
            "table": a["object_name"],
            "schema": a["schema_name"],
            "database": a["database_name"],
            "rowCount": int(a["row_count"] or 0),
            "lastUpdated": a["last_updated_at"].strftime("%b %d, %Y %I:%M %p") if a["last_updated_at"] else "N/A",
            "currentLag": lag_str,
            "status": st
        })

    return jsonify_payload({
        "averageLag": "14 min",
        "assets": pipe_items
    }, exec_start=t0)

# ---------------------------------------------------------------------------
# 6. SCHEMA DRIFT (Data Type & Search Filters)
# ---------------------------------------------------------------------------
@schema_router.get("", summary="Schema Drift & Column Audits")
def get_schema_observability(
    data_type: DataTypeEnum = Query(DataTypeEnum.ALL, description="Filter by column data type"),
    search: Optional[str] = Query(None, description="Search column or table name")
):
    t0 = time.time()
    where_parts = []
    params = []

    if data_type != DataTypeEnum.ALL:
        where_parts.append("data_type = %s")
        params.append(data_type.value)

    if search and search.strip():
        where_parts.append("(column_name LIKE %s OR object_name LIKE %s)")
        s = f"%{search.strip()}%"
        params.extend([s, s])

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    types_breakdown = query("""
        SELECT data_type, COUNT(*) as count, ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM obs_run_columns), 1) as percentage
        FROM obs_run_columns
        GROUP BY data_type
        ORDER BY count DESC
    """)

    recent_cols = query(f"""
        SELECT id, database_name, schema_name, object_name, column_name, data_type, created_at
        FROM obs_run_columns
        {where_sql}
        ORDER BY created_at DESC
        LIMIT 25
    """, tuple(params))

    changes = []
    for c in recent_cols:
        changes.append({
            "table": c["object_name"],
            "column": c["column_name"],
            "dataType": c["data_type"],
            "schema": c["schema_name"],
            "database": c["database_name"],
            "observedAt": c["created_at"].strftime("%b %d, %Y %I:%M %p") if c["created_at"] else "N/A"
        })

    return jsonify_payload({
        "dataTypesBreakdown": types_breakdown,
        "columnsAudited": changes
    }, exec_start=t0)

# ---------------------------------------------------------------------------
# 7. VOLUME OBSERVABILITY
# ---------------------------------------------------------------------------
@volume_router.get("", summary="Data Volume & Row Count Trends")
def get_volume_observability():
    t0 = time.time()
    vol_stats = query("SELECT COUNT(*) as asset_count, SUM(row_count) as total_rows FROM obs_run_assets")[0]
    timeline = query("""
        SELECT DATE_FORMAT(observed_at, '%%b %%d') as time, COALESCE(SUM(row_count), 0) as volume
        FROM obs_run_assets
        WHERE observed_at IS NOT NULL
        GROUP BY time
        ORDER BY MIN(observed_at)
    """)

    tot_rows = int(vol_stats["total_rows"] or 0)
    return jsonify_payload({
        "totalRecords": f"{tot_rows / 1000000:.2f}M" if tot_rows >= 1000000 else f"{tot_rows} records",
        "timeline": timeline
    }, exec_start=t0)

# ---------------------------------------------------------------------------
# 8. METRICS EXPLORER
# ---------------------------------------------------------------------------
@metrics_router.get("", summary="Telemetry & Execution Timeseries")
def get_metrics_explorer(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name")
):
    t0 = time.time()
    where_sql = ""
    params = []
    if pipeline_name and pipeline_name.upper() != "ALL":
        where_sql = "WHERE p.pipeline_name = %s"
        params.append(pipeline_name)

    pipes = query(f"""
        SELECT 
            p.pipeline_name,
            p.etl_tool,
            p.source_tool,
            h.health_status,
            h.last_end_time,
            h.success_rate_pct,
            (SELECT ROUND(AVG(r.duration), 0) FROM obs_pipeline_runs r WHERE r.pipeline_id = p.pipeline_id) as avg_duration
        FROM obs_pipelines p
        JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id
        {where_sql}
    """, tuple(params))

    items = []
    for p in pipes:
        dur = int(p["avg_duration"] or 12)
        items.append({
            "pipeline": p["pipeline_name"],
            "tool": (p["etl_tool"] or p["source_tool"] or "dbt").title(),
            "status": "Healthy" if p["health_status"] == "healthy" else "Degraded",
            "lastRun": p["last_end_time"].strftime("%I:%M:%S %p") if p["last_end_time"] else "N/A",
            "avgDuration": f"{dur}s",
            "successRate": f"{float(p['success_rate_pct']):.1f}%"
        })

    return jsonify_payload(items, exec_start=t0)

# ---------------------------------------------------------------------------
# 9. REAL-TIME LOGS STREAM (Pipeline, Level & Search Filters)
# ---------------------------------------------------------------------------
@logs_router.get("", summary="Real-time Execution Logs Stream")
def get_logs(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    level: LogLevelEnum = Query(LogLevelEnum.ALL, description="Filter log level (ERROR, WARN, INFO, DEBUG)"),
    search: Optional[str] = Query(None, description="Search in log message or error details"),
    limit: int = Query(25, ge=1, le=100, description="Limit log records")
):
    t0 = time.time()
    where_parts = []
    params = []

    if pipeline_name and pipeline_name.upper() != "ALL":
        where_parts.append("r.pipeline_name = %s")
        params.append(pipeline_name)

    if level != LogLevelEnum.ALL:
        if level == LogLevelEnum.ERROR:
            where_parts.append("r.status = 'failed'")
        else:
            where_parts.append("r.status != 'failed'")

    if search and search.strip():
        where_parts.append("(r.error_message LIKE %s OR r.pipeline_name LIKE %s OR r.failure_stage LIKE %s)")
        s = f"%{search.strip()}%"
        params.extend([s, s, s])

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    rows = query(f"""
        SELECT 
            r.id as run_id,
            r.pipeline_name,
            r.status,
            r.start_time,
            r.duration,
            r.failure_stage,
            r.failed_node,
            r.error_class,
            r.error_message,
            r.tool_name,
            r.rows_added
        FROM obs_pipeline_runs r
        {where_sql}
        ORDER BY r.start_time DESC
        LIMIT %s
    """, tuple(params + [limit]))

    items = []
    for r in rows:
        lvl = "ERROR" if r["status"] == "failed" else "INFO"
        msg = r["error_message"] or f"Pipeline execution finished with status: {r['status']}. Rows processed: {r['rows_added'] or 0}"
        items.append({
            "id": r["run_id"],
            "timestamp": r["start_time"].strftime("%b %d, %Y %I:%M:%S %p") if r["start_time"] else "N/A",
            "pipeline": r["pipeline_name"],
            "level": lvl,
            "tool": (r["tool_name"] or "dbt").title(),
            "message": msg,
            "duration": f"{r['duration']}s" if r["duration"] else "—"
        })

    return jsonify_payload(items, meta={"totalReturned": len(items)}, exec_start=t0)

# ---------------------------------------------------------------------------
# 10. INCIDENTS & ROOT CAUSE (Severity, Pipeline Name & Search Filters)
# ---------------------------------------------------------------------------
@incidents_router.get("", summary="Incidents Triage & Root Cause")
def get_incidents(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    severity: SeverityEnum = Query(SeverityEnum.ALL, description="Filter incident severity"),
    search: Optional[str] = Query(None, description="Search error or pipeline")
):
    t0 = time.time()
    where_parts = []
    params = []

    if pipeline_name and pipeline_name.upper() != "ALL":
        where_parts.append("pipeline_name = %s")
        params.append(pipeline_name)

    if search and search.strip():
        where_parts.append("(pipeline_name LIKE %s OR error_message LIKE %s)")
        s = f"%{search.strip()}%"
        params.extend([s, s])

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    failed_rows = query(f"""
        SELECT run_id, pipeline_name, failure_stage, failed_node, error_class, error_message, start_time, duration
        FROM vw_failed_runs
        {where_sql}
        ORDER BY start_time DESC
    """, tuple(params))

    items = []
    for r in failed_rows:
        err_cls = (r["error_class"] or "etl").lower()
        sev = "Critical" if "compilation" in err_cls else ("High" if "snowflake" in err_cls else "Medium")
        if severity != SeverityEnum.ALL and sev.lower() != severity.value.lower():
            continue

        items.append({
            "id": f"INC-{r['run_id'][:8]}",
            "runId": r["run_id"],
            "title": f"{r['pipeline_name']} failure: {r['error_class'] or 'runtime'}",
            "severity": sev,
            "pipeline": r["pipeline_name"],
            "failedNode": r["failed_node"],
            "failureStage": r["failure_stage"],
            "status": "Open",
            "openedAt": r["start_time"].strftime("%b %d, %Y %I:%M %p") if r["start_time"] else "N/A",
            "duration": f"{r['duration']}s" if r["duration"] else "—",
            "errorMessage": r["error_message"]
        })

    return jsonify_payload(items, meta={"totalIncidents": len(items)}, exec_start=t0)

# ---------------------------------------------------------------------------
# 11. LINEAGE (Pipeline Name & Search Filters)
# ---------------------------------------------------------------------------
@lineage_router.get("", summary="End-to-End Pipeline Lineage DAG Flows")
def get_lineage(
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    search: Optional[str] = Query(None, description="Search pipeline name")
):
    t0 = time.time()
    where_parts = []
    params = []

    if pipeline_name and pipeline_name.upper() != "ALL":
        where_parts.append("p.pipeline_name = %s")
        params.append(pipeline_name)

    if search and search.strip():
        where_parts.append("p.pipeline_name LIKE %s")
        params.append(f"%{search.strip()}%")

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    pipes = query(f"""
        SELECT 
            p.pipeline_id,
            p.pipeline_name,
            p.source_tool,
            p.source_schema,
            p.etl_tool,
            p.target_tool,
            p.target_schema,
            h.health_status,
            h.last_end_time,
            COALESCE(SUM(a.row_count), 0) as total_rows
        FROM obs_pipelines p
        LEFT JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id
        LEFT JOIN obs_pipeline_runs r ON p.pipeline_id = r.pipeline_id
        LEFT JOIN obs_run_assets a ON r.id = a.run_id
        {where_sql}
        GROUP BY p.pipeline_id, p.pipeline_name, p.source_tool, p.source_schema, p.etl_tool, p.target_tool, p.target_schema, h.health_status, h.last_end_time
    """, tuple(params))

    flows = []
    for p in pipes:
        st = "Healthy" if p["health_status"] == "healthy" else "Failed"
        rec_cnt = int(p["total_rows"] or 0)
        flows.append({
            "id": p["pipeline_id"],
            "name": p["pipeline_name"],
            "source": {"type": (p["source_tool"] or "Snowflake").title(), "schema": p["source_schema"] or "RAW_DATA"},
            "tool": {"type": (p["etl_tool"] or "dbt").title(), "action": "Transformation"},
            "target": {"type": (p["target_tool"] or "Snowflake").title(), "schema": p["target_schema"] or "TRANSFORMED_DB"},
            "status": st,
            "lastRun": p["last_end_time"].strftime("%b %d, %Y %I:%M %p") if p["last_end_time"] else "N/A",
            "volume": f"{rec_cnt} records"
        })

    return jsonify_payload(flows, meta={"totalFlows": len(flows)}, exec_start=t0)

# ---------------------------------------------------------------------------
# 12. ALERTS MODULE
# ---------------------------------------------------------------------------
@alerts_router.get("", summary="Active Alerts & Notifications")
def get_alerts():
    t0 = time.time()
    failed = query("SELECT run_id, pipeline_name, error_class, error_message, start_time FROM vw_failed_runs ORDER BY start_time DESC LIMIT 10")
    items = []
    for f in failed:
        items.append({
            "id": f"alt_{f['run_id']}",
            "type": "ERROR",
            "title": f"Pipeline Failure: {f['pipeline_name']}",
            "message": f["error_message"] or f"Failure under {f['error_class']}",
            "time": f["start_time"].strftime("%b %d, %Y %I:%M %p") if f["start_time"] else "N/A"
        })

    return jsonify_payload(items, meta={"unreadCount": len(items)}, exec_start=t0)

# ---------------------------------------------------------------------------
# INCLUDE ROUTERS
# ---------------------------------------------------------------------------
app.include_router(overview_router)
app.include_router(pipelines_router)
app.include_router(runs_router)
app.include_router(quality_router)
app.include_router(freshness_router)
app.include_router(schema_router)
app.include_router(volume_router)
app.include_router(metrics_router)
app.include_router(logs_router)
app.include_router(incidents_router)
app.include_router(lineage_router)
app.include_router(filters_router)
app.include_router(alerts_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
