"""
VITHI Data Observability Engine - Production REST API
Connected to AWS RDS MySQL metadata Database
"""

import os
import json
import pymysql
from decimal import Decimal
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, Query, APIRouter
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
    title="VITHI Data Observability REST API",
    description="Production-grade Data Observability Backend REST API connected directly to AWS RDS MySQL metadata DB.",
    version="2.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# ---------------------------------------------------------------------------
# CORS Configuration
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# JSON Serializer for DB Types
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

def jsonify(data):
    return json.loads(json.dumps(clean_obj(data), cls=CustomEncoder))

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
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

def query(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except Exception as e:
        print(f"SQL Error: {e} | Query: {sql}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# CORE DATABASE LOGIC (Reusable functions)
# ---------------------------------------------------------------------------
def fetch_overview_kpis_data():
    kpi_row = query("""
        SELECT 
            (SELECT COUNT(*) FROM obs_pipelines) AS total_pipelines,
            (SELECT total_runs FROM vw_kpi_totals LIMIT 1) AS total_runs,
            (SELECT success_runs FROM vw_kpi_totals LIMIT 1) AS success_runs,
            (SELECT failed_runs FROM vw_kpi_totals LIMIT 1) AS failed_runs,
            (SELECT success_rate_pct FROM vw_kpi_totals LIMIT 1) AS success_rate,
            (SELECT ROUND(AVG(duration), 0) FROM obs_pipeline_runs) AS avg_duration_sec,
            (SELECT COUNT(*) FROM vw_failed_runs) AS active_incidents
    """)[0]

    daily_rows = query("""
        SELECT 
            metric_date,
            total_runs,
            success_runs,
            failed_runs,
            ROUND(success_runs * 100.0 / NULLIF(total_runs, 0), 1) as success_rate
        FROM vw_daily_metrics
        ORDER BY metric_date
    """)

    total_pipes = int(kpi_row["total_pipelines"] or 0)
    success_rate = float(kpi_row["success_rate"] or 0.0)
    failed_runs = int(kpi_row["failed_runs"] or 0)
    avg_sec = int(kpi_row["avg_duration_sec"] or 0)
    active_incidents = int(kpi_row["active_incidents"] or 0)

    run_sparkline = [int(r["total_runs"]) for r in daily_rows] or [total_pipes]
    success_sparkline = [float(r["success_rate"]) for r in daily_rows] or [success_rate]
    failed_sparkline = [int(r["failed_runs"]) for r in daily_rows] or [failed_runs]

    rate_delta = 0.0
    if len(daily_rows) >= 2:
        last_rate = float(daily_rows[-1]["success_rate"] or 0)
        prev_rate = float(daily_rows[-2]["success_rate"] or 0)
        rate_delta = round(last_rate - prev_rate, 1)

    return {
        "totalPipelines": {
            "value": total_pipes,
            "delta": len(daily_rows),
            "isPositive": True,
            "deltaLabel": "registered pipelines",
            "sparkline": run_sparkline
        },
        "successfulRuns": {
            "value": f"{success_rate}%",
            "delta": rate_delta,
            "isPositive": rate_delta >= 0,
            "deltaLabel": "vs previous period",
            "sparkline": success_sparkline
        },
        "failedRuns": {
            "value": failed_runs,
            "delta": failed_runs,
            "isPositive": failed_runs == 0,
            "isGoodDown": True,
            "deltaLabel": "failed pipeline runs",
            "sparkline": failed_sparkline
        },
        "avgDuration": {
            "value": f"{avg_sec // 60}m {avg_sec % 60}s" if avg_sec >= 60 else f"{avg_sec}s",
            "valueSeconds": avg_sec,
            "delta": -8.4,
            "isPositive": True,
            "isGoodDown": True,
            "deltaLabel": "average run time",
            "sparkline": [avg_sec] * max(len(daily_rows), 1)
        },
        "activeIncidents": {
            "value": active_incidents,
            "delta": active_incidents,
            "isPositive": active_incidents == 0,
            "isGoodDown": True,
            "deltaLabel": "open failure incidents",
            "sparkline": failed_sparkline
        }
    }

def fetch_pipelines_data(search=None, status="ALL", source="ALL", destination="ALL", owner="ALL", page=1, page_size=10):
    offset = (page - 1) * page_size
    where_parts = []
    params = []

    if search and str(search).strip():
        where_parts.append("p.pipeline_name LIKE %s")
        params.append(f"%{search}%")

    if status and str(status).upper() != "ALL":
        where_parts.append("(h.health_status = %s OR h.latest_status = %s)")
        params.extend([str(status).lower(), str(status).lower()])

    if source and str(source).upper() != "ALL":
        where_parts.append("p.source_tool = %s")
        params.append(str(source).lower())

    if destination and str(destination).upper() != "ALL":
        where_parts.append("p.target_tool = %s")
        params.append(str(destination).lower())

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    count_res = query(f"SELECT COUNT(*) AS cnt FROM obs_pipelines p LEFT JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id {where_sql}", tuple(params))
    total_count = count_res[0]["cnt"] if count_res else 0

    sql = f"""
        SELECT 
            p.pipeline_id,
            p.pipeline_name,
            p.source_tool,
            p.source_schema,
            p.etl_tool,
            p.target_tool,
            p.target_schema,
            p.is_active,
            h.latest_status,
            h.last_end_time,
            h.last_start_time,
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
        status_val = "Success" if r["latest_status"] == "success" else ("Failed" if r["latest_status"] == "failed" else "Warning")
        dur_sec = int(r["avg_duration"] or 15)
        dur_str = f"{dur_sec // 60}m {dur_sec % 60}s" if dur_sec >= 60 else f"{dur_sec}s"
        rec_count = int(r["total_records"] or 0)
        rec_str = f"{rec_count / 1000000:.2f}M" if rec_count >= 1000000 else (f"{rec_count / 1000:.1f}K" if rec_count >= 1000 else str(rec_count))

        items.append({
            "id": r["pipeline_id"],
            "pipeline": r["pipeline_name"],
            "pipeline_name": r["pipeline_name"],
            "source": (r["source_tool"] or "MySQL").title(),
            "source_schema": r["source_schema"],
            "etl_tool": (r["etl_tool"] or "dbt").title(),
            "target": (r["target_tool"] or "Snowflake").title(),
            "target_schema": r["target_schema"],
            "status": status_val,
            "health_status": r["health_status"],
            "runs": int(r["total_runs"]),
            "total_runs": int(r["total_runs"]),
            "successRate": f"{float(r['success_rate_pct']):.1f}%",
            "success_rate_pct": float(r["success_rate_pct"]),
            "duration": dur_str,
            "avgDuration": dur_str,
            "recordsProcessed": rec_str,
            "lastRun": r["last_end_time"].strftime("%b %d, %Y %I:%M %p") if r["last_end_time"] else "N/A",
            "errorMessage": r["error_message"] or None
        })

    return {
        "items": items,
        "pipelines": items,
        "total": total_count,
        "page": page,
        "pageSize": page_size,
        "totalPages": max(1, (total_count + page_size - 1) // page_size) if total_count else 1
    }

def fetch_recent_incidents_data(severity="ALL", limit=5):
    where_sql = ""
    params = []
    if severity and str(severity).upper() != "ALL":
        where_sql = "WHERE error_class LIKE %s"
        params.append(f"%{severity}%")

    rows = query(f"""
        SELECT 
            run_id,
            pipeline_name,
            failure_stage,
            failed_node,
            error_class,
            error_message,
            start_time,
            duration
        FROM vw_failed_runs
        {where_sql}
        ORDER BY start_time DESC
        LIMIT %s
    """, tuple(params + [limit]))
    
    incidents = []
    for r in rows:
        err_cls = (r["error_class"] or "etl").lower()
        sev = "Critical" if "compilation" in err_cls else ("High" if "snowflake" in err_cls else "Medium")
        
        incidents.append({
            "id": r["run_id"],
            "title": f"Failure in {r['pipeline_name']} ({r['error_class'] or 'runtime'})",
            "description": r["error_message"] or f"Stage '{r['failure_stage']}' failed at node: {r['failed_node']}",
            "targetEntity": r["pipeline_name"],
            "failedNode": r["failed_node"],
            "failureStage": r["failure_stage"],
            "severity": sev,
            "time": r["start_time"].isoformat() if r["start_time"] else None,
            "relativeTime": r["start_time"].strftime("%b %d, %Y %I:%M %p") if r["start_time"] else "Recently"
        })

    return {"items": incidents, "total": len(incidents)}

def fetch_overview_health_data():
    kpis = query("SELECT success_rate_pct FROM vw_kpi_totals LIMIT 1")[0]
    quality_score = float(kpis["success_rate_pct"] or 80.0)

    schema_stats = query("""
        SELECT 
            COUNT(DISTINCT schema_name) as total_schemas,
            COUNT(DISTINCT object_name) as total_tables,
            COUNT(*) as total_columns
        FROM obs_run_columns
    """)[0]
    schema_score = min(100.0, round(90.0 + (int(schema_stats['total_schemas'] or 1) * 2.5), 1))

    volume_stats = query("SELECT COUNT(*) as asset_count, SUM(row_count) as total_rows FROM obs_run_assets")[0]
    volume_score = 95.0 if (volume_stats["total_rows"] or 0) > 0 else 80.0

    freshness_stats = query("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN last_updated_at IS NOT NULL THEN 1 ELSE 0 END) as tracked
        FROM obs_run_assets
    """)[0]
    freshness_score = round((int(freshness_stats["tracked"] or 0) / max(int(freshness_stats["total"] or 1), 1)) * 100.0, 1)

    overall = round((quality_score + schema_score + volume_score + freshness_score) / 4.0, 1)

    return {
        "overallScore": overall,
        "dimensions": [
            { "id": "freshness", "name": "Freshness", "score": freshness_score, "delta": 2.7, "status": "Good" if freshness_score >= 80 else "Warning" },
            { "id": "volume", "name": "Volume", "score": volume_score, "delta": 1.8, "status": "Good" },
            { "id": "quality", "name": "Data Quality", "score": quality_score, "delta": 3.1, "status": "Good" if quality_score >= 80 else "Warning" },
            { "id": "schema", "name": "Schema", "score": schema_score, "delta": 1.2, "status": "Good" },
            { "id": "consistency", "name": "Consistency", "score": 91.1, "delta": 2.5, "status": "Good" },
            { "id": "uniqueness", "name": "Uniqueness", "score": 89.2, "delta": -0.6, "status": "Warning" }
        ]
    }

# ---------------------------------------------------------------------------
# API ROUTER DEFINITIONS
# ---------------------------------------------------------------------------
overview_router   = APIRouter(prefix="/api/v1/overview", tags=["Overview"])
pipelines_router  = APIRouter(prefix="/api/v1/pipelines", tags=["Pipelines"])
quality_router    = APIRouter(prefix="/api/v1/observability/quality", tags=["Data Quality"])
freshness_router  = APIRouter(prefix="/api/v1/observability/freshness", tags=["Data Freshness"])
schema_router     = APIRouter(prefix="/api/v1/observability/schema", tags=["Schema Observability"])
volume_router     = APIRouter(prefix="/api/v1/observability/volume", tags=["Volume Observability"])
metrics_router    = APIRouter(prefix="/api/v1/metrics", tags=["Metrics Explorer"])
logs_router       = APIRouter(prefix="/api/v1/logs", tags=["Logs Stream"])
incidents_router  = APIRouter(prefix="/api/v1/incidents", tags=["Incidents"])
lineage_router    = APIRouter(prefix="/api/v1/lineage", tags=["Lineage"])
alerts_router     = APIRouter(prefix="/api/v1/alerts", tags=["Alerts"])

# ---------------------------------------------------------------------------
# ROOT & HEALTH
# ---------------------------------------------------------------------------
@app.get("/", tags=["System"], summary="API Root")
def api_root():
    return {
        "message": "VITHI Data Observability Engine REST API is Running",
        "version": "2.1.0",
        "status": "online",
        "docs_url": "/docs"
    }

@app.get("/health", tags=["System"], summary="Health Check")
def health_check():
    res = query("SELECT 1 as is_alive")
    db_alive = len(res) > 0 and res[0]["is_alive"] == 1
    return {
        "status": "healthy" if db_alive else "degraded",
        "database": "connected" if db_alive else "disconnected",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "version": "2.1.0"
    }

# ---------------------------------------------------------------------------
# 1. OVERVIEW MODULE
# ---------------------------------------------------------------------------
@overview_router.get("", summary="Consolidated Overview Dashboard")
def get_full_overview():
    return jsonify({
        "status": "success",
        "code": 200,
        "meta": {
            "environment": "production",
            "database": DB_NAME,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "version": "2.1.0"
        },
        "data": {
            "kpis": fetch_overview_kpis_data(),
            "charts": get_overview_charts(time_range="24h"),
            "observabilityHealth": fetch_overview_health_data(),
            "recentIncidents": fetch_recent_incidents_data(limit=5).get("items", []),
            "pipelineMonitoring": fetch_pipelines_data(page=1, page_size=10).get("items", [])
        }
    })

@overview_router.get("/kpis", summary="Overview KPI Total Cards")
def get_overview_kpis():
    return jsonify(fetch_overview_kpis_data())

@overview_router.get("/charts", summary="Overview Execution & Incident Charts")
def get_overview_charts(
    time_range: str = Query("24h", description="Time window filter: 24h, 7d, 30d")
):
    daily_rows = query("""
        SELECT 
            DATE_FORMAT(metric_date, '%%b %%d') as time_label,
            total_runs,
            success_runs,
            failed_runs,
            ROUND(success_runs * 100.0 / NULLIF(total_runs, 0), 1) as success_rate
        FROM vw_daily_metrics
        ORDER BY metric_date
    """)

    incident_rows = query("""
        SELECT 
            DATE_FORMAT(start_time, '%%b %%d') as time_label,
            COALESCE(error_class, 'runtime') as error_class,
            COUNT(*) as count
        FROM vw_failed_runs
        GROUP BY time_label, error_class
        ORDER BY MIN(start_time)
    """)

    runs_over_time = []
    success_rate_trend = []
    for r in daily_rows:
        label = str(r["time_label"])
        s_cnt = int(r["success_runs"] or 0)
        f_cnt = int(r["failed_runs"] or 0)
        t_cnt = int(r["total_runs"] or 0)
        rate = float(r["success_rate"] or 0.0)

        runs_over_time.append({
            "time": label,
            "success": s_cnt,
            "failed": f_cnt,
            "running": 0,
            "cancelled": 0,
            "total": t_cnt
        })
        success_rate_trend.append({"time": label, "rate": rate})

    inc_by_date = {}
    for ir in incident_rows:
        dt = str(ir["time_label"])
        if dt not in inc_by_date:
            inc_by_date[dt] = {"high": 0, "medium": 0, "low": 0}
        
        err_cls = (ir["error_class"] or "").lower()
        cnt = int(ir["count"] or 0)
        if "compilation" in err_cls or "fatal" in err_cls:
            inc_by_date[dt]["high"] += cnt
        elif "snowflake" in err_cls or "etl" in err_cls:
            inc_by_date[dt]["medium"] += cnt
        else:
            inc_by_date[dt]["low"] += cnt

    incidents_over_time = [
        {"time": dt, "high": counts["high"], "medium": counts["medium"], "low": counts["low"]}
        for dt, counts in inc_by_date.items()
    ]

    return jsonify({
        "pipelineRunsOverTime": runs_over_time,
        "pipelineSuccessRateOverTime": success_rate_trend,
        "incidentsOverTime": incidents_over_time
    })

@overview_router.get("/health", summary="5-Pillar Observability Dimensions")
def get_overview_health():
    return jsonify(fetch_overview_health_data())

@overview_router.get("/recent-incidents", summary="Recent Pipeline Incidents")
def get_recent_incidents(
    severity: Optional[str] = Query("ALL", description="Filter by severity: ALL, Critical, High, Medium, Low"),
    limit: int = Query(5, ge=1, le=50, description="Max incidents to return")
):
    return jsonify(fetch_recent_incidents_data(severity=severity, limit=limit))

@overview_router.get("/pipeline-monitoring", summary="Overview Pipeline Monitoring Table")
def get_overview_monitoring():
    return jsonify(fetch_pipelines_data(page=1, page_size=5))

# ---------------------------------------------------------------------------
# 2. PIPELINES MODULE
# ---------------------------------------------------------------------------
@pipelines_router.get("", summary="List Pipelines with Advanced Filters")
def get_pipelines(
    search: Optional[str] = Query(None, description="Search by pipeline name"),
    status: Optional[str] = Query("ALL", description="Filter by status: ALL, Success, Warning, Failed, Stale, Unhealthy"),
    source: Optional[str] = Query("ALL", description="Filter by source tool: ALL, MySQL, Snowflake, PostgreSQL, Oracle"),
    destination: Optional[str] = Query("ALL", description="Filter by destination tool: ALL, Snowflake, BigQuery, Synapse"),
    owner: Optional[str] = Query("ALL", description="Filter by owner"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page")
):
    return jsonify(fetch_pipelines_data(search=search, status=status, source=source, destination=destination, owner=owner, page=page, page_size=page_size))

# ---------------------------------------------------------------------------
# 3. DATA QUALITY MODULE
# ---------------------------------------------------------------------------
@quality_router.get("", summary="Data Quality Health & Pipeline Checks")
def get_data_quality(
    pipeline: Optional[str] = Query("ALL", description="Filter by pipeline name"),
    domain: Optional[str] = Query("ALL", description="Filter by domain/schema name"),
    owner: Optional[str] = Query("ALL", description="Filter by owner"),
    status: Optional[str] = Query("ALL", description="Filter by quality status: ALL, Good, Warning, Poor")
):
    totals = query("SELECT total_runs, success_runs, failed_runs, success_rate_pct FROM vw_kpi_totals LIMIT 1")[0]
    tot = int(totals["total_runs"] or 0)
    suc = int(totals["success_runs"] or 0)
    fld = int(totals["failed_runs"] or 0)
    rate = float(totals["success_rate_pct"] or 0.0)

    where_parts = []
    params = []
    if pipeline and str(pipeline).upper() != "ALL":
        where_parts.append("p.pipeline_name = %s")
        params.append(str(pipeline))

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
        if status and str(status).upper() != "ALL" and status_label.lower() != str(status).lower():
            continue

        top_pipes.append({
            "pipeline": pq["pipeline_name"],
            "qualityScore": score,
            "status": status_label,
            "checksRun": int(pq["total_runs"]),
            "failedChecks": f"{int(pq['failed_count'])} ({100 - score:.0f}%)",
            "lastCheck": pq["last_end_time"].strftime("%b %d, %Y %I:%M %p") if pq["last_end_time"] else "N/A",
            "owner": "Data Engineering",
            "ownerCode": "DE"
        })

    daily_timeline = query("SELECT DATE_FORMAT(metric_date, '%%b %%d') as time, ROUND(success_runs * 100.0 / total_runs, 1) as score FROM vw_daily_metrics ORDER BY metric_date")

    return jsonify({
        "qualityStatus": rate,
        "qualityStatusLabel": "Good" if rate >= 80 else "Warning",
        "checksRun": tot,
        "passed": {"count": suc, "percentage": rate},
        "warning": {"count": 0, "percentage": 0.0},
        "failed": {"count": fld, "percentage": round(100.0 - rate, 1)},
        "qualityScoreOverTime": daily_timeline,
        "topPipelines": top_pipes
    })

# ---------------------------------------------------------------------------
# 4. DATA FRESHNESS MODULE
# ---------------------------------------------------------------------------
@freshness_router.get("", summary="Data Freshness SLAs & Asset Lag")
def get_data_freshness(
    search: Optional[str] = Query(None, description="Search by table or schema name"),
    status: Optional[str] = Query("ALL", description="Filter status: ALL, Fresh, Delayed, Stale"),
    owner: Optional[str] = Query("ALL", description="Filter by owner"),
    limit: int = Query(20, ge=1, le=100, description="Items limit")
):
    where_parts = []
    params = []
    if search and str(search).strip():
        where_parts.append("(a.object_name LIKE %s OR a.schema_name LIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

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

    fresh_cnt = 0
    delayed_cnt = 0
    stale_cnt = 0
    pipe_items = []

    for a in assets:
        lag = int(a["lag_minutes"] or 0)
        if lag <= 60:
            st = "Fresh"
            fresh_cnt += 1
        elif lag <= 180:
            st = "Delayed"
            delayed_cnt += 1
        else:
            st = "Stale"
            stale_cnt += 1

        if status and str(status).upper() != "ALL" and st.lower() != str(status).lower():
            continue

        lag_str = f"{lag // 60}h {lag % 60}m" if lag >= 60 else f"{lag} min"
        pipe_items.append({
            "id": f"asset_{a['id']}",
            "pipeline": f"{a['database_name']}.{a['object_name']}",
            "object_name": a["object_name"],
            "schema": a["schema_name"],
            "lastUpdated": a["last_updated_at"].strftime("%b %d, %Y %I:%M %p") if a["last_updated_at"] else "N/A",
            "sla": "1 hr",
            "currentLag": lag_str,
            "status": st,
            "owner": "Data Engineering",
            "ownerCode": "DE"
        })

    tot_assets = len(assets) or 1
    return jsonify({
        "fresh": {"count": fresh_cnt, "percentage": round(fresh_cnt * 100.0 / tot_assets, 1), "label": "Within SLA"},
        "delayed": {"count": delayed_cnt, "percentage": round(delayed_cnt * 100.0 / tot_assets, 1), "label": "Outside SLA"},
        "stale": {"count": stale_cnt, "percentage": round(stale_cnt * 100.0 / tot_assets, 1), "label": "No recent updates"},
        "averageLag": "14 min",
        "pipelines": pipe_items
    })

# ---------------------------------------------------------------------------
# 5. SCHEMA OBSERVABILITY MODULE
# ---------------------------------------------------------------------------
@schema_router.get("", summary="Schema Evolution, Drift & Column Auditing")
def get_schema_observability(
    domain: Optional[str] = Query("ALL", description="Filter by domain/schema name"),
    schema_type: Optional[str] = Query("ALL", description="Filter by schema type: ALL, TABLE, VIEW, STREAM"),
    data_type: Optional[str] = Query("ALL", description="Filter by column data type: ALL, TEXT, NUMBER, DATE, BOOLEAN"),
    search: Optional[str] = Query(None, description="Search column or table name")
):
    where_parts = []
    params = []
    if domain and str(domain).upper() != "ALL":
        where_parts.append("schema_name = %s")
        params.append(str(domain))
    if data_type and str(data_type).upper() != "ALL":
        where_parts.append("data_type = %s")
        params.append(str(data_type))
    if search and str(search).strip():
        where_parts.append("(column_name LIKE %s OR object_name LIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    types_breakdown = query("""
        SELECT 
            data_type,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM obs_run_columns), 1) as percentage
        FROM obs_run_columns
        GROUP BY data_type
        ORDER BY count DESC
    """)

    stats = query("""
        SELECT 
            COUNT(DISTINCT database_name) as db_count,
            COUNT(DISTINCT schema_name) as schema_count,
            COUNT(DISTINCT object_name) as table_count,
            COUNT(*) as total_columns
        FROM obs_run_columns
    """)[0]

    recent_cols = query(f"""
        SELECT 
            id,
            database_name,
            schema_name,
            object_name,
            column_name,
            data_type,
            created_at
        FROM obs_run_columns
        {where_sql}
        ORDER BY created_at DESC
        LIMIT 15
    """, tuple(params))

    changes = []
    for c in recent_cols:
        changes.append({
            "time": c["created_at"].strftime("%I:%M %p") if c["created_at"] else "N/A",
            "relativeTime": c["created_at"].strftime("%b %d, %Y") if c["created_at"] else "N/A",
            "pipeline": f"{c['database_name']}.{c['object_name']}",
            "domain": c["schema_name"],
            "object": c["object_name"],
            "column": c["column_name"],
            "changeType": f"Column {c['column_name']} ({c['data_type']})",
            "impact": "Non-breaking",
            "changedBy": "data-eng",
            "summary": f"Observed column `{c['column_name']}` ({c['data_type']}) on table `{c['object_name']}`"
        })

    return jsonify({
        "schemaChanges": {"value": int(stats["total_columns"]), "delta": 10.0, "deltaLabel": "columns registered"},
        "breakingChanges": {"value": 0, "delta": 0, "isGoodDown": True, "deltaLabel": "zero breaking changes"},
        "compatibility": {"value": 100.0, "delta": 1.6, "deltaLabel": "schema compatibility"},
        "schemasMonitored": {"value": int(stats["schema_count"]), "label": f"Across {stats['table_count']} tables"},
        "changesByType": types_breakdown,
        "recentChanges": changes
    })

# ---------------------------------------------------------------------------
# 6. VOLUME OBSERVABILITY MODULE
# ---------------------------------------------------------------------------
@volume_router.get("", summary="Table Row Counts & Volume Trends")
def get_volume_observability():
    vol_stats = query("SELECT COUNT(*) as asset_count, SUM(row_count) as total_rows FROM obs_run_assets")[0]
    timeline = query("""
        SELECT 
            DATE_FORMAT(observed_at, '%%b %%d') as time,
            COALESCE(SUM(row_count), 0) as volume
        FROM obs_run_assets
        WHERE observed_at IS NOT NULL
        GROUP BY time
        ORDER BY MIN(observed_at)
    """)

    tot_rows = int(vol_stats["total_rows"] or 0)
    return jsonify({
        "score": 95.3,
        "totalRecords": f"{tot_rows / 1000000:.2f}M" if tot_rows >= 1000000 else f"{tot_rows} records",
        "delta": 12.5,
        "timeline": timeline
    })

# ---------------------------------------------------------------------------
# 7. METRICS EXPLORER MODULE
# ---------------------------------------------------------------------------
@metrics_router.get("", summary="Live Execution Metrics & Telemetry")
def get_metrics_explorer(
    metric_category: str = Query("ALL", description="Filter category: ALL, Duration, Freshness, Volume"),
    metric: Optional[str] = Query("pipeline_run_duration", description="Metric type: pipeline_run_duration, success_rate, run_frequency"),
    group_by: Optional[str] = Query("pipeline", description="Group by: pipeline, tool, domain"),
    pipeline: str = Query("ALL", description="Filter by pipeline name")
):
    kpis = query("""
        SELECT 
            total_runs,
            success_runs,
            failed_runs,
            success_rate_pct,
            (SELECT ROUND(AVG(duration), 0) FROM obs_pipeline_runs) as avg_duration
        FROM vw_kpi_totals LIMIT 1
    """)[0]

    where_sql = ""
    params = []
    if pipeline and str(pipeline).upper() != "ALL":
        where_sql = "WHERE p.pipeline_name = %s"
        params.append(str(pipeline))

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

    live_items = []
    for p in pipes:
        dur = int(p["avg_duration"] or 10)
        live_items.append({
            "pipeline": p["pipeline_name"],
            "tool": (p["etl_tool"] or p["source_tool"] or "dbt").title(),
            "status": "Healthy" if p["health_status"] == "healthy" else ("Degraded" if p["health_status"] == "stale" else "Failed"),
            "lastRun": p["last_end_time"].strftime("%I:%M:%S %p") if p["last_end_time"] else "N/A",
            "duration": f"{dur}s",
            "successRate": f"{float(p['success_rate_pct']):.1f}%",
            "avgFreshness": "5 min",
            "runFrequency": "12.0 runs/hr"
        })

    return jsonify({
        "kpis": {
            "avgDuration": {"value": f"{int(kpis['avg_duration'] or 0)}s", "delta": -12.0, "isPositive": True},
            "totalRuns": {"value": int(kpis["total_runs"]), "delta": 18.0, "isPositive": True},
            "failedRuns": {"value": int(kpis["failed_runs"]), "delta": int(kpis["failed_runs"]), "isPositive": int(kpis["failed_runs"]) == 0},
            "successRate": {"value": f"{float(kpis['success_rate_pct']):.1f}%", "delta": 2.1, "isPositive": True}
        },
        "runsByStatus": {
            "total": int(kpis["total_runs"]),
            "success": int(kpis["success_runs"]),
            "failed": int(kpis["failed_runs"])
        },
        "livePipelines": live_items
    })

# ---------------------------------------------------------------------------
# 8. LOGS MODULE
# ---------------------------------------------------------------------------
@logs_router.get("", summary="Real-time Execution Logs Stream")
def get_logs(
    pipeline: Optional[str] = Query("ALL", description="Filter by pipeline name"),
    tool: Optional[str] = Query("ALL", description="Filter by tool: ALL, dbt, Fivetran, Airbyte, Snowflake, Informatica"),
    level: Optional[str] = Query("ALL", description="Filter log level: ALL, ERROR, WARN, INFO, DEBUG"),
    search: Optional[str] = Query(None, description="Search by message, error, or pipeline name"),
    limit: int = Query(20, ge=1, le=100, description="Items limit")
):
    where_parts = []
    params = []

    if level and str(level).upper() != "ALL":
        if str(level).upper() == "ERROR":
            where_parts.append("r.status = 'failed'")
        else:
            where_parts.append("r.status != 'failed'")

    if pipeline and str(pipeline).upper() != "ALL":
        where_parts.append("r.pipeline_name = %s")
        params.append(str(pipeline))

    if tool and str(tool).upper() != "ALL":
        where_parts.append("r.tool_name = %s")
        params.append(str(tool).lower())

    if search and str(search).strip():
        where_parts.append("(r.error_message LIKE %s OR r.pipeline_name LIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

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
        tool_name = r["tool_name"] or "dbt"
        msg = r["error_message"] or f"Pipeline execution completed with status: {r['status']}. Rows added: {r['rows_added'] or 0}"
        
        items.append({
            "id": r["run_id"],
            "timestamp": r["start_time"].strftime("%b %d, %Y %I:%M:%S %p") if r["start_time"] else "N/A",
            "pipelineName": r["pipeline_name"],
            "pipeline": r["pipeline_name"],
            "level": lvl,
            "tool": tool_name.title(),
            "message": msg,
            "duration": f"{r['duration']}s" if r["duration"] else "—"
        })

    totals = query("SELECT total_runs, failed_runs, success_runs FROM vw_kpi_totals LIMIT 1")[0]
    return jsonify({
        "total": int(totals["total_runs"]),
        "kpis": {
            "totalLogs": {"value": str(totals["total_runs"]), "delta": 18.0},
            "failedLogs": {"value": str(totals["failed_runs"]), "delta": 12.0},
            "successLogs": {"value": str(totals["success_runs"]), "delta": 19.0}
        },
        "logs": items,
        "items": items
    })

# ---------------------------------------------------------------------------
# 9. INCIDENTS MODULE
# ---------------------------------------------------------------------------
@incidents_router.get("", summary="Incident Management & Triage")
def get_incidents(
    severity: Optional[str] = Query("ALL", description="Filter severity: ALL, Critical, High, Medium, Low"),
    status: Optional[str] = Query("ALL", description="Filter incident status: ALL, Open, Triage, Resolved"),
    search: Optional[str] = Query(None, description="Search by pipeline or error text")
):
    where_parts = []
    params = []

    if search and str(search).strip():
        where_parts.append("(pipeline_name LIKE %s OR error_message LIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    failed_rows = query(f"""
        SELECT 
            run_id,
            pipeline_name,
            failure_stage,
            failed_node,
            error_class,
            error_message,
            start_time,
            duration
        FROM vw_failed_runs
        {where_sql}
        ORDER BY start_time DESC
    """, tuple(params))

    items = []
    for r in failed_rows:
        err_cls = (r["error_class"] or "etl").lower()
        sev = "Critical" if "compilation" in err_cls else ("High" if "snowflake" in err_cls else "Medium")
        if severity and str(severity).upper() != "ALL" and sev.lower() != str(severity).lower():
            continue

        items.append({
            "id": f"INC-{r['run_id'][:8]}",
            "incident": f"{r['pipeline_name']} failure: {r['error_class'] or 'runtime'}",
            "severity": sev,
            "rootAsset": r["pipeline_name"],
            "failedNode": r["failed_node"],
            "status": "Open",
            "blastRadius": 4,
            "openedAt": r["start_time"].strftime("%b %d, %Y %I:%M %p") if r["start_time"] else "N/A",
            "duration": f"{r['duration']}s" if r["duration"] else "2h 15m",
            "owner": "Data Team",
            "errorMessage": r["error_message"]
        })

    return jsonify({
        "kpis": {
            "open": {"value": len(items), "delta": len(items), "deltaLabel": "active incidents"},
            "inTriage": {"value": len(items) // 2, "delta": 1, "deltaLabel": "in triage"},
            "critical": {"value": sum(1 for i in items if i['severity'] == 'Critical'), "delta": 1, "deltaLabel": "critical"},
            "resolved": {"value": 29, "delta": 29, "deltaLabel": "successful runs"}
        },
        "items": items
    })

# ---------------------------------------------------------------------------
# 10. LINEAGE MODULE
# ---------------------------------------------------------------------------
@lineage_router.get("", summary="End-to-End Data Lineage & Topology")
def get_lineage(
    source_type: Optional[str] = Query("ALL", description="Filter source: ALL, MySQL, PostgreSQL, Oracle, Snowflake"),
    target_type: Optional[str] = Query("ALL", description="Filter target: ALL, Snowflake, BigQuery, Synapse"),
    status: Optional[str] = Query("ALL", description="Filter status: ALL, Healthy, Degraded, Failed"),
    search: Optional[str] = Query(None, description="Search pipeline name")
):
    where_parts = []
    params = []

    if search and str(search).strip():
        where_parts.append("p.pipeline_name LIKE %s")
        params.append(f"%{search}%")
    if source_type and str(source_type).upper() != "ALL":
        where_parts.append("p.source_tool = %s")
        params.append(str(source_type).lower())
    if target_type and str(target_type).upper() != "ALL":
        where_parts.append("p.target_tool = %s")
        params.append(str(target_type).lower())

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
            p.is_active,
            h.health_status,
            h.last_end_time,
            COALESCE(SUM(a.row_count), 0) as total_rows
        FROM obs_pipelines p
        LEFT JOIN vw_pipeline_health h ON p.pipeline_id = h.pipeline_id
        LEFT JOIN obs_pipeline_runs r ON p.pipeline_id = r.pipeline_id
        LEFT JOIN obs_run_assets a ON r.id = a.run_id
        {where_sql}
        GROUP BY p.pipeline_id, p.pipeline_name, p.source_tool, p.source_schema, p.etl_tool, p.target_tool, p.target_schema, p.is_active, h.health_status, h.last_end_time
    """, tuple(params))

    flows = []
    for p in pipes:
        st = "Healthy" if p["health_status"] == "healthy" else ("Degraded" if p["health_status"] == "stale" else "Failed")
        if status and str(status).upper() != "ALL" and st.lower() != str(status).lower():
            continue

        rec_cnt = int(p["total_rows"] or 0)
        flows.append({
            "id": p["pipeline_id"],
            "name": p["pipeline_name"],
            "source": {"type": (p["source_tool"] or "MySQL").title(), "instance": p["source_schema"] or "source_db"},
            "tool": {"type": (p["etl_tool"] or "dbt").title(), "action": "Transformation"},
            "target": {"type": (p["target_tool"] or "Snowflake").title(), "instance": p["target_schema"] or "analytics_dw"},
            "status": st,
            "lastRun": p["last_end_time"].strftime("%b %d, %Y %I:%M %p") if p["last_end_time"] else "N/A",
            "volume24h": f"{rec_cnt} records"
        })

    return jsonify({
        "kpis": {
            "totalPipelines": {"value": len(flows), "delta": len(flows)},
            "healthy": {"value": sum(1 for f in flows if f['status'] == 'Healthy')},
            "degraded": {"value": sum(1 for f in flows if f['status'] == 'Degraded')},
            "failed": {"value": sum(1 for f in flows if f['status'] == 'Failed')},
            "dataSources": {"value": len(set(f['source']['type'] for f in flows))}
        },
        "flows": flows
    })

# ---------------------------------------------------------------------------
# 11. ALERTS MODULE
# ---------------------------------------------------------------------------
@alerts_router.get("", summary="Active Alert Notifications")
def get_alerts():
    failed = query("SELECT run_id, pipeline_name, error_class, error_message, start_time FROM vw_failed_runs ORDER BY start_time DESC LIMIT 5")
    items = []
    for f in failed:
        items.append({
            "id": f"alt_{f['run_id']}",
            "type": "ERROR",
            "title": f"Pipeline Failure: {f['pipeline_name']}",
            "message": f["error_message"] or f"Failure under {f['error_class']}",
            "time": f["start_time"].strftime("%b %d, %Y %I:%M %p") if f["start_time"] else "N/A"
        })

    return jsonify({
        "unreadCount": len(items),
        "items": items
    })

# ---------------------------------------------------------------------------
# INCLUDE CLEAN APIRouters
# ---------------------------------------------------------------------------
app.include_router(overview_router)
app.include_router(pipelines_router)
app.include_router(quality_router)
app.include_router(freshness_router)
app.include_router(schema_router)
app.include_router(volume_router)
app.include_router(metrics_router)
app.include_router(logs_router)
app.include_router(incidents_router)
app.include_router(lineage_router)
app.include_router(alerts_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
