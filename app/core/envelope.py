import json
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj) if "." in str(obj) else int(obj)
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        return super().default(obj)

def clean_val(val):
    if isinstance(val, dict):
        return {k: clean_val(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [clean_val(v) for v in val]
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val) if "." in str(val) else int(val)
    return val

def build_envelope(
    range_info: Dict[str, Any] = None,
    filters_applied: Dict[str, Any] = None,
    kpis: Any = None,
    series: Any = None,
    charts: Dict[str, Any] = None,
    items: List[Any] = None,
    pagination: Dict[str, Any] = None,
    pillars: List[Any] = None,
    incidents: List[Any] = None,
    pipelines: List[Any] = None,
    health: Dict[str, Any] = None,
    summary: Dict[str, Any] = None,
    extra_fields: Dict[str, Any] = None
) -> Dict[str, Any]:
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    res = {
        "ok": True,
        "generated_at": now_iso,
        "range": range_info or {
            "from": (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S"),
            "to": now_str,
            "preset": "24h"
        },
        "filters_applied": filters_applied or {
            "preset": "24h",
            "pipeline_name": None,
            "pipeline_id": None,
            "status": None,
            "tool": None
        },
        "kpis": kpis if kpis is not None else [],
        "series": series if series is not None else {},
        "charts": charts if charts is not None else {},
        "items": items if items is not None else [],
        "pagination": pagination or {
            "page": 1,
            "page_size": 20,
            "total": len(items) if items else 0,
            "total_pages": 1
        },
        "pillars": pillars if pillars is not None else [],
        "incidents": incidents if incidents is not None else [],
        "pipelines": pipelines if pipelines is not None else [],
        "health": health if health is not None else {},
        "summary": summary if summary is not None else {},
        "meta": {
            "environment": "production",
            "api_version": "v1"
        }
    }
    if extra_fields:
        res.update(extra_fields)
    return json.loads(json.dumps(clean_val(res), cls=CustomEncoder))
