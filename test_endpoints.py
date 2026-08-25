import sys
from fastapi.testclient import TestClient

from main import app

client = TestClient(app)

endpoints = [
    ("/health", "Health Check"),
    ("/api/v1/overview/kpis", "Overview KPIs"),
    ("/api/v1/overview/charts", "Overview Charts"),
    ("/api/v1/overview/health", "Overview Health"),
    ("/api/v1/overview/recent-incidents", "Recent Incidents"),
    ("/api/v1/pipelines", "Pipelines List"),
    ("/api/v1/overview/pipeline-monitoring", "Pipeline Monitoring"),
    ("/api/v1/overview", "Consolidated Overview"),
    ("/api/v1/observability/quality", "Data Quality"),
    ("/api/v1/observability/freshness", "Data Freshness"),
    ("/api/v1/observability/schema", "Schema Observability"),
    ("/api/v1/observability/volume", "Volume Observability"),
    ("/api/v1/metrics", "Metrics Explorer"),
    ("/api/v1/logs", "Logs Stream"),
    ("/api/v1/incidents", "Incidents"),
    ("/api/v1/lineage", "Lineage"),
    ("/api/v1/alerts", "Alerts")
]

print("==================================================")
print("TESTING D:\\elt_api ENDPOINTS AGAINST RDS METADATA DB")
print("==================================================")

passed = 0
for path, desc in endpoints:
    try:
        res = client.get(path)
        if res.status_code == 200:
            print(f"[PASSED] 200 OK  : {path:<38} ({desc})")
            passed += 1
        else:
            print(f"[FAILED] {res.status_code} : {path:<38} ({desc}) -> {res.text[:100]}")
    except Exception as e:
        print(f"[ERROR]           : {path:<38} ({desc}) -> {str(e)}")

print("==================================================")
print(f"RESULTS: {passed}/{len(endpoints)} endpoints passed successfully.")
print("==================================================")
