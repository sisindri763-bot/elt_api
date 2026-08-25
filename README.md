# VITHI Data Observability Engine API (`elt_api`)

Production-grade REST API backend for the **VITHI Data Observability Platform**, connecting to the central AWS RDS MySQL `metadata` database.

---

## Architecture & Features

- **Dynamic Analytics & KPI Metrics**: Aggregates real-time run performance across registered pipelines.
- **SLA & Freshness Engine**: Calculates data asset latency against SLA thresholds.
- **Schema Drift Detection**: Tracks data type evolution and column-level mutations across table runs.
- **Error & Incident Intelligence**: Surfaces stack traces, failure stages (`etl`, `compilation`), and root-cause assets.
- **Data Lineage**: Maps source systems to targets via transformation engines (`dbt`, `Fivetran`, `Airbyte`, `Snowflake`).

---

## Quick Start

### 1. Prerequisites
- Python 3.10+
- MySQL client libraries

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment
Copy `.env.example` to `.env` and fill in credentials:
```bash
cp .env.example .env
```

### 4. Run the Development Server
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
Interactive Swagger docs: `http://localhost:8000/docs`

---

## Available API Endpoints

| Endpoint | Method | Description |
| :--- | :---: | :--- |
| `/health` | `GET` | Database connectivity & health status |
| `/api/v1/overview` | `GET` | Consolidated overview dashboard payload |
| `/api/v1/overview/kpis` | `GET` | Real-time KPI cards & sparklines |
| `/api/v1/overview/charts` | `GET` | Daily execution & incident charts |
| `/api/v1/overview/health` | `GET` | 5-pillar observability health score |
| `/api/v1/overview/recent-incidents` | `GET` | Unresolved failures & pipeline incidents |
| `/api/v1/pipelines` | `GET` | Pipeline performance table & directory |
| `/api/v1/observability/quality` | `GET` | Data quality checks & pass/fail ratio |
| `/api/v1/observability/freshness` | `GET` | Asset freshness SLAs & lag metrics |
| `/api/v1/observability/schema` | `GET` | Schema definitions & data type breakdowns |
| `/api/v1/observability/volume` | `GET` | Row counts & volume timelines |
| `/api/v1/metrics` | `GET` | Live telemetry & pipeline performance |
| `/api/v1/logs` | `GET` | Real-time execution logs |
| `/api/v1/incidents` | `GET` | Incident manager with blast radius & triage |
| `/api/v1/lineage` | `GET` | End-to-end data lineage DAG |
| `/api/v1/alerts` | `GET` | Active system alert notifications |

---

## Running Tests
```bash
python test_endpoints.py
```
