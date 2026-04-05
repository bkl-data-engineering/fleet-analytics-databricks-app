# Fleet Analytics Databricks App

This repository is the Databricks App packaging and deployment-oriented evolution of the original `fleet-analytics-api-databricks` project.

## Overview

This application provides FastAPI endpoints for fleet analytics on curated Databricks Gold tables:

- `/ask/driver`
- `/ask/vehicle`
- `/ask/fleet-daily-summary`

It uses a hybrid pattern:

- deterministic Spark queries for known questions
- optional LLM fallback for flexible natural-language responses

## Project Structure

```text
app/
├── __init__.py
├── main.py
├── config.py
├── dependencies.py
├── routers/
│   ├── __init__.py
│   ├── driver.py
│   ├── vehicle.py
│   └── fleet_summary.py
├── services/
│   ├── __init__.py
│   ├── driver_analytics_service.py
│   ├── vehicle_efficiency_service.py
│   └── fleet_daily_summary_service.py
└── clients/
    ├── __init__.py
    ├── databricks_client.py
    └── llm_client.py
```

## Local Run
``` bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## Git steps
```bash
mkdir fleet-analytics-databricks-app
cd fleet-analytics-databricks-app
git init
mkdir -p app/routers app/services app/clients tests
touch app/__init__.py app/routers/__init__.py app/services/__init__.py app/clients/__init__.py
```
