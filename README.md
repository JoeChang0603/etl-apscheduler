# Data Warehouse APScheduler

This project packages a collection of asynchronous ETL jobs behind an APScheduler
instance and exposes their status, controls, and telemetry through a FastAPI
application. It also provides real-time notifications via Discord webhooks and
structured logging utilities.


## Table of Contents

1. [Project Structure](#project-structure)
2. [Prerequisites](#prerequisites)
3. [Environment Configuration](#environment-configuration)
4. [Running the Scheduler API](#running-the-scheduler-api)
5. [HTTP API Endpoints](#http-api-endpoints)
6. [WebSocket Streaming](#websocket-streaming)
7. [Manual Job Control](#manual-job-control)
8. [System Metrics](#system-metrics)
9. [Discord Alerts & Logging](#discord-alerts--logging)
10. [Jobs Overview](#jobs-overview)
11. [Development Workflow](#development-workflow)


## Project Structure

```
src/
  api/                 FastAPI application, routers, websockets
  bot/                 Discord webhook transports, handlers, alerters
  configs/             Environment configuration helpers
  jobs/                APScheduler job implementations
  mongo/               Motor-based MongoDB client wrapper
  scheduler/           APScheduler service, monitor, signal handling
  snapshot/            Exchange snapshot factories and implementations
  utils/               Shared utilities (logging, casting, metrics, etc.)

configs/jobs.yaml      Scheduler job definitions loaded at startup
docker-compose.yml     Local stack with Postgres + scheduler API
Dockerfile             Build instructions for the scheduler container
```


## Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) for dependency management (project uses `uv.sync`)
- Docker + Docker Compose (optional but recommended for parity with deployment)
- A running PostgreSQL instance configured via environment variables


## Environment Configuration

Populate `.env` (you can start from `.env.example`) with the required environment
variables. The application validates the presence of critical values at startup:

| Variable | Description |
|----------|-------------|
| `IS_TEST` | Whether to use the test Mongo databases (`T_DATA`, `T_MART`) |
| `MONGO_URI` | MongoDB connection string used by `motor` |
| `SQLALCHEMY_URL` | SQLAlchemy URL used by APScheduler job store |
| `ETL_PROCESS_WEBHOOK` | Discord webhook for scheduler/log notifications |
| `ETL_TOTAL_USD_VALUE_ALERT` | Discord webhook for threshold alerts |

Validate the configuration with:

```bash
uv run python -c "from configs.env_config import Env; Env.validate()"
```


## Running the Scheduler API

### Local (uv + uvicorn)

```bash
uv sync  # install dependencies
uv run uvicorn src.api.app:app --host 0.0.0.0 --port 8000
```

The FastAPI application boots the APScheduler service during startup, loads
`jobs.yaml`, and begins executing registered jobs.

### Docker Compose

```bash
docker-compose up --build
```

This starts Postgres and the scheduler API container listening on port `8000`.


## HTTP API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health/` | Service heartbeat plus scheduler state summary |
| `GET` | `/scheduler/status` | Scheduler state, job count, next run time, timezone |
| `GET` | `/scheduler/jobs` | All jobs with triggers, kwargs, and recent stats |
| `GET` | `/scheduler/jobs/{job_id}` | Detailed metadata and telemetry for one job |
| `POST` | `/scheduler/jobs/{job_id}/trigger` | Immediately run a job (supports overrides) |
| `POST` | `/scheduler/jobs/{job_id}/pause` | Temporarily pause a scheduled job |
| `POST` | `/scheduler/jobs/{job_id}/resume` | Resume a previously paused job |
| `POST` | `/scheduler/reload` | Reload job definitions from `jobs.yaml` |
| `GET` | `/system/metrics` | Process, system, and cgroup metrics for the container |

All timestamps are reported in UTC. Per-job telemetry retains up to 50 historical
events (submitted, success, error, missed).


## WebSocket Streaming

Connect to `ws://<host>:8000/ws/scheduler` to receive real-time scheduler events.

1. Upon connection, a `snapshot` payload is delivered containing scheduler status
   and the full job list with current stats.
2. Subsequent `event` payloads stream each job submission, success, error, or
   missed event.

Example subscriber (JavaScript):

```js
const ws = new WebSocket("ws://localhost:8000/ws/scheduler");
ws.onmessage = evt => console.log(JSON.parse(evt.data));
```


## Manual Job Control

Use the `POST /scheduler/jobs/{job_id}/trigger` endpoint to accelerate a job.
Optionally include overrides inside the request body:

```json
{
  "overrides": {
    "kwargs": {
      "symbol": "ETHUSDT",
      "dry_run": true
    }
  }
}
```

When overrides are provided, the scheduler clones the job with a one-off
`DateTrigger`. Without overrides, it modifies the existing job’s `next_run_time`
to run immediately.


## System Metrics

The `GET /system/metrics` endpoint surfaces:

- Process-level CPU, RSS, thread count, open files, command line
- Host CPU load, memory, swap utilisation
- cgroup memory/cpu quotas and statistics (if running in a container)

Useful for health dashboards and alerting when running on EC2 or similar VMs.


## Discord Alerts & Logging

`src/bot/discord.py` defines reusable transports:

- `DiscordHandler`: integrates with the custom logger to forward log events.
- `DiscordAlerter`: key-based throttled alerting with deduplication support.

Jobs such as `DATA_account_summary_1_minute` use `DiscordAlerter` to notify when
portfolio thresholds are breached. Configure webhooks via environment variables.


## Jobs Overview

| Job | Description |
|-----|-------------|
| `DATA_account_summary_1_minute` | Snapshots active portfolios, persists results, and alerts on USD thresholds. |
| `MART_portfolio_performance` | Aggregates account summaries and computes portfolio performance metrics. |
| `MART_master_portfolio_performance` | Aggregates composite/master portfolios and computes performance metrics. |

Jobs rely on MongoDB (`C_DATA`, `C_MART` or test equivalents) and produce rotated
logs under `logs/` using the custom logging framework.


## Development Workflow

1. **Install dependencies:** `uv sync`
2. **Run style checkers / type hints** (optional but recommended) using your
   preferred tooling.
3. **Start services:** either `uvicorn` locally or `docker-compose up --build`
4. **Interact with APIs:**
   - Swagger UI at `http://localhost:8000/docs`
   - WebSocket via `ws://localhost:8000/ws/scheduler`
5. **Run jobs manually:** `POST /scheduler/jobs/{job_id}/trigger`
6. **Monitor logs:** Rotating files under `logs/` and Discord channels if
   configured.

When contributing code, maintain the docstring format used throughout the
repository (module docstring plus `:param`/`:return`/`:raises` sections where
appropriate) and keep job definitions up to date in `configs/jobs.yaml`.
