<div align="center">

# 🛡️ Agentic Aegis

**Data pipelines that fix themselves.**

![Python](https://img.shields.io/badge/python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![Polars](https://img.shields.io/badge/polars-Arrow--native-CD792C?style=flat-square)
![Prefect](https://img.shields.io/badge/prefect-orchestrated-024DFD?style=flat-square)
![Streamlit](https://img.shields.io/badge/streamlit-live_dashboard-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![CI](https://img.shields.io/github/actions/workflow/status/viharireddy2-byte/Agentic-Aegis/ci.yml?branch=main&style=flat-square&label=CI)
![Docker](https://img.shields.io/badge/docker-ready-2496ED?style=flat-square&logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/kubernetes-manifests-326CE5?style=flat-square&logo=kubernetes&logoColor=white)
![MIT](https://img.shields.io/badge/license-MIT-6E56CF?style=flat-square)

</div>

<br>

## Introduction

Most ETL pipelines fail quietly — a schema drifts, a source starts sending
nulls it never used to, a duplicate slips past a `dropna()` someone wrote
two years ago — and nobody notices until a downstream number looks wrong.

Aegis takes a different approach. Four autonomous agents sit inside the
pipeline itself: one profiles the data, one scores it, one fixes what it
can, and one watches for the anomalies a fixed rule set would never catch.
Everything lands in a DuckDB-backed **Medallion architecture** (Bronze →
Silver → Gold), with a 10-page Streamlit dashboard on top — including
plain-English querying of your own data, live connector status, and
pipeline metrics/alerting.

<br>

## 💡 Why This Architecture Matters

Hand-written cleaning logic doesn't scale — it's a pile of `.dropna()` and
`.drop_duplicates()` calls that grows forever and breaks silently the
moment a source changes shape. Aegis separates that work into four
narrow, auditable responsibilities instead of one growing script:

- **Detection is separated from correction.** Scout only finds problems; Healer only fixes them. Neither has to guess at the other's job.
- **Quality is a number, not a vibe.** Sentinel's 0–100 score means "is this pipeline healthy" has an actual answer you can track over time, not a gut feeling.
- **Fixes are explainable.** Every Healer action is logged — what changed, and why — so nothing gets silently rewritten.
- **Rules alone aren't enough.** Oracle's statistical drift and anomaly detection catches the class of problems that no if-statement was written to check for.
- **Storage stays simple.** DuckDB runs in-process — no server to provision, patch, or take down for maintenance.

<br>

## 🔧 Hardcore Features

- **🕵️ ScoutAgent** — profiles every dataset across 8 issue categories: nulls, duplicates, whitespace, negative values, outliers, inconsistent casing, format violations, schema drift.
- **🧭 SentinelAgent** — converts that profile into an explainable 0–100 quality score across four dimensions, and tracks the trend run-over-run.
- **🩹 HealerAgent** — deterministically remediates what Scout found, with a full audit trail of every action taken.
- **🔮 OracleAgent** *(new)* — IsolationForest-based statistical anomaly detection and cross-run drift detection.
- **🏗️ Medallion storage** — DuckDB-backed Bronze/Silver/Gold layers, in-process, zero server required.
- **📊 Live dashboard** — a 10-page Streamlit app covering every layer, quality trends, lineage, pipeline performance, connectors, observability, and natural-language querying.
- **🔌 Multi-source connectors** — CSV, PostgreSQL, MySQL, and S3, behind one `DataSource` interface, so the agents never need to know where a row came from.
- **📡 Metrics + alerting** — Prometheus-compatible pipeline metrics and threshold-based Slack-style webhook alerts on quality regressions or anomaly spikes.
- **🤖 QueryAgent** — ask a plain-English question, get back safety-validated SQL (SELECT-only) and the answer, powered by Claude.
- **📥 Streaming ingestion** — watch a directory for new files and trigger a pipeline run per arrival, event-driven if `watchdog` is installed.
- **🐳 Docker & Kubernetes** — a real `Dockerfile`/`docker-compose.yml`, plus a full `k8s/` manifest set (Deployment, CronJob, RBAC, PVC) for production deployment.
- **✅ CI/CD** — GitHub Actions runs lint + the full test suite across Python 3.10–3.12, plus a Docker build smoke test, on every push/PR.

<br>

## 💪 Built for Real-World Challenges

Real pipelines don't fail in the ways a toy demo fails — they fail in
messy, inconsistent, slowly-drifting ways, which is what Aegis is actually
built to handle:

- **Schema drift** — Oracle's cross-run comparison flags structural changes before they break something downstream.
- **Silent quality decay** — Sentinel's run-over-run score trend surfaces a slow decline long before it becomes an incident.
- **"It fixed something, but what?"** — Healer's audit trail means every remediation is traceable back to the exact issue it addressed.
- **Outliers that rules miss** — Oracle's IsolationForest model catches statistically anomalous rows that a fixed threshold wouldn't flag.
- **Schema contracts that actually hold** — Pandera validation on the Silver layer means "cleaned" data has a guaranteed shape, not just a hopeful one.

<br>

## Why these tools specifically

**Polars** — multi-threaded and Arrow-native, which matters once datasets stop being small.
**DuckDB** — in-process OLAP with zero ops burden, and it speaks Arrow/Polars natively.
**Prefect for orchestration** — Pythonic flows with retries and logging built in, not bolted on.
**Pandera for validation** — schema contracts for the Silver layer are declarative, not scattered `assert` statements.
**scikit-learn's IsolationForest** — the model behind Oracle's anomaly detection.
**Streamlit for the dashboard** — a fast, pure-Python way to get 10 interactive pages running.
**SQLAlchemy + boto3** — the same connector interface talks to Postgres, MySQL, and S3 without agent-side changes.
**prometheus-client** — standard `/metrics` exposition format, with a zero-dependency in-memory fallback.
**Anthropic's Claude** — the model behind QueryAgent's natural-language-to-SQL translation.

<br>

## 🚀 How to Get Started

**Prerequisites:** Python 3.10+, 4GB RAM (minimum), 1GB free disk space.

```bash
git clone https://github.com/viharireddy2-byte/Agentic-Aegis.git
cd Agentic-Aegis

python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate

pip install -r requirements.txt

# Optional: connectors (Postgres/MySQL/S3), Prometheus metrics, QueryAgent,
# and event-driven streaming. Skip this and everything still works with
# graceful, actionable errors instead of a crash.
pip install -r requirements-extra.txt
```

```bash
# 1. One-time setup — creates data dirs + initializes the DuckDB vault
python scripts/setup_initial.py

# 2. Generate a sample e-commerce dataset with intentionally injected issues
python scripts/generate_sample_data.py --rows 1000

# 3. Run the full agentic pipeline
python -m src.orchestration.foundry_flows

# 4. Launch the dashboard
streamlit run dashboards/foundry_dashboard.py
```

Open **http://localhost:8080** and explore all 10 pages: Overview, Bronze
Explorer, Silver Analytics, Gold Insights, Quality Monitoring, Data
Lineage, Pipeline Performance, Data Connectors, Ask Aegis, and Observability.

### Docker, in one command

```bash
docker build -t agentic-aegis .
docker run -p 8080:8080 -v $(pwd)/data:/app/data agentic-aegis
```

Or `docker compose up --build` for the dashboard plus a throwaway
Postgres instance to try the Postgres connector against. See
[`docs/deployment.md`](docs/deployment.md) for Kubernetes manifests.

<details>
<summary>Expected pipeline output</summary>

```
Starting Agentic Aegis pipeline — run 3f9a1c2e
Extracted 1,020 rows from data/raw/orders.csv
Scout found 9 issue(s) across 7 categories
Sentinel quality score: 88.4/100 (good)
Healer applied 6 remediation action(s)
Oracle flagged 4 anomalous row(s)
Pipeline complete. Quality score 88.4/100, 6 remediation action(s),
4 anomaly(ies) flagged, 7 gold rows produced.
```

</details>

**Testing:**

```bash
pytest tests/ -v
```

Covers agent logic (`tests/test_agents.py`), the DuckDB storage layer
(`tests/test_database.py`), multi-source connectors
(`tests/test_connectors.py`), metrics/alerting
(`tests/test_observability.py`), the NL QueryAgent's SQL safety checks
(`tests/test_nlquery.py`), and streaming ingestion
(`tests/test_streaming.py`) — all with fixtures, no external services
required. CI runs the full suite on every push via
[`.github/workflows/ci.yml`](.github/workflows/ci.yml).

**Further reading:**

- [`docs/architecture.md`](docs/architecture.md) — system design, data flow, storage model
- [`docs/agents.md`](docs/agents.md) — algorithm-level detail for Scout, Sentinel, Healer, Oracle
- [`docs/connectors.md`](docs/connectors.md) — multi-source connectors (CSV, Postgres, MySQL, S3)
- [`docs/observability.md`](docs/observability.md) — metrics, alerting, streaming ingestion, QueryAgent
- [`docs/api-reference.md`](docs/api-reference.md) — full API reference for every public class/method
- [`docs/quickstart.md`](docs/quickstart.md) — expanded walkthrough with troubleshooting
- [`docs/deployment.md`](docs/deployment.md) — Docker, docker compose, Kubernetes, and CI/CD

Issues and PRs are welcome — please run `pytest` and `ruff check .` before submitting.

<br>

## ⚖️ Future Enhancements

- [x] Medallion architecture (Bronze/Silver/Gold)
- [x] Four autonomous agents (Scout, Sentinel, Healer, Oracle)
- [x] Streamlit dashboard (10 pages)
- [x] Data lineage + quality history persistence
- [x] Multi-source connectors (Postgres, MySQL, S3)
- [x] Claude-powered natural-language data queries (QueryAgent)
- [x] Real-time streaming ingestion (directory watcher, event-driven or polling)
- [x] Kubernetes deployment manifests + RBAC
- [x] CI/CD (GitHub Actions: lint, multi-version test matrix, Docker build)
- [x] Prometheus-compatible metrics + threshold-based alerting
- [ ] Shared/multi-writer backend for horizontally-scaled dashboards (e.g. MotherDuck)
- [ ] Managed Prefect deployment templates for the CronJob equivalent outside K8s

<br>

## License

MIT — see [`LICENSE`](LICENSE).
"# Agentic" 
