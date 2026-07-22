<div align="center">

# 🛡️ Agentic Aegis

**Self-healing ETL: four cooperating agents that profile, score, fix, and watch your data — on top of a Bronze/Silver/Gold DuckDB warehouse.**

![Python](https://img.shields.io/badge/python-3.10%2B-3776AB?style=flat-square&logo=python&logoColor=white)
![Polars](https://img.shields.io/badge/polars-Arrow--native-CD792C?style=flat-square)
![Prefect](https://img.shields.io/badge/prefect-orchestrated-024DFD?style=flat-square)
![Streamlit](https://img.shields.io/badge/streamlit-live_dashboard-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![CI](https://img.shields.io/github/actions/workflow/status/viharireddy2-byte/Agentic-Aegis/ci.yml?branch=main&style=flat-square&label=CI)
![Docker](https://img.shields.io/badge/docker-ready-2496ED?style=flat-square&logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/kubernetes-manifests-326CE5?style=flat-square&logo=kubernetes&logoColor=white)
![MIT](https://img.shields.io/badge/license-MIT-6E56CF?style=flat-square)

[Quickstart](#quickstart) · [Architecture](#architecture) · [Design decisions](#design-decisions) · [Testing & CI](#testing--ci) · [Deployment](#deployment) · [Roadmap](#roadmap)

</div>

---

## The problem

Most ETL pipelines fail quietly. A schema drifts, a source starts sending nulls it never used to, a duplicate slips past a `dropna()` someone wrote two years ago — and nobody notices until a downstream number looks wrong. The usual fix is a growing pile of ad-hoc cleaning scripts that get harder to trust the longer they exist, because nobody can say what they changed or why.

Aegis takes a different approach: **detection, scoring, remediation, and anomaly-watching are four separate, narrow responsibilities**, not one script that tries to do all of it. Every fix is logged. Every score is explainable. Everything lands in a proper medallion warehouse instead of a folder of CSVs.

## Architecture

```
                    ┌──────────────┐
  CSV / Postgres /  │   Connector   │
  MySQL / S3   ───▶ │   (extract)   │
                    └──────┬───────┘
                           ▼
                  ┌────────────────┐
                  │  ScoutAgent    │  profiles the data,
                  │  (detect)      │  finds issues across
                  └───────┬────────┘  8 categories
                          ▼
        ┌─────────────────┴─────────────────┐
        ▼                                    ▼
┌───────────────┐                   ┌────────────────┐
│ SentinelAgent │                   │  HealerAgent   │
│ (score 0-100) │                   │  (remediate)   │
└───────────────┘                   └────────┬───────┘
                                              ▼
                                     ┌────────────────┐
                                     │  OracleAgent   │
                                     │ (anomalies +   │
                                     │  drift)        │
                                     └────────┬───────┘
                                              ▼
                        ┌─────────────────────────────────────┐
                        │   MedallionVault (DuckDB, in-proc)   │
                        │   Bronze  →  Silver  →  Gold         │
                        │   + pipeline_runs + data_lineage log │
                        └───────────────────┬───────────────────┘
                                            ▼
                              Streamlit dashboard (10 pages)
                              + Prometheus metrics + alerting
                              + Claude-powered NL querying
```

Scout only finds problems. Healer only fixes them. Neither has to guess at the other's job, and every Healer action is written to a lineage log — so "it fixed something, but what?" always has an answer.

## Features

| Component | What it does |
|---|---|
| 🕵️ **ScoutAgent** | Profiles a dataset across 8 categories: nulls, duplicates, whitespace, negative values, IQR-based outliers, inconsistent casing, format violations (regex), and schema drift against a reference schema. |
| 🧭 **SentinelAgent** | Converts a profile into a weighted 0–100 quality score across four dimensions (completeness, validity, consistency, uniqueness), and tracks the score run-over-run. |
| 🩹 **HealerAgent** | Deterministically remediates what Scout found — trims whitespace, drops duplicates, fixes negatives, imputes nulls, clips outliers — with a full audit trail per action. |
| 🔮 **OracleAgent** | `IsolationForest`-based statistical anomaly detection, plus cross-run drift detection that flags columns whose mean or null-rate has moved beyond a threshold. |
| 🏗️ **MedallionVault** | DuckDB-backed Bronze/Silver/Gold layers, in-process, zero server to run or patch. Persists quality history and a row-level lineage log. |
| 🔌 **Connectors** | CSV, PostgreSQL, MySQL, and S3 behind one `DataSource` interface — the agents and orchestration layer never know or care where a row came from. |
| 📡 **Observability** | Prometheus-compatible metrics (falls back to an in-memory implementation if `prometheus_client` isn't installed) and threshold-based webhook alerting on quality regressions or anomaly spikes. |
| 🤖 **QueryAgent** | Plain-English questions over the warehouse. Claude generates SQL from schema metadata only (never row data), and a regex + prefix check rejects anything that isn't a single `SELECT`. |
| 📥 **Streaming ingestion** | Watches a directory and triggers a pipeline run per file arrival — event-driven via `watchdog` if installed, polling otherwise. |
| 📊 **Dashboard** | 10-page Streamlit app: Overview, Bronze Explorer, Silver Analytics, Gold Insights, Quality Monitoring, Data Lineage, Pipeline Performance, Data Connectors, Ask Aegis, Observability. |
| 🐳 **Docker & Kubernetes** | A real `Dockerfile` / `docker-compose.yml`, plus `k8s/` manifests (Deployment, CronJob, RBAC, PVC) sized for how this actually runs — see [Design decisions](#design-decisions). |
| ✅ **CI/CD** | GitHub Actions: lint (ruff), full test suite across Python 3.10–3.12, and a Docker build smoke test, on every push/PR. |

## Design decisions

Short version of *why*, for anyone deciding whether to reuse a piece of this:

- **Polars, not pandas, for the hot path.** Multi-threaded and Arrow-native — matters once a dataset stops being small. Pandas is still in the dependency tree as a transitive requirement for a couple of libraries, but the pipeline itself never touches it.
- **DuckDB, not Postgres, for the warehouse.** In-process OLAP with zero ops burden, and it speaks Arrow/Polars natively — no serialization tax moving data between the two. The tradeoff, called out explicitly in the Kubernetes manifests (`replicas: 1`), is that DuckDB is single-writer: this doesn't horizontally scale as-is, and a shared backend (e.g. MotherDuck) would be the next step if it needed to.
- **Prefect for orchestration.** Retries, logging, and a task/flow model built in, instead of bolted on with a bespoke scheduler.
- **Detection is separated from correction on purpose.** It's not just naming — Scout has no ability to mutate data, and Healer has no ability to decide what counts as an issue. That boundary is what makes each side unit-testable in isolation and keeps a bad Healer change from silently redefining what "wrong" means.
- **QueryAgent is the one place an LLM touches this system, and it's deliberately boxed in.** The rest of the pipeline is rule-based and statistical — deterministic and auditable, which is the right call for anything that mutates data. Natural-language querying is read-only by nature, so it's the one component where non-determinism is an acceptable trade for flexibility. It still gets three separate guardrails: schema-only context sent to the model (never row-level data), a `SELECT`-only prefix check, and a keyword denylist for `INSERT`/`UPDATE`/`DELETE`/`DROP`/`ALTER`/etc.
- **Optional dependencies degrade to a clear error, not a crash.** Connector, observability, streaming, and QueryAgent extras live in `requirements-extra.txt`. Skip it and the core pipeline still runs — you get an actionable `ConnectorError`/`QueryAgentError` if you try to use a feature you haven't installed, not an `ImportError` stack trace.

### Known limitations (being upfront about the gaps)

- **No schema-contract enforcement on the Silver layer yet.** Schema *drift detection* exists (Scout compares against a reference schema; Oracle compares stats across runs), but there's no declarative schema contract that rejects a write outright. This is the top item in the [roadmap](#roadmap).
- **The Gold-layer aggregation is currently a fixed query** (category/revenue rollup) tied to the sample e-commerce schema, not a configurable aggregation step. Fine for the demo dataset; would need to be parameterized before pointing this at an arbitrary source.
- **Connector table/query identifiers are not parameterized.** They're interpolated into SQL directly, which is safe as long as connector config comes from a trusted source (your own config file/env), but would need an identifier allowlist before accepting that config from anything less trusted.
- **The dashboard has no auth layer.** It's designed to sit behind whatever the deployment environment already provides (a reverse proxy, an internal-only ingress) — it does not do its own authentication.

## Quickstart

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

Open **http://localhost:8080** to explore the dashboard.

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

### Docker, in one command

```bash
docker build -t agentic-aegis .
docker run -p 8080:8080 -v $(pwd)/data:/app/data agentic-aegis
```

Or `docker compose up --build` for the dashboard plus a throwaway Postgres instance to try the Postgres connector against.

## Testing & CI

```bash
pytest tests/ -v
```

| File | Covers |
|---|---|
| `tests/test_agents.py` | Scout/Sentinel/Healer/Oracle logic against known-dirty fixtures |
| `tests/test_database.py` | MedallionVault: layer promotion, lineage logging, query history |
| `tests/test_connectors.py` | Registry dispatch, error handling, credential redaction in `describe()` |
| `tests/test_observability.py` | Metrics recording, alert threshold logic |
| `tests/test_nlquery.py` | QueryAgent's SQL safety checks (the part that matters most) |
| `tests/test_streaming.py` | Directory-watcher ingestion triggers |

All tests run on fixtures — no external services required locally. CI (`.github/workflows/ci.yml`) runs `ruff check .`, the full suite across Python 3.10/3.11/3.12, and a Docker build smoke test on every push and PR.

## Deployment

`k8s/` has a full manifest set: `Deployment`, `Service`, `CronJob` (for scheduled pipeline runs separate from the always-on dashboard), `RBAC`, `ConfigMap`, and `PVC`. See [`docs/deployment.md`](docs/deployment.md) for the full walkthrough, including the DuckDB single-writer constraint mentioned above and what it means for scaling the dashboard versus the pipeline runner.

## Further reading

- [`docs/architecture.md`](docs/architecture.md) — system design, data flow, storage model
- [`docs/agents.md`](docs/agents.md) — algorithm-level detail for Scout, Sentinel, Healer, Oracle
- [`docs/connectors.md`](docs/connectors.md) — multi-source connectors (CSV, Postgres, MySQL, S3)
- [`docs/observability.md`](docs/observability.md) — metrics, alerting, streaming ingestion, QueryAgent
- [`docs/api-reference.md`](docs/api-reference.md) — full API reference for every public class/method
- [`docs/quickstart.md`](docs/quickstart.md) — expanded walkthrough with troubleshooting

Issues and PRs are welcome — please run `pytest` and `ruff check .` before submitting.

## Roadmap

- [ ] Declarative schema contract on the Silver layer (reject-on-write, not just drift-detection)
- [ ] Parameterized Gold-layer aggregations instead of a fixed category/revenue rollup
- [ ] Identifier allowlisting for connector table/query config
- [ ] Shared/multi-writer backend for horizontally-scaled dashboards (e.g. MotherDuck)
- [ ] Managed Prefect deployment templates for the CronJob equivalent outside K8s

**Already shipped:** medallion architecture · four autonomous agents · 10-page dashboard · data lineage + quality history · multi-source connectors · NL querying via Claude · streaming ingestion · K8s manifests + RBAC · CI/CD · Prometheus metrics + alerting.

## License

MIT — see [`LICENSE`](LICENSE).
