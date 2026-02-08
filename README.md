# Manufacturing Ops SQL (PostgreSQL)
**Daily Line KPI Mart from RUN/STOP event logs + Work Orders**

This project builds a small manufacturing analytics warehouse in PostgreSQL and materializes a daily, line-level KPI mart that summarizes **runtime, downtime, yield, and throughput**.

---

## Quick Start (Run Locally)

### Prerequisites
- PostgreSQL (v13+ recommended)
- `psql` CLI **or** pgAdmin Query Tool

### psql (recommended)
Create a database (example name: `manufacturing_ops`), then run:

```bash
createdb manufacturing_ops

psql -d manufacturing_ops -f sql/00_schema.sql
psql -d manufacturing_ops -f sql/01_seed.sql
psql -d manufacturing_ops -f sql/02_mart_daily_line_kpi.sql
psql -d manufacturing_ops -f sql/03_analysis.sql