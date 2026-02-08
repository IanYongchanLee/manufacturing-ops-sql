-- 03_analysis.sql
-- Analysis queries (PostgreSQL) for Manufacturing Ops SQL project
-- Goal: demonstrate how the KPI mart + facts enable common manufacturing analytics:
-- (1) Line performance, (2) Downtime Pareto, (3) Quality, (4) Schedule adherence, (5) OTIF-style shipping,
-- (6) Data quality checks between logs and production.

-- ============================================================
-- A) KPI MART QUICK CHECKS
-- ============================================================

-- A1) Latest 14 days KPI snapshot (line x day)
select *
from mart_daily_line_kpi
where day >= current_date - 14
order by day desc, line_id;

-- A2) Best / worst days by throughput (last 30 days)
select
  day,
  line_id,
  throughput_per_hour,
  runtime_min,
  downtime_min,
  good_qty,
  scrap_qty,
  yield
from mart_daily_line_kpi
where day >= current_date - 30
  and throughput_per_hour is not null
order by throughput_per_hour desc
limit 15;

select
  day,
  line_id,
  throughput_per_hour,
  runtime_min,
  downtime_min,
  good_qty,
  scrap_qty,
  yield
from mart_daily_line_kpi
where day >= current_date - 30
  and throughput_per_hour is not null
order by throughput_per_hour asc
limit 15;


-- ============================================================
-- B) DOWNTIME ANALYSIS (PARETO + TOP DRIVERS)
-- ============================================================

-- B1) Downtime Pareto by reason category (last 30 days)
-- Interprets STOP durations and aggregates by reason.
with ordered as (
  select
    e.*,
    lead(event_ts) over (partition by machine_id order by event_ts) as next_ts
  from fct_machine_events e
),
durations as (
  select
    date_trunc('day', e.event_ts)::date as day,
    e.state,
    rc.reason_category,
    greatest(extract(epoch from (coalesce(e.next_ts, e.event_ts) - e.event_ts)) / 60.0, 0) as duration_min
  from ordered e
  left join dim_reason_codes rc on rc.reason_code = e.reason_code
  where e.event_ts::date >= current_date - 30
),
stops as (
  select
    coalesce(reason_category, 'UNKNOWN') as reason_category,
    sum(case when state = 'STOP' then duration_min else 0 end) as downtime_min
  from durations
  group by 1
),
pareto as (
  select
    reason_category,
    downtime_min,
    sum(downtime_min) over () as total_downtime_min,
    sum(downtime_min) over (order by downtime_min desc) as cumulative_downtime_min
  from stops
)
select
  reason_category,
  round(downtime_min, 2) as downtime_min,
  round(downtime_min / nullif(total_downtime_min, 0), 4) as share,
  round(cumulative_downtime_min / nullif(total_downtime_min, 0), 4) as cumulative_share
from pareto
order by downtime_min desc;

-- B2) Top downtime drivers by line (last 30 days)
with ordered as (
  select
    e.*,
    lead(event_ts) over (partition by machine_id order by event_ts) as next_ts
  from fct_machine_events e
),
durations as (
  select
    dm.line_id,
    date_trunc('day', e.event_ts)::date as day,
    e.state,
    rc.reason_category,
    greatest(extract(epoch from (coalesce(e.next_ts, e.event_ts) - e.event_ts)) / 60.0, 0) as duration_min
  from ordered e
  join dim_machines dm on dm.machine_id = e.machine_id
  left join dim_reason_codes rc on rc.reason_code = e.reason_code
  where e.event_ts::date >= current_date - 30
)
select
  line_id,
  coalesce(reason_category, 'UNKNOWN') as reason_category,
  round(sum(case when state='STOP' then duration_min else 0 end), 2) as downtime_min
from durations
group by 1,2
order by line_id, downtime_min desc;


-- ============================================================
-- C) PRODUCTION ANALYSIS (OUTPUT, SCRAP, SCHEDULE ADHERENCE)
-- ============================================================

-- C1) Output by product family (last 30 days)
select
  pr.actual_end_ts::date as day,
  p.product_family,
  sum(pr.good_qty) as good_qty,
  sum(pr.scrap_qty) as scrap_qty,
  round(sum(pr.good_qty)::numeric / nullif(sum(pr.good_qty + pr.scrap_qty), 0), 4) as yield
from fct_production_runs pr
join dim_products p on p.product_id = pr.product_id
where pr.actual_end_ts::date >= current_date - 30
group by 1,2
order by day desc, product_family;

-- C2) Line × product mix (last 30 days)
select
  pr.line_id,
  p.product_family,
  count(*) as runs,
  sum(pr.good_qty) as good_qty,
  sum(pr.scrap_qty) as scrap_qty
from fct_production_runs pr
join dim_products p on p.product_id = pr.product_id
where pr.actual_end_ts::date >= current_date - 30
group by 1,2
order by pr.line_id, good_qty desc;


-- ============================================================
-- D) QUALITY ANALYSIS (QC FAIL RATE + LINK TO YIELD)
-- ============================================================

-- D1) QC fail rate by test type (last 30 days)
select
  qr.test_type,
  count(*) as samples,
  sum(case when qr.in_spec = false then 1 else 0 end) as fails,
  round(avg(case when qr.in_spec = false then 1 else 0 end)::numeric, 4) as fail_rate
from fct_qc_results qr
where qr.sample_ts::date >= current_date - 30
group by 1
order by fail_rate desc, samples desc;

-- D2) QC fail rate by line/day (last 30 days)
-- Joins qc -> work order -> line
select
  pr.line_id,
  qr.sample_ts::date as day,
  count(*) as samples,
  sum(case when qr.in_spec = false then 1 else 0 end) as fails,
  round(avg(case when qr.in_spec = false then 1 else 0 end)::numeric, 4) as fail_rate
from fct_qc_results qr
join fct_production_runs pr on pr.work_order_id = qr.work_order_id
where qr.sample_ts::date >= current_date - 30
group by 1,2
order by day desc, line_id;


-- ============================================================
-- E) SHIPPING / OTIF-STYLE ANALYSIS (REQUESTED vs ACTUAL SHIP DATE)
-- ============================================================

-- E1) On-time shipping rate overall (last 60 days)
select
  round(avg(case when actual_ship_date <= requested_ship_date then 1 else 0 end)::numeric, 4) as on_time_rate,
  count(*) as shipments
from fct_shipments
where requested_ship_date >= current_date - 60;

-- E2) On-time shipping by customer (last 60 days)
select
  c.customer_name,
  count(*) as shipments,
  round(avg(case when s.actual_ship_date <= s.requested_ship_date then 1 else 0 end)::numeric, 4) as on_time_rate,
  round(avg(greatest((s.actual_ship_date - s.requested_ship_date), 0)), 2) as avg_days_late
from fct_shipments s
join dim_customers c on c.customer_id = s.customer_id
where s.requested_ship_date >= current_date - 60
group by 1
order by on_time_rate asc, shipments desc;


-- ============================================================
-- End of 03_analysis.sql
-- Usage:
-- 1) Build schema + seed + mart (00 -> 01 -> 02)
-- 2) Run selected queries here for portfolio screenshots / insights
-- Suggested screenshots:
-- - Downtime Pareto (B1)
-- - KPI snapshot (A1)
-- - OTIF by customer (E2)
-- ============================================================
