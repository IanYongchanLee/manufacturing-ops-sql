-- 02_mart_daily_line_kpi.sql
-- Build daily line KPI mart from machine events + production runs

drop table if exists mart_daily_line_kpi;

create table mart_daily_line_kpi as
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
    e.reason_code,
    greatest(
      extract(epoch from (coalesce(e.next_ts, e.event_ts) - e.event_ts)) / 60.0,
      0
    ) as duration_min
  from ordered e
  join dim_machines dm on dm.machine_id = e.machine_id
),
line_time as (
  select
    line_id,
    day,
    sum(case when state='RUN'  then duration_min else 0 end) as runtime_min,
    sum(case when state='STOP' then duration_min else 0 end) as downtime_min
  from durations
  group by 1,2
),
line_prod as (
  select
    line_id,
    actual_end_ts::date as day,
    sum(good_qty) as good_qty,
    sum(scrap_qty) as scrap_qty
  from fct_production_runs
  group by 1,2
)
select
  coalesce(t.line_id, p.line_id) as line_id,
  coalesce(t.day, p.day) as day,
  coalesce(runtime_min, 0)::numeric(12,2) as runtime_min,
  coalesce(downtime_min, 0)::numeric(12,2) as downtime_min,
  coalesce(good_qty, 0) as good_qty,
  coalesce(scrap_qty, 0) as scrap_qty,
  case
    when (coalesce(good_qty,0) + coalesce(scrap_qty,0)) = 0 then null
    else (coalesce(good_qty,0)::numeric / (coalesce(good_qty,0)+coalesce(scrap_qty,0)))
  end as yield,
  case
    when coalesce(runtime_min,0) = 0 then null
    when (coalesce(good_qty,0) + coalesce(scrap_qty,0)) = 0 then null
    else (coalesce(good_qty,0)::numeric / (runtime_min/60.0))
  end as throughput_per_hour
from line_time t
full join line_prod p
  on t.line_id = p.line_id and t.day = p.day;

create index if not exists idx_mart_daily_line_kpi_day_line
  on mart_daily_line_kpi(day, line_id);

-- ------------------------------------------------------------
-- Notes
-- - "Sessionizes" event logs into durations:
--     next_ts = LEAD(event_ts) per machine
--     duration = next_ts - event_ts
-- - Aggregates durations into line/day runtime and downtime (minutes).
-- - Joins runtime/downtime with production outputs (good/scrap) to compute:
--     yield = good / (good + scrap)
--     throughput_per_hour = good / (runtime_min / 60)
-- - FULL JOIN keeps line-days that appear in either events or production,
--   which helps surface log-vs-output data quality mismatches.
-- - Index (day, line_id) speeds up common queries:
--     WHERE day >= ...  ORDER BY day, line_id  and line comparisons.
-- ------------------------------------------------------------
