-- 01_seed.sql
-- Synthetic seed data for Manufacturing Ops SQL project (PostgreSQL)

-- Dimensions
insert into dim_plants values
  (1, 'Plant A'),
  (2, 'Plant B');

insert into dim_lines values
  (10, 1, 'Line A1'),
  (11, 1, 'Line A2'),
  (20, 2, 'Line B1');

insert into dim_machines values
  (100, 10, 'Mixer-10'),
  (101, 10, 'Filler-10'),
  (110, 11, 'Mixer-11'),
  (111, 11, 'Filler-11'),
  (200, 20, 'Mixer-20'),
  (201, 20, 'Filler-20');

insert into dim_products values
  (1, 'Coatings',   'Product C1'),
  (2, 'Coatings',   'Product C2'),
  (3, 'Adhesives',  'Product A1'),
  (4, 'Sealants',   'Product S1');

insert into dim_customers values
  (1, 'Customer East'),
  (2, 'Customer West'),
  (3, 'Customer Central');

insert into dim_reason_codes values
  ('MECH', 'MECH',       'Mechanical failure'),
  ('ELEC', 'ELEC',       'Electrical issue'),
  ('CHNG', 'CHANGEOVER', 'Changeover/setup'),
  ('MTRL', 'MATERIAL',   'Material shortage/quality'),
  ('QUAL', 'QUALITY',    'Quality hold'),
  ('PLAN', 'PLANNING',   'Planning/starvation');

-- Production runs (work orders): ~2 runs/day across the 3 lines (random assignment)
with days as (
  select (current_date - 60 + i)::date as d
  from generate_series(1, 60) as g(i)
),
runs as (
  select
    row_number() over ()::bigint as work_order_id,
    (array[10,11,20])[1 + (random()*2)::int] as line_id,
    (array[1,2,3,4])[1 + (random()*3)::int] as product_id,
    (d::timestamp + time '06:00' + ((random()*10)::int) * interval '1 hour') as planned_start_ts
  from days
  cross join generate_series(1, 2) as r(n)
)
insert into fct_production_runs (
  work_order_id, line_id, product_id,
  planned_start_ts, planned_end_ts,
  actual_start_ts, actual_end_ts,
  good_qty, scrap_qty, uom
)
select
  work_order_id,
  line_id,
  product_id,
  planned_start_ts,
  planned_start_ts + interval '6 hour' as planned_end_ts,
  planned_start_ts + ((random()*30)::int) * interval '1 minute' as actual_start_ts,
  planned_start_ts + interval '6 hour' + ((random()*60)::int) * interval '1 minute' as actual_end_ts,
  (800 + (random()*400)::int) as good_qty,
  (random()*80)::int as scrap_qty,
  'unit'
from runs;

-- Machine events: create state-change timestamps per machine across days
-- Pattern: mostly RUN with periodic STOP; STOP events get a random reason code
with days as (
  select (current_date - 60 + i)::date as d
  from generate_series(1, 60) as g(i)
),
machine_day as (
  select
    m.machine_id,
    d.d,
    (d.d::timestamp + time '00:00') as day_start
  from dim_machines m
  cross join days d
),
event_times as (
  select
    row_number() over ()::bigint as event_id,
    machine_id,
    (day_start + (k * interval '2 hour') + ((random()*40)::int) * interval '1 minute') as event_ts,
    k
  from machine_day
  cross join generate_series(0, 11) as k
),
states as (
  select
    event_id,
    machine_id,
    event_ts,
    case when (k % 5 = 0) then 'STOP' else 'RUN' end as state
  from event_times
)
insert into fct_machine_events (event_id, machine_id, event_ts, state, reason_code, work_order_id)
select
  s.event_id,
  s.machine_id,
  s.event_ts,
  s.state,
  case
    when s.state = 'STOP' then (array['MECH','ELEC','CHNG','MTRL','QUAL','PLAN'])[1 + (random()*5)::int]
    else null
  end as reason_code,
  (
    select pr.work_order_id
    from fct_production_runs pr
    join dim_machines dm on dm.line_id = pr.line_id
    where dm.machine_id = s.machine_id
      and pr.actual_start_ts::date = s.event_ts::date
    order by random()
    limit 1
  ) as work_order_id
from states s;

-- QC results: 2 samples per work order; ~12% out of spec
with samples as (
  select
    row_number() over ()::bigint as qc_id,
    pr.work_order_id,
    pr.actual_start_ts + ((random()*300)::int) * interval '1 minute' as sample_ts,
    (array['viscosity','thickness','adhesion'])[1 + (random()*2)::int] as test_type,
    (random()*100)::numeric as value,
    (random() > 0.12) as in_spec
  from fct_production_runs pr
  cross join generate_series(1, 2) as k
)
insert into fct_qc_results (qc_id, work_order_id, sample_ts, test_type, value, in_spec)
select qc_id, work_order_id, sample_ts, test_type, value, in_spec
from samples;

-- Shipments: requested vs actual ship date, ~22% late by 1-7 days
with orders as (
  select
    row_number() over ()::bigint as order_id,
    (array[1,2,3])[1 + (random()*2)::int] as customer_id,
    (array[1,2,3,4])[1 + (random()*3)::int] as product_id,
    (current_date - 60 + (random()*60)::int)::date as requested_ship_date,
    (200 + (random()*800)::int) as shipped_qty
  from generate_series(1, 300) as g(i)
)
insert into fct_shipments (shipment_id, customer_id, product_id, order_id, requested_ship_date, actual_ship_date, shipped_qty)
select
  row_number() over ()::bigint as shipment_id,
  customer_id,
  product_id,
  order_id,
  requested_ship_date,
  requested_ship_date + (
    case
      when random() < 0.22 then (1 + (random()*6)::int)
      else 0
    end
  ) as actual_ship_date,
  shipped_qty
from orders;

-- ------------------------------------------------------------
-- Notes
-- - Data is intentionally synthetic but structured like real ops data:
--   (1) Work orders with planned vs actual times and good/scrap
--   (2) Machine state changes (RUN/STOP) with reason codes on STOP
--   (3) QC sampling (in-spec / out-of-spec)
--   (4) Shipments (OTIF-ready: requested vs actual ship dates)
-- - Because work orders are randomly assigned, some line-days may have
--   machine events but no production runs (useful for data quality checks).
-- ------------------------------------------------------------
