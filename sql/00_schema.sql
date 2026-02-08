-- 00_schema.sql
-- Manufacturing Ops SQL Schema (PostgreSQL)

-- Drop in dependency-safe order
drop table if exists fct_machine_events;
drop table if exists fct_qc_results;
drop table if exists fct_shipments;
drop table if exists fct_production_runs;

drop table if exists dim_reason_codes;
drop table if exists dim_machines;
drop table if exists dim_lines;
drop table if exists dim_plants;
drop table if exists dim_products;
drop table if exists dim_customers;

-- Dimensions
create table dim_plants (
  plant_id      int primary key,
  plant_name    text not null
);

create table dim_lines (
  line_id       int primary key,
  plant_id      int not null references dim_plants(plant_id),
  line_name     text not null
);

create table dim_machines (
  machine_id    int primary key,
  line_id       int not null references dim_lines(line_id),
  machine_name  text not null
);

create table dim_products (
  product_id     int primary key,
  product_family text not null,
  product_name   text not null
);

create table dim_customers (
  customer_id    int primary key,
  customer_name  text not null
);

create table dim_reason_codes (
  reason_code      text primary key,
  reason_category  text not null,
  reason_desc      text not null
);

-- Facts
create table fct_production_runs (
  work_order_id     bigint primary key,
  line_id           int not null references dim_lines(line_id),
  product_id        int not null references dim_products(product_id),
  planned_start_ts  timestamp not null,
  planned_end_ts    timestamp not null,
  actual_start_ts   timestamp not null,
  actual_end_ts     timestamp not null,
  good_qty          int not null,
  scrap_qty         int not null,
  uom               text not null default 'unit'
);

create table fct_machine_events (
  event_id      bigint primary key,
  machine_id    int not null references dim_machines(machine_id),
  event_ts      timestamp not null,
  state         text not null check (state in ('RUN','STOP')),
  reason_code   text null references dim_reason_codes(reason_code),
  work_order_id bigint null references fct_production_runs(work_order_id)
);

create index if not exists idx_fct_machine_events_machine_ts
  on fct_machine_events(machine_id, event_ts);

create table fct_qc_results (
  qc_id         bigint primary key,
  work_order_id bigint not null references fct_production_runs(work_order_id),
  sample_ts     timestamp not null,
  test_type     text not null,
  value         numeric not null,
  in_spec       boolean not null
);

create table fct_shipments (
  shipment_id         bigint primary key,
  customer_id         int not null references dim_customers(customer_id),
  product_id          int not null references dim_products(product_id),
  order_id            bigint not null,
  requested_ship_date date not null,
  actual_ship_date    date not null,
  shipped_qty         int not null
);

-- ------------------------------------------------------------
-- Notes
-- - Star-ish schema: dimensions provide context; facts store events and transactions.
-- - fct_machine_events stores state changes (RUN/STOP). Duration is derived later via LEAD().
-- - work_order_id on fct_machine_events is optional (nullable) because not all events map cleanly to a work order.
-- ------------------------------------------------------------
