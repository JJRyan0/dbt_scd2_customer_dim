-- Incremental model for dim_customers
-- Target table created by dbt will be analytics.dim_customers

{{ config(
  materialized='incremental',
  post_hook="{{ update_dim_customer(this) }}"
) }}

with deduped_customer as (  -- Deduplicate by customer_id and sort by latest record
  select
    customer_id,
    name,
    address,
    updated_dt,
    md5(coalesce(name, '') || coalesce(address, '')) AS hash_key,
    row_number() over(partition by customer_id order by updated_dt desc) as rn
  from {{ source('raw_customer', 'raw_customer') }}  -- Reference your source here (adjust schema if needed)
),
-- Keep only the latest records per customer
latest_customer_updates as (
  select
    customer_id,
    name,
    address,
    updated_dt,
    hash_key,
    rn
  from deduped_customer
  where rn = 1
),
-- Identify changes based on hash_key comparison
changed_customer_records as (
  select 
    lcu.customer_id,
    lcu.name,
    lcu.address,
    lcu.updated_dt,
    lcu.hash_key
  from latest_customer_updates lcu
  {% if is_incremental() %}
  left join {{ this }} t
    on lcu.customer_id = t.customer_id
    and t.is_current = TRUE
  where t.customer_id is null
    or lcu.hash_key <> t.hash_key
  {% endif %}
)

-- This is the part where we perform the INSERT
select
    nextval('customer_sk_seq') as customer_sk,  -- Make sure 'customer_sk_seq' exists
    customer_id,
    name,
    address,
    hash_key,
    updated_dt as effective_start,
    null as effective_end,
    true as is_current
from changed_customer_records

{% if is_incremental() %}
-- Get the max updated_dt from the raw_customer source table, not from {{ this }}
where updated_dt > (select max(updated_dt) from {{ source('raw_customer', 'raw_customer') }})
{% endif %}
