-- incremental model for dim customers
-- target table created by dbt will be analytics.dim_customers

{{ config(materialized ='incremental') }}

with deduped_customer as (         -- depuplicate by customer_id and sort by latest record
  select
        customer_id,
        name,
        address,
        updated_dt,
        hash_key,
        row_number() over(partition by customer_id order by updated_dt desc) as rn
  from source_customer
  ),
-- keep only the leatest records per customer
latest_customer_udates as (
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
        customer_id,
        name,
        address,
        updated_dt,
        hash_key,
        rn,
  from latest_customer_udates lcu
  left join {{ this }} t
        on lcu.customer_id = t.customer_id
        and t.is_current = TRUE
    where t.customer_id is null
      or lcu.hash_key <> t.hash_key
  )
insert into {{ this }} (        -- insert new records with effective start date and is current true, end date is null.
    customer_sk,
    customer_id
    name,
    address,
    hash_key,
    effective_start,
    effective_end,
    is_current
  )
select
    nextval('customer_sk_seq') as customer_sk,
    customer_id,
    name,
    address,
    hash_key,
    updated_dt as effective_start_date,
    null as effective_end_date
    true as is_current,
from changed_customer_records
{% if is_incremental() %}
where updated_dt > (select max(updated_dt) from {{ this }})
{% endif %}
