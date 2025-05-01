-- incremental model for dim customers
-- target table created by dbt will be analytics.dim_customers

{{ config(materialized ='incremental') }}

with deduped as (
  select
        customer_id,
        name,
        address,
        updated_dt,
        hash_key,
        row_number() over(partition by customer_id order by updated_dt desc) as rn
  from 
