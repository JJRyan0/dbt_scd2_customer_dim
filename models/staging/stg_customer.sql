
-- select all rows from raw_customer source data
with source_customer as (
select customer_id
        name,
        address,
        updated_dt,
        md5(coalesce(name, '') || coalesce(address, '')) as hash_key
from {{ source('raw_schema_customer', 'raw_customer') }}
)

