--post hook macro for dim_customer incremental update

{% macro update_dim_customer(dim_customer) %}
update {{ this }} t
set
  t.is_current = false,
  t.effective_end = ccr.updated_dt
from {{ ref('stg_customer') }} ccr
where t.customer_id = ccr.customer_id
and t.is_current = TRUE;
and md5(coalesce(t.name, '') || coalesce(t.address, '')) <> ccr.hash_key
and cr.updated_dt > coalesce(t.effective_start, '1900-01-01')

