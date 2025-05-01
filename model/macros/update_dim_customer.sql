--post hook macro for dim_customer incremental update

{% macro update_dim_customer(this) %}
update {{ this }} as t
set
  is_current = false,
  effective_end = ccr.updated_dt
from {{ source('raw_customer', 'raw_customer') }} as ccr
where t.customer_id = ccr.customer_id
and t.is_current = TRUE
and md5(coalesce(t.name, '') || coalesce(t.address, '')) <> md5(coalesce(ccr.name, '') || coalesce(ccr.address, ''))  -- Recreate hash_key logic
and ccr.updated_dt > coalesce(t.effective_start, '1900-01-01')
{% endmacro %}
