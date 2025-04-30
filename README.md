# How do you implement an incremental model in dbt for a customer dimension

An incremental model in dbt is a model that only processes new or changed data instead of rebuilding the whole table every time.

* Normal model:
-> Drop everything -> Rebuild the entire table from strach, every time you run dbt.

* Incremental Model: -> Keep the existing table -> only add new rows(or changed rows) to save time.

__Example situation:__

Imagine you have a huge events table with 10 million rows. Every day, 10,000 new events are added. Without incremental: Every time you run dbt, it would reprocess all 10 million rows — slow and expensive. With incremental: dbt just processes the 10,000 new rows — fast and efficient.

To implement an incremental model in dbt, you mainly do three things:

Use materialized='incremental' in your model config. tell dbt: "This model should be built incrementally."
Add new logic to is_incremental() function only for incremental runs
Make sure your table has a good "filter column" (like a timestamp or an auto-incrementing ID). Without that, dbt won't know how to safely add new data without duplicates or gaps.

```sql

{{ config(
    materialized='incremental', --tells dbt this is incremental model
    unique_key = ['order_id', 'customer_id'] -- <- this makes it an upsert!
) }}

select
  order_id,
  customer_id,
  status,
  updated_at
from {{ ref('stg_orders') }}
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }}) -- this reference the current version of this model
{% endif %}

```

## How would you model a Type 2 Slowly Changing Dimension (SCD) in dbt using SQL Server

__Provide the incremental logic:__

```sql

-- 1. Select all raw source data
WITH source_data AS (
    SELECT *,
           md5(coalesce(name, '') || coalesce(address, '')) AS hash_key
    FROM {{ source('raw', 'customers') }}
),

-- 2. Deduplicate by customer_id, keeping the most recent update
deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_dt DESC) AS rn
    FROM source_data
),

-- 3. Keep only the latest records per customer
latest_updates AS (
    SELECT * FROM deduped WHERE rn = 1
),

-- 4. Identify changes based on hash_key comparison
changed_records AS (
    SELECT lu.*
    FROM latest_updates lu
    LEFT JOIN {{ this }} t
      ON lu.customer_id = t.customer_id AND t.is_current = TRUE
    WHERE t.customer_id IS NULL
       OR lu.hash_key <> t.hash_key
)

-- INSERT new versioned records
INSERT INTO {{ this }} (
    customer_sk,
    customer_id,
    name,
    address,
    hash_key,
    effective_start,
    effective_end,
    is_current
)
SELECT
    nextval('customer_sk_seq') AS customer_sk,
    customer_id,
    name,
    address,
    hash_key,
    updated_dt AS effective_start,
    NULL AS effective_end,
    TRUE AS is_current
FROM changed_records;

-- UPDATE previous versions to mark as not current
UPDATE {{ this }} t
SET
    is_current = FALSE,
    effective_end = cr.updated_dt
FROM changed_records cr
WHERE t.customer_id = cr.customer_id
  AND t.is_current = TRUE;

-- Optional: filter for incremental processing only
{% if is_incremental() %}
  AND latest_updates.updated_dt > (SELECT MAX(updated_dt) FROM {{ this }})
{% endif %}

```


## What happens during dbt run, dbt test and dbt build

* dbt run → builds models

* dbt test → runs tests

* dbt build → runs models + tests + snapshots + seeds (everything)

## What types of materializations are there in dbt

Materialization Meaning view Just a SQL view (no storage) table A physical table (refreshes fully) incremental Adds only new/changed data ephemeral Temporary CTE, no table created.

## How would you optimize an incremental model for late-arriving data

To optimize an incremental model for late-arriving data, you can focus on ensuring that your incremental logic accounts for records that arrive after the main batch of data has already been processed.

incremental() with a timestamp: Ensure that your incremental model uses a timestamp column (e.g., updated_at) for tracking changes or new records. Make sure your filter checks the most recent records:

```sql

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}

```

This ensures that only the "new or updated" records are processed.

Handling late-arriving data: For late-arriving data that could be overwritten (i.e., records with the same primary key), consider using an upsert pattern. Use unique_key with an incremental model to update rows instead of just inserting new ones.

Change data capture (CDC): If your source system supports it, implement CDC (capturing inserts/updates/deletes). You can track changes using inserted_at or updated_at fields.

## What is a snapshot in dbt

Snapshots let you track changes over time in slowly changing tables (SCD Type 2).
