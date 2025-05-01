
-- select all rows from raw_customer source data
with source_customer as (
select *, md5(coalesce(name, ' ') || coalesce(
