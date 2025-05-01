create schema if not exists raw_schema_customer

create table raw_schema_customer.raw_customer(
  customer_id int primary key,
  name text,
  address text,
  update_dt timestamp
);
