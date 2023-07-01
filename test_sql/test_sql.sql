CREATE EXTENSION IF NOT EXISTS pxf;

CREATE SCHEMA IF NOT EXISTS test_part;
DROP table IF EXISTS test_part.sales_test;
CREATE TABLE test_part.sales_test (id int, date date, amt decimal(10,2))
DISTRIBUTED BY (id)
PARTITION BY RANGE (date) (DEFAULT PARTITION other);

INSERT INTO test_part.sales_test (id, "date", amt)
with test as(
select
    generate_series('2023-01-01'::date - '5 year'::interval, '2023-06-12'::date, '1 day'::interval) as date
)
select
    to_char(date, 'YYYYMMDD')::integer as id
    , date
    , (
        random() * 1000
      )::int + 1 as amt
from
    test;