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

   
/*test GP*/   
select count(*), sum(st.amt)
from test_part.sales_test st 
where "date" >= '2020-01-01'::date  and "date" <= '2021-01-01'::date


/*click*/
--DROP DATABASE transfer_ods;
--DROP DATABASE transfer_stage_s3;
select count(*), sum(st.amt)
from transfer_ods.sale_test st 
where st.`date` >= toDate('2020-01-01') and st.`date` <= toDate('2021-01-01')