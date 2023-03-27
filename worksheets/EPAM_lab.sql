create database if not exists EPAM_LAB;
drop schema if exists public;

use EPAM_LAB;
create or replace schema CORE_DWH;
create or replace schema DATA_MART;
create or replace schema tpch;


create or replace table tpch.region
(
  r_regionkey INTEGER,
  r_name      CHAR(25),
  r_comment   VARCHAR(152)
);



create or replace table tpch.nation
(
  n_nationkey INTEGER not null,
  n_name      CHAR(27),
  n_regionkey INTEGER,
  n_comment   VARCHAR(155)
);



create or replace table tpch.supplier
(
  s_suppkey   INTEGER not null,
  s_name      CHAR(25),
  s_address   VARCHAR(40),
  s_nationkey INTEGER,
  s_phone     CHAR(15),
  s_acctbal   FLOAT8,
  s_comment   VARCHAR(101)
);



create or replace table tpch.orders
(
  o_orderkey      INTEGER not null,
  o_custkey       INTEGER not null,
  o_orderstatus   CHAR(1),
  o_totalprice    FLOAT8,
  o_orderdate     DATE,
  o_orderpriority CHAR(15),
  o_clerk         CHAR(15),
  o_shippriority  INTEGER,
  o_comment       VARCHAR(79)
);



create or replace table tpch.partsupp
(
  ps_partkey    INTEGER not null,
  ps_suppkey    INTEGER not null,
  ps_availqty   INTEGER,
  ps_supplycost FLOAT8 not null,
  ps_comment    VARCHAR(199)
);


create or replace table tpch.part
(
  p_partkey     INTEGER not null,
  p_name        VARCHAR(55),
  p_mfgr        CHAR(25),
  p_brand       CHAR(10),
  p_type        VARCHAR(25),
  p_size        INTEGER,
  p_container   CHAR(10),
  p_retailprice INTEGER,
  p_comment     VARCHAR(23)
);



create or replace table tpch.customer
(
  c_custkey    INTEGER not null,
  c_name       VARCHAR(25),
  c_address    VARCHAR(40),
  c_nationkey  INTEGER,
  c_phone      CHAR(15),
  c_acctbal    FLOAT8,
  c_mktsegment CHAR(10),
  c_comment    VARCHAR(117)
);



create or replace  table tpch.lineitem
(
  l_orderkey      INTEGER not null,
  l_partkey       INTEGER not null,
  l_suppkey       INTEGER not null,
  l_linenumber    INTEGER not null,
  l_quantity      INTEGER not null,
  l_extendedprice FLOAT8 not null,
  l_discount      FLOAT8 not null,
  l_tax           FLOAT8 not null,
  l_returnflag    CHAR(1),
  l_linestatus    CHAR(1),
  l_shipdate      DATE,
  l_commitdate    DATE,
  l_receiptdate   DATE,
  l_shipinstruct  CHAR(25),
  l_shipmode      CHAR(10),
  l_comment       VARCHAR(44)
);

select * from region;

use role accountadmin;
SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();



CREATE STORAGE INTEGRATION azure_integrat
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'b6ab74e8-392b-4b26-8973-6bb4448630b5'
  STORAGE_ALLOWED_LOCATIONS = ('azure://epamlabsnowflake.blob.core.windows.net/snowflake/');

  DESC STORAGE INTEGRATION azure_integrat;

  CREATE STAGE my_azure_stage
  STORAGE_INTEGRATION = azure_integrat
  URL = 'azure://epamlabsnowflake.blob.core.windows.net/snowflake/';

  list @my_azure_stage;
-- azure://epamlabsnowflake.blob.core.windows.net/snowflake/h_customer.dsv
-- azure://epamlabsnowflake.blob.core.windows.net/snowflake/h_lineitem.zip


CREATE  OR REPLACE  FILE FORMAT dsv
  TYPE = CSV
  FIELD_DELIMITER = '|'
  ESCAPE = '\"'
  TRIM_SPACE = TRUE
  SKIP_HEADER = 1
  ;

  CREATE  OR REPLACE  FILE FORMAT csv
  TYPE = CSV
  FIELD_DELIMITER = ','
  ESCAPE = '\"'
  TRIM_SPACE = TRUE
  SKIP_HEADER = 1
  ;

  select $1 as l_orderkey,
  $2 as l_partkey,
  $3 as l_suppkey,
  $4 as l_linenumber,
  $5 as l_quantity,
  replace($6,',','') as l_extendedprice,
  replace($7,',','') as l_discount,
  replace($8,',','') as l_tax,
  replace($9,'"','') as l_returnflag,
  replace($10,'"','') as l_linestatus,
  TO_DATE($11, 'DD.MM.YY') as l_shipdate,
  TO_DATE($12, 'DD.MM.YY') as l_commitdate,
  TO_DATE($13, 'DD.MM.YY') as l_receiptdate,
   trim(replace($14,'"','')) as l_shipinstruct,
   trim(replace($15,'"','')) as l_shipmode,
   trim(replace($16,'"','')) as l_comment
   from @my_azure_stage/data_3fdc290e-7fe4-44df-8267-e96020f70564_4332090c-be5b-49d3-8469-80cee2fc776d.dsv
  (file_format => dsv )
  limit 100;

  copy into tpch.lineitem
  from (
  select $1 as l_orderkey,
  $2 as l_partkey,
  $3 as l_suppkey,
  $4 as l_linenumber,
  $5 as l_quantity,
  replace($6,',','') as l_extendedprice,
  replace($7,',','') as l_discount,
  replace($8,',','') as l_tax,
  replace($9,'"','') as l_returnflag,
  replace($10,'"','') as l_linestatus,
 TO_DATE($11, 'DD.MM.YY') as l_shipdate,
  TO_DATE($12, 'DD.MM.YY') as l_commitdate,
  TO_DATE($13, 'DD.MM.YY') as l_receiptdate,
   trim(replace($14,'"','')) as l_shipinstruct,
   trim(replace($15,'"','')) as l_shipmode,
   trim(replace($16,'"','')) as l_comment
   from @my_azure_stage/data_3fdc290e-7fe4-44df-8267-e96020f70564_4332090c-be5b-49d3-8469-80cee2fc776d.dsv
  (file_format => dsv )
  );

  select * from tpch.lineitem
  LIMIT 10;

copy into tpch.customer
from (
select 
$1  as c_custkey,
replace($2,'"','') as c_name,
replace($3,'"','') as c_address,
$4 as c_nationkey,
trim(replace($5,'"','')) as c_phone,
replace($6,',','') as c_acctbal,
trim(replace($7,'"','')) as c_mktsegment,
replace($8,'"','') as c_comment
from @my_azure_stage/h_customer.dsv
(file_format => dsv ));

select * from customer;

copy into tpch.region
from (
select 
$1 as r_regionkey,
trim(replace($2,'"','')) as r_name,
replace($3,'"','') as r_comment
from @my_azure_stage/h_region.csv
(file_format => csv ));
  
  select * from tpch.region;
  
copy into tpch.nation
from (
select 
$1 as n_nationkey,
replace($2,'"','') as n_name,
$3 as n_regionkey,
replace($4,'"','') as n_comment
from @my_azure_stage/h_nation.dsv
(file_format => dsv));

select * from tpch.nation;

list @my_azure_stage;

copy into tpch.supplier
from (
select 
$1 as s_suppkey,
trim(replace($2,'"','')) as s_name,
replace($3,'"','') as s_address,
$4 as s_nationkey,
trim(replace($5,'"','')) as s_phone,
replace($6,',','') as s_acctbal,
replace($7,'"','') as s_comment
from @my_azure_stage/h_supplier.dsv
(file_format => dsv));

copy into tpch.orders
from (
select 
$1 as o_orderkey,
$2 as o_custkey,
replace($3,'"','') as o_orderstatus,
replace($4,',','') as o_totalprice,
to_date($5,'DD.MM.YY') as o_orderdate,
trim(replace($6,'"','')) as o_orderpriority,
replace($7,'"','') as o_clerk,
$8 as o_shippriority,
replace($9,'"','') as o_comment
from @my_azure_stage/h_order.dsv
(file_format => dsv));

list @my_azure_stage;

copy into tpch.partsupp
from (
select 
  $1 as ps_partkey, 
  $2 as ps_suppkey,
  $3 as ps_availqty , 
  replace($4,',','') as ps_supplycost,
  replace($5,'"','') as ps_comment 
  from @my_azure_stage/h_partsupp.dsv
(file_format => dsv));

copy into tpch.part
from (
select 
  $1 as p_partkey,
  replace($2,'"','') as p_name,
  trim(replace($3,'"','')) as p_mfgr,
  trim(replace($4,'"','')) as p_brand,
  trim(replace($5,'"','')) as p_type,
  $6 as p_size,
  trim(replace($7,'"','')) as p_container,
  $8 as p_retailprice,
  trim(replace($9,'"','')) as p_comment
  from @my_azure_stage/h_part.dsv
(file_format => dsv));

select 'lineitem' as tbl, count(*) as count  from tpch.lineitem
union
select 'orders' as tbl, count(*) as count from tpch.orders
union
select 'partsupp' as tbl, count(*) as count from tpch.partsupp
union
select 'part' as tbl, count(*) as count from tpch.part
union
select 'customer' as tbl, count(*) as count from tpch.customer
union
select 'supplier' as tbl, count(*) as count from tpch.supplier
union
select 'nation' as tbl, count(*) as count from tpch.nation
union
select 'region' as tbl, count(*) as count from tpch.region;

use role sysadmin;
create schema if not exists CORE_DWH;
