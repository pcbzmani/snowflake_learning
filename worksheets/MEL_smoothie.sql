use role sysadmin;
create database if not exists MELS_SMOOTHIE_CHALLENGE_DB;
drop schema if exists public;
create schema if not exists TRAILS;


create or replace stage TRAILS_PARQUET
    url = 's3://uni-lab-files-more/dlkw/trails/trails_parquet';

create or replace stage TRAILS_GEOJSON
    url = 's3://uni-lab-files-more/dlkw/trails/trails_geojson';

list @trails_geojson;

list @trails_parquet;

create OR REPLACE file format FF_JSON 
TYPE = JSON;


 create OR REPLACE file format FF_PARQUET 
    TYPE = PARQUET;

select $1
from @TRAILS_GEOJSON
(file_format => FF_JSON);

select $1
from @TRAILS_PARQUET
(file_format => FF_PARQUET);

select 
$1:sequence_1 as sequence_1,
$1:trail_name::varchar as trail_name,
$1:latitude::number(11,8) as lng,
$1:longitude::number(11,8) as lat,
$1:sequence_2 as sequence_2,
$1:elevation as elevation
from @TRAILS_PARQUET
(file_format => FF_PARQUET)
order by sequence_1;

create or replace view CHERRY_CREEK_TRAIL as (
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng, --remember we did a gut check on this data
 $1:longitude::number(11,8) as lat
from @trails_parquet
(file_format => ff_parquet)
order by point_id);

select top 100
lng || ' ' || lat as coord_pair,
'POINT(' || coord_pair || ')' as trail_point
from CHERRY_CREEK_TRAIL;

create or replace view cherry_creek_trail as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng,
 $1:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;


select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
where point_id <= 10
group by trail_name;

create or replace view DENVER_AREA_TRAILS as (
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json)
);


select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
,TO_GEOGRAPHY(my_linestring) as length_of_trail --this line is new! but it won't work!
from cherry_creek_trail
group by trail_name;

select feature_name,
st_length(TO_GEOGRAPHY(GEOMETRY)) as trail_length
from denver_area_trails;

select get_ddl('view', 'DENVER_AREA_TRAILS');

create or replace view DENVER_AREA_TRAILS(
	FEATURE_NAME,
	FEATURE_COORDINATES,
	GEOMETRY,
    TRAIL_LENGTH,
	FEATURE_PROPERTIES,
	SPECS,
	WHOLE_OBJECT
) as (
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,st_length(TO_GEOGRAPHY(geometry)) as trail_length
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json)
);

select * from DENVER_AREA_TRAILS;

--So let's make it look like geoJSON instead.
create view DENVER_AREA_TRAILS_2 as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',')||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry)) as trail_length
from cherry_creek_trail
group by trail_name;

select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS_2;
create view TRAILS_AND_BOUNDARIES as (
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2);


select min(min_eastwest) as western_edge,
min(min_northsouth) as southern_edge,
max(min_eastwest) as eastern_edge,
max(min_eastwest) as northern_edge
from  TRAILS_AND_BOUNDARIES;


-- Melanie's Location into a 2 Variables (mc for melanies cafe)
set mc_lat='-104.97300245114094';
set mc_lng='39.76471253574085';

--Confluence Park into a Variable (loc for location)
set loc_lat='-105.00840763333615'; 
set loc_lng='39.754141917497826';

--Test your variables to see if they work with the Makepoint function
select st_makepoint($mc_lat,$mc_lng) as melanies_cafe_point;
select st_makepoint($loc_lat,$loc_lng) as confluent_park_point;

--use the variables to calculate the distance from 
--Melanie's Cafe to Confluent Park
select st_distance(
        st_makepoint($mc_lat,$mc_lng)
        ,st_makepoint($loc_lat,$loc_lng)
        ) as mc_to_cp;

CREATE OR REPLACE FUNCTION distance_to_mc(loc_lat number(38,32), loc_lng number(38,32))
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,st_makepoint(loc_lat,loc_lng)
        )
  $$
  ;

  --Tivoli Center into the variables 
set tc_lat='-105.00532059763648'; 
set tc_lng='39.74548137398218';

select distance_to_mc($tc_lat,$tc_lng);

create or replace schema locations;

create view COMPETITION  as (
select * 
from SONRA_DENVER_CO_USA_FREE.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%'));

SELECT
 name
 ,cuisine
 , ST_DISTANCE(
    st_makepoint('-104.97300245114094','39.76471253574085')
    , coordinates
  ) AS distance_from_melanies
 ,*
FROM  competition
ORDER by distance_from_melanies;

CREATE OR REPLACE FUNCTION distance_to_mc(lat_and_lng GEOGRAPHY)
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,lat_and_lng
        )
  $$
  ;

  
SELECT
 name
 ,cuisine
 ,distance_to_mc(coordinates) AS distance_from_melanies
 ,*
FROM  competition
ORDER by distance_from_melanies;


set tcb_lat='-104.9956203'; 
set tcb_lng='39.754874';

--this will run the first version of the UDF
select distance_to_mc($tcb_lat,$tcb_lng);

--this will run the second version of the UDF, bc it converts the coords 
--to a geography object before passing them into the function
select distance_to_mc(st_makepoint($tcb_lat,$tcb_lng));

--this will run the second version bc the Sonra Coordinates column
-- contains geography objects already
select name
, distance_to_mc(coordinates) as distance_to_melanies 
, ST_ASWKT(coordinates)
from SONRA_DENVER_CO_USA_FREE.DENVER.V_OSM_DEN_SHOP
where shop='books' 
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';

create or replace view DENVER_BIKE_SHOPS as (
select name,
distance_to_mc(coordinates) as distance_to_melanies from 
SONRA_DENVER_CO_USA_FREE.DENVER.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES
where shop = 'bicycle');

select * from DENVER_BIKE_SHOPS;

create or replace external table T_CHERRY_CREEK_TRAIL(
	my_filename varchar(50) as (metadata$filename::varchar(50))
) 
location= @trails_parquet
auto_refresh = true
file_format = (type = parquet);

select get_ddl('view','mels_smoothie_challenge_db.trails.v_cherry_creek_trail');

create or replace external table mels_smoothie_challenge_db.trails.T_CHERRY_CREEK_TRAIL(
	POINT_ID number as ($1:sequence_1::number),
	TRAIL_NAME varchar(50) as  ($1:trail_name::varchar),
	LNG number(11,8) as ($1:latitude::number(11,8)),
	LAT number(11,8) as ($1:longitude::number(11,8)),
	COORD_PAIR varchar(50) as (lng::varchar||' '||lat::varchar)
) 
location= @mels_smoothie_challenge_db.trails.trails_parquet
auto_refresh = true
file_format = mels_smoothie_challenge_db.trails.ff_parquet;


create secure materialized view SMV_CHERRY_CREEK_TRAIL
    as select * from T_CHERRY_CREEK_TRAIL;

    select count(*) from SMV_CHERRY_CREEK_TRAIL;
