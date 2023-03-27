create database if not exists ZENAS_ATHLEISURE_DB;

drop schema ZENAS_ATHLEISURE_DB.public;

create schema PRODUCTS;

list @uni_klaus_clothing;

list @uni_klaus_zmd;

list @uni_klaus_sneakers ;

select $1
from @uni_klaus_zmd; 

select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt;

create or replace file format zmd_file_format_1
RECORD_DELIMITER = ';';

create or replace file format zmd_file_format_2
FIELD_DELIMITER = '|'
RECORD_DELIMITER = ';'
TRIM_SPACE = True;


select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_1 );

select $1,$2,$3
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_2 );

create file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^';

select $1 as PRODUCT_CODE,$2 as HAS_MATCHING_SWEATSUIT
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_3 );


select trim($1) as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 );

select trim(regexp_replace($1, '\\s+', ' ')) as PRODUCT_CODE, 
$2 as HEADBAND_DESCRIPTION, 
$3 as WRISTBAND_DESCRIPTION
from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_2 );

create or replace view zenas_athleisure_db.products.sweatsuit_sizes as 
select trim(regexp_replace($1, '\\s+', ' ')) as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 )
where sizes_available <> '' or sizes_available is null ;

select * from zenas_athleisure_db.products.sweatsuit_sizes;


create or replace view zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE as
select trim(regexp_replace($1, '\\s+', ' ')) as PRODUCT_CODE, 
$2 as HEADBAND_DESCRIPTION, 
$3 as WRISTBAND_DESCRIPTION
from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_2 );

select * from zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE;

create or replace view zenas_athleisure_db.products.SWEATBAND_COORDINATION as 
select $1 as PRODUCT_CODE,$2 as HAS_MATCHING_SWEATSUIT
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_3 );

select * from zenas_athleisure_db.products.SWEATBAND_COORDINATION;

select $1
from @uni_klaus_clothing/90s_tracksuit.png; 

select metadata$filename, max(metadata$file_row_number) as NUMBER_OF_ROWS
from @uni_klaus_clothing
group by metadata$filename;


select * from directory(@uni_klaus_clothing);

-- Oh Yeah! We have to turn them on, first
alter stage uni_klaus_clothing 
set directory = (enable = true);

--Now?
select * from directory(@uni_klaus_clothing);

--Oh Yeah! Then we have to refresh the directory table!
alter stage uni_klaus_clothing refresh;

--Now?
select * from directory(@uni_klaus_clothing);

select UPPER(RELATIVE_PATH) as uppercase_filename
, REPLACE(uppercase_filename,'/') as no_slash_filename
, REPLACE(no_slash_filename,'_',' ') as no_underscores_filename
, REPLACE(no_underscores_filename,'.PNG') as just_words_filename
from directory(@uni_klaus_clothing);


create or replace TABLE ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS (
	COLOR_OR_STYLE VARCHAR(25),
	DIRECT_URL VARCHAR(200),
	PRICE NUMBER(5,2)
);

insert into  ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS 
          (COLOR_OR_STYLE, DIRECT_URL, PRICE)
values
('90s', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/90s_tracksuit.png',500)
,('Burgundy', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/forest_green_sweatsuit.png',65)
,('Navy Blue', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/navy_blue_sweatsuit.png',65)
,('Orange', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/orange_sweatsuit.png',65)
,('Pink', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/pink_sweatsuit.png',65)
,('Purple', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/purple_sweatsuit.png',65)
,('Red', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/red_sweatsuit.png',65)
,('Royal Blue',	'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/royal_blue_sweatsuit.png',65)
,('Yellow', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/yellow_sweatsuit.png',65);


select * from directory(@uni_klaus_clothing);

select * from sweatsuits;

create or replace view catalog as (
select 
COLOR_OR_STYLE,
direct_url,
price,
size as image_size,
last_modified as image_last_modified
, sizes_available
from directory(@uni_klaus_clothing) as dir
left join sweatsuits as sw
on dir.relative_path = replace(sw.direct_url, 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/', '/')
cross join sweatsuit_sizes);

select count(*) from catalog;

-- Add a table to map the sweat suits to the sweat band sets
create table ZENAS_ATHLEISURE_DB.PRODUCTS.UPSELL_MAPPING
(
SWEATSUIT_COLOR_OR_STYLE varchar(25)
,UPSELL_PRODUCT_CODE varchar(10)
);


--populate the upsell table
insert into ZENAS_ATHLEISURE_DB.PRODUCTS.UPSELL_MAPPING
(
SWEATSUIT_COLOR_OR_STYLE
,UPSELL_PRODUCT_CODE 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');
-- Zena needs a single view she can query for her website prototype
create or replace view catalog_for_website as 
select color_or_style
,price
,direct_url
,size_list
,coalesce('BONUS: ' ||  headband_description || ' & ' || wristband_description, 'Consider White, Black or Grey Sweat Accessories')  as upsell_product_desc
from
(   select color_or_style, price, direct_url, image_last_modified,image_size
    ,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, direct_url, image_last_modified, image_size
) c
left join upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style
left join sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
on spl.product_code = sc.product_code
where price < 200 -- high priced items like vintage sweatsuits aren't a good fit for this website
and image_size < 1000000 -- large images need to be processed to a smaller size
;

select * from catalog_for_website;
