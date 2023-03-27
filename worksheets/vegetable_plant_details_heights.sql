CREATE OR REPLACE TABLE VEGETABLE_DETAILS_PLANT_HEIGHT (
    plant_name TEXT(25),
    uom TEXT(1),
    low_end_of_range number(2),
    high_end_of_range number(2)
);

copy into VEGETABLE_DETAILS_PLANT_HEIGHT
from @like_a_window_into_an_s3_bucket
files = ( 'veg_plant_height.csv')
file_format = ( format_name=COMMASEP_DBLQUOT_ONEHEADROW );

select * from VEGETABLE_DETAILS_PLANT_HEIGHT;
