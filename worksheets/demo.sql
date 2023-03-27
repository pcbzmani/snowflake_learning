show schemas in account;

select current_region();

select current_account();

set mystery_bag = 'This is empty bag';

select $mystery_bag;

set var1 = 2;
set var2 = 4;

select $var1 + $var2;
-- drop function if exists sum_mystery_bag_vars(number,number,number);

create function sum_mystery_bag_vars(var1 number, var2 number, var3 number)
    returns number as 'select var1+var2+var3';

select sum_mystery_bag_vars(5,5,5);

-- Set these local variables according to the instructions
set this = -10.5;
set that = 2;
set the_other = 1000 ;



-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW04' as step
 ,( select demo_db.public.sum_mystery_bag_vars($this,$that,$the_other)) as actual
 , 991.5 as expected
 ,'Mystery Bag Function Output' as description
);


set alternating_caps_pharse = 'aLtErNaTiNg CaPs!';

select initcap($alternating_caps_pharse);


create function NEUTRALIZE_WHINING(var text)
    returns text as 'select initcap(var)';

select NEUTRALIZE_WHINING('why tHis is');

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DABW05' as step
 ,( select hash(neutralize_whining('bUt mOm i wAsHeD tHe dIsHes yEsTeRdAy'))) as actual
 , -4759027801154767056 as expected
 ,'WHINGE UDF Works' as description
);


show stages in account;

list @my_internal_named_stage;

select $1 from @my_internal_named_stage/my_file.txt.gz;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW06' as step
 ,( select count(distinct METADATA$FILENAME) 
   from @demo_db.public.my_internal_named_stage) as actual
 , 3 as expected
 ,'I PUT 3 files!' as description
);


use role accountadmin;

select demo_db.public.grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123 ) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 
