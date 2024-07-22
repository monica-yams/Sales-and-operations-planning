------STEP 1: Get data from the actual source
with SRC as
(
Select distinct
COL1,---(assume this is an id column)
COL2,
COL3,
COL4,
COL5
FROM XXXX
WHERE COL1 = YOUR CONDITION
)

select
   HASHBYTES('SHA2_512', CONCAT(COL1,COL2,COL3,COL4,COL5)) as hash_value,
   COL1,
   COL2,
   COL3,
   COL4,
   COL5
from SRC;

---This data should to inserted or sinked into stg table ( truncate stg table before inserting )

------STEP 2: Instert data from stg table to history table using a left join. 

with hist_data as (
    select *,
        row_number() over(partition by ID order by elt_modified_date desc) as row_num 
    from hist_table
)

-- Insert new data into history
insert into hist_table
select  
    stg.hash_value,
	stg.COL1,
    getdate() as elt_modified_date
from stg_table  stg 
left join (
	select COL1, hash_value from hist_data where row_num = 1
) hist on hist.hash_value = stg.hash_value
where hist.hash_value is null;

-- STEP 3: Retrieve the delta set
select
    stg.COL1, 
    stg.COL2, 
    stg.COL3, 
    stg.COL3,
    stg.COL4
from stg_table stg 
join hist_table hist on hist.hash_value = stg.hash_value
where hist.elt_modified_date > '@{variables('last_execution_datetime')}' 
order by stg.COL1;

