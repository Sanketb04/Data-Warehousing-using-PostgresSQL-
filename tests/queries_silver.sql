/*
===============================================================================
Queries and Quality Checks
===============================================================================
Script Purpose:
    This script prvides step by step procedure performed in the Silver layer and performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.
===============================================================================
*/


-- Quality Check for Bronze Layer Data

---check for nulls and duplicates

select cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null; 

select *
from bronze.crm_cust_info
where cst_id = 29466;

---Rank the duplicates by last create date
select *,
	row_number() over( partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id = 29466;

select *,
	row_number() over( partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info;

---Check for the duplicates with flag last = 1
select * 
from 
(
select *,
	row_number() over( partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id is not null
)
where flag_last = 1;

---Check for unwanted spaces in columns

select cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

select cst_gndr
from bronze.crm_cust_info
where cst_gndr != TRIM(cst_gndr);

---Trimming the unwanted spaces and updating the data transformation query
select
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	trim(cst_marital_status) as cst_marital_status,
	trim(cst_gndr) as cst_gndr,
	cst_create_date
from 
(
select *,
	row_number() over( partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id is not null
)
where flag_last = 1;

---Check for consistencies in low cardinality columns

select distinct cst_gndr 
from bronze.crm_cust_info;

select distinct cst_marital_status  
from bronze.crm_cust_info;

---Updating the transformation query
select
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case when upper(trim(cst_marital_status)) = 'M' then 'Married'
		 when upper(trim(cst_marital_status)) = 'S' then 'Single'
		 else 'n/a'
	end cst_marital_status,
	case when upper(trim(cst_gndr)) = 'M' then 'Male'
		 when upper(trim(cst_gndr)) = 'F' then 'Female'
		 else 'n/a'
	end cst_gndr,
	cst_create_date
from 
(
select *,
	row_number() over( partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id is not null
)
where flag_last = 1;

--- Insert into Silver Layer

insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
select
	cst_id,
	cst_key,
	
	trim(cst_firstname) as cst_firstname, -- Remove Unwanted Spaces
	trim(cst_lastname) as cst_lastname,
	
	case when upper(trim(cst_marital_status)) = 'M' then 'Married' -- Normalizing the cardinal values
		 when upper(trim(cst_marital_status)) = 'S' then 'Single'
		 else 'n/a'
	end cst_marital_status,
	
	case when upper(trim(cst_gndr)) = 'M' then 'Male'
		 when upper(trim(cst_gndr)) = 'F' then 'Female'
		 else 'n/a'
	end cst_gndr,
	cst_create_date
from 
(
	select *,
		row_number() over( partition by cst_id order by cst_create_date desc) as flag_last -- Recording the most recent value
	from bronze.crm_cust_info
	where cst_id is not null
)
where flag_last = 1;


-- Validate the silver layer table

select * from silver.crm_cust_info;

select cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;  -- Expectation: no result


-- Quality Checks on crm_prd_info table

select * from bronze.crm_prd_info;

-- Check for nulls and duplicates

select prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
having count(prd_id) > 1 or prd_id is null;

-- Create new columns
select 
	prd_id,
	prd_key,
	substring(prd_key, 1, 5) as cat_id,-- create new column for category id to join on category table
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt	
from bronze.crm_prd_info;

select distinct id from bronze.erp_px_cat_g1v2;

select 
	prd_id,
	prd_key,
	replace(substring(prd_key, 1, 5),'-','_') as cat_id, -- replace - with _ in cat id
	substring(prd_key, 7, length(prd_key)) as prd_key,  -- create new column prd key to connect with sales table 
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt	
from bronze.crm_prd_info
where replace(substring(prd_key, 1, 5),'-','_') not in 
(select distinct id from bronze.erp_px_cat_g1v2);

select * from bronze.crm_sales_details;

select 
	prd_id,
	prd_key,
	replace(substring(prd_key, 1, 5),'-','_') as cat_id, -- replace - with _ in cat id
	substring(prd_key, 7, length(prd_key)) as prd_key,  -- create new column prd key to connect with sales table 
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt	
from bronze.crm_prd_info
where substring(prd_key, 7, length(prd_key)) in 
(select sls_prd_key from bronze.crm_sales_details);

-- Check for unwanted spaces

select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm);

-- Check for null or negative numbers

select * from bronze.crm_prd_info;
select prd_cost
from bronze.crm_prd_info
where cast(prd_cost as int) < 0 or cast(prd_cost as int) is null;

-- Updated transformation query
select 
	prd_id,
	replace(substring(prd_key, 1, 5),'-','_') as cat_id, -- replace - with _ in cat id
	substring(prd_key, 7, length(prd_key)) as prd_key,  -- create new column prd key to connect with sales table 
	prd_nm,
	coalesce(prd_cost::int, 0) as prd_cost,
	case when upper(trim(prd_line)) = 'M' then 'Mountain'
		 when upper(trim(prd_line)) = 'T' then 'Touring'
		 when upper(trim(prd_line)) = 'R' then 'Roads'
		 when upper(trim(prd_line)) = 'S' then 'Other Sales'
		 else 'n/a'
	end as prd_line,
	prd_start_dt,
	prd_end_dt	
from bronze.crm_prd_info;

-- Check for dates

select * 
from bronze.crm_prd_info
where prd_start_dt > prd_end_dt;

-- Solve the invalid date issue where end date is earlier than start date
select prd_id, prd_key,prd_nm, prd_cost, prd_line, prd_start_dt,prd_end_dt,
LEAD(prd_start_dt) OVER( partition by prd_key order by prd_end_dt) - 1 as prd_end_date_test
from bronze.crm_prd_info;

-- Updated transformation query

select 
	prd_id,
	replace(substring(prd_key, 1, 5),'-','_') as cat_id, -- extract category id
	substring(prd_key, 7, length(prd_key)) as prd_key,  -- extract product key
	prd_nm,
	coalesce(prd_cost::int, 0) as prd_cost,
	case when upper(trim(prd_line)) = 'M' then 'Mountain'
		 when upper(trim(prd_line)) = 'T' then 'Touring'
		 when upper(trim(prd_line)) = 'R' then 'Roads'
		 when upper(trim(prd_line)) = 'S' then 'Other Sales'
		 else 'n/a'
	end as prd_line, -- map product line
	prd_start_dt,
	LEAD(prd_start_dt) OVER( partition by prd_key order by prd_end_dt) - 1 as prd_end_dt -- Fix end date is less than start date
from bronze.crm_prd_info;

-- validate the silve table
select prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
having count(prd_id) > 1 or prd_id is null;

select * 
from silver.crm_prd_info
where prd_start_dt > prd_end_dt;

select * from silver.crm_prd_info;

-- For sls_sales_details table
select * from bronze.crm_sales_details;

select * from bronze .crm_sales_details
where sls_ord_num != trim(sls_ord_num);

-- Check whether the product key/ customer id in sales details table matches with product key in product table/ cust id in customer table
select * from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info); -- Matching the join keys

select * from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

-- Check for invaild dates
select sls_order_dt from bronze.crm_sales_details
where sls_order_dt <= 0;

select nullif(sls_order_dt, 0) sls_order_dt from bronze.crm_sales_details
where sls_order_dt <= 0 or sls_order_dt > 20500101 or sls_order_dt < 19900101 or length(sls_order_dt::text) !=8;

select * from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

select distinct
sls_sales,sls_quantity, sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price 
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
order by sls_sales,sls_quantity ,sls_price;

-- Transformation Query

select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	-- Handling the invalid dates
	case when sls_order_dt <=0 or length(sls_order_dt::text) != 8 then null
		else TO_DATE(sls_order_dt ::text, 'YYYYMMDD')
	end as sls_order_dt,
	case when sls_ship_dt <=0 or length(sls_ship_dt::text) != 8 then null
		else TO_DATE(sls_ship_dt ::text, 'YYYYMMDD')
	end as sls_ship_dt,
	case when sls_due_dt <=0 or length(sls_due_dt::text) != 8 then null
		else TO_DATE(sls_due_dt ::text, 'YYYYMMDD')
	end as sls_due_dt,
	sls_sales as old_sales,
	case when sls_sales is null or sls_sales <= 0 or sls_sales != abs(sls_price) * sls_quantity
		then abs(sls_price * sls_quantity)
	else sls_sales
	end sls_sales,
	sls_quantity,
	sls_price as old_price,
	case when sls_price is null or sls_price <= 0
		then sls_sales/nullif(sls_quantity,0)
	else sls_price
	end as sls_price
from bronze.crm_sales_details;

-- Quality Checks
select * from silver.crm_sales_details;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'crm_sales_details' AND table_schema = 'silver';

-- For erp_cust_az12 table
select * from bronze.erp_cust_az12
where cid like '%AW00011000';

select * from silver.crm_cust_info
where cst_key like '%NAS%';

-- Check for out of range dates
select *
from bronze.erp_cust_az12
where bdate > current_date;

-- Data Standardization and Consistencies
select distinct gen,
	case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
		when upper(trim(gen)) in ('M','MALE') then 'Male'
		else 'n/a'
	end as gen
from bronze.erp_cust_az12;
-- Transformation Query
select 
	case when cid like 'NAS%' then substring(cid,4,length(cid))
	else cid
	end as cid,
	case when bdate > current_date then null
	else bdate
	end as bdate,
	case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
		when upper(trim(gen)) in ('M','MALE') then 'Male'
		else 'n/a'
	end as gen
from bronze.erp_cust_az12;

-- For erp_loc table

select * from bronze.erp_loc_a101;

select * from silver.crm_cust_info;

select distinct cntry
from bronze.erp_loc_a101;

-- Remove the '-' from erp loc table
select
	replace(cid,'-','') as cid,
	case when upper(trim(cntry)) in ('US','USA','UNITED STATES','UNITED STATES OF AMERICA') then 'United States'
		 when upper(trim(cntry)) = 'DE' then 'Germany'
		 when trim(cntry) = '' or null then 'n/a'
		 else trim(cntry)
	end as cntry	
from bronze.erp_loc_a101;

-- For table erp_px_cat
select * from bronze.erp_px_cat_g1v2;
