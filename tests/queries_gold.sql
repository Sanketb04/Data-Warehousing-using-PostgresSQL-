/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script provides step by step procedure in the gold layer and performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.
===============================================================================
*/
select *
from silver.crm_cust_info; --Master Customer Table

select *
from silver.erp_cust_az12; -- Customer Birthdate table

select *
from silver.erp_loc_a101; -- Customer location Table



-- Aggregate the customer table columns needed in gold layer

select 
	cci.cst_id,
	cci.cst_key,
	cci.cst_firstname,
	cci.cst_lastname,
	cci.cst_marital_status,
	cci.cst_gndr,
	cci.cst_create_date,
	eca.bdate,
	eca.gen,
	ela.cntry
from silver.crm_cust_info cci
left join silver.erp_cust_az12 eca
on cci.cst_key = eca.cid
left join silver.erp_loc_a101 ela
on cci.cst_key = ela.cid;

-- Check for duplicates (No duplicates shoould be there)
select cst_id, count(*) from
(
select 
	cci.cst_id,
	cci.cst_key,
	cci.cst_firstname,
	cci.cst_lastname,
	cci.cst_marital_status,
	cci.cst_gndr,
	cci.cst_create_date,
	eca.bdate,
	eca.gen,
	ela.cntry
from silver.crm_cust_info cci
left join silver.erp_cust_az12 eca
on cci.cst_key = eca.cid
left join silver.erp_loc_a101 ela
on cci.cst_key = ela.cid
)t group by cst_id
having count(*) > 1;

-- Check for gender columns
select distinct
	cci.cst_gndr,
	eca.gen
from silver.crm_cust_info cci
left join silver.erp_cust_az12 eca
on cci.cst_key = eca.cid
left join silver.erp_loc_a101 ela
on cci.cst_key = ela.cid
order by 1,2;

-- Fixing the gender mismatch
select distinct
	cci.cst_gndr,
	eca.gen,
	case when cci.cst_gndr !='n/a' then cci.cst_gndr -- Take the gender from CRM
		 else coalesce(eca.gen,'n/a')
	end as new_gen
from silver.crm_cust_info cci
left join silver.erp_cust_az12 eca
on cci.cst_key = eca.cid
left join silver.erp_loc_a101 ela
on cci.cst_key = ela.cid
order by 1,2;

-- Updated aggregation query for customer table

select 
	row_number() over(order by cci.cst_id) as customer_key, --Generated Surroagate key to use as primary key
	cci.cst_id as customer_id,
	cci.cst_key as customer_number,
	cci.cst_firstname as first_name,
	cci.cst_lastname as last_name,
	ela.cntry as country,
	cci.cst_marital_status as marital_status,
	case when cci.cst_gndr !='n/a' then cci.cst_gndr -- Take the gender from CRM
		 else coalesce(eca.gen,'n/a')
	end as gender,
	eca.bdate as birthdate,
	cci.cst_create_date as create_date
from silver.crm_cust_info cci
left join silver.erp_cust_az12 eca
on cci.cst_key = eca.cid
left join silver.erp_loc_a101 ela
on cci.cst_key = ela.cid;

-- Create the view in the gold layer
create view gold.dim_customers as
select 
	row_number() over(order by cci.cst_id) as customer_key, --Generated Surroagate key to use as primary key
	cci.cst_id as customer_id,
	cci.cst_key as customer_number,
	cci.cst_firstname as first_name,
	cci.cst_lastname as last_name,
	ela.cntry as country,
	cci.cst_marital_status as marital_status,
	case when cci.cst_gndr !='n/a' then cci.cst_gndr -- Take the gender from CRM
		 else coalesce(eca.gen,'n/a')
	end as gender,
	eca.bdate as birthdate,
	cci.cst_create_date as create_date
from silver.crm_cust_info cci
left join silver.erp_cust_az12 eca
on cci.cst_key = eca.cid
left join silver.erp_loc_a101 ela
on cci.cst_key = ela.cid;

--Validation
select * from gold.dim_customers;
select distinct gender from gold.dim_customers;

-- Aggregation for Product Table columns

select * from silver.crm_prd_info;
select * from silver.erp_px_cat_g1v2;

select 
	cpi.prd_id,
	cpi.prd_key,
	cpi.prd_nm,
	cpi.prd_cost,
	cpi.prd_line,
	cpi.cat_id,
	cpi.prd_start_dt,
	cpi.prd_end_dt,
	epc.cat,
	epc.subcat,
	epc.maintenance
from silver.crm_prd_info cpi
left join silver.erp_px_cat_g1v2 epc
on cpi.cat_id = epc.id
where cpi.prd_end_dt is null; -- Keeping only current products by filtering historical data

-- Check duplicates
select prd_id, count(*) from
(
select 
	cpi.prd_id,
	cpi.prd_key,
	cpi.prd_nm,
	cpi.prd_cost,
	cpi.prd_line,
	cpi.cat_id,
	cpi.prd_start_dt,
	cpi.prd_end_dt,
	epc.cat,
	epc.subcat,
	epc.maintenance
from silver.crm_prd_info cpi
left join silver.erp_px_cat_g1v2 epc
on cpi.cat_id = epc.id
where cpi.prd_end_dt is null
)t group by prd_id
having count(*) > 1;

-- Updated aggreation query for product columns

select
	row_number() over(order by cpi.prd_start_dt, cpi.prd_key) as product_key, -- Surrogate key
	cpi.prd_id as product_id,
	cpi.prd_key as product_number,
	cpi.prd_nm as product_name,
	cpi.cat_id as category_id,
	epc.cat as category,
	epc.subcat as subcategory,
	epc.maintenance,
	cpi.prd_cost as cost,
	cpi.prd_line as product_line,
	cpi.prd_start_dt as start_date
from silver.crm_prd_info cpi
left join silver.erp_px_cat_g1v2 epc
on cpi.cat_id = epc.id
where cpi.prd_end_dt is null; -- Keeping only current products by filtering historical data

-- Create the  view for product columns
create view gold.dim_products as
select
	row_number() over(order by cpi.prd_start_dt, cpi.prd_key) as product_key, -- Surrogate key
	cpi.prd_id as product_id,
	cpi.prd_key as product_number,
	cpi.prd_nm as product_name,
	cpi.cat_id as category_id,
	epc.cat as category,
	epc.subcat as subcategory,
	epc.maintenance,
	cpi.prd_cost as cost,
	cpi.prd_line as product_line,
	cpi.prd_start_dt as start_date
from silver.crm_prd_info cpi
left join silver.erp_px_cat_g1v2 epc
on cpi.cat_id = epc.id
where cpi.prd_end_dt is null; -- Keeping only current products by filtering historical data


--validating
select * from gold.dim_products;


-- Aggregating Sales Columns
select * from silver.crm_sales_details;

select * from gold.dim_customers;

select * from gold.dim_products;


select 
	csd.sls_ord_num as order_number,
	dp.product_key,
	dc.customer_key,
	csd.sls_order_dt as order_date,
	csd.sls_ship_dt as ship_date,
	csd.sls_due_dt as due_date,
	csd.sls_sales as sales_amount,
	csd.sls_quantity as quantity,
	csd.sls_price as price 
from silver.crm_sales_details csd
left join gold.dim_customers dc 
on csd.sls_cust_id = dc.customer_id
left join gold.dim_products dp
on csd.sls_prd_key = dp.product_number;

-- Create view for Sales Details
create view gold.fact_sales as
select 
	csd.sls_ord_num as order_number,
	dp.product_key,
	dc.customer_key,
	csd.sls_order_dt as order_date,
	csd.sls_ship_dt as ship_date,
	csd.sls_due_dt as due_date,
	csd.sls_sales as sales_amount,
	csd.sls_quantity as quantity,
	csd.sls_price as price 
from silver.crm_sales_details csd
left join gold.dim_customers dc 
on csd.sls_cust_id = dc.customer_id
left join gold.dim_products dp
on csd.sls_prd_key = dp.product_number;

-- Validation
select * from gold.fact_sales;


-- Check foreign key integrity with dimensions table

select * from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
where c.customer_key is null;  -- Checks whether values are matched or not (Expectation: Returns no values)


select * from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where p.product_key is null;
