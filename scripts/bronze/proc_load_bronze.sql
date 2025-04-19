/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze;
===============================================================================
*/
-- Loading the data using Bulk Insert with Stored Procedure

SET datestyle TO 'DMY'; -- to match the date column from csv file

	
truncate table bronze.crm_cust_info;
COPY bronze.crm_cust_info
FROM '/Users/atishdhamala/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
DELIMITER ','
CSV HEADER;
	
select count(*) from bronze.crm_cust_info;

truncate table bronze.crm_prd_info;
COPY bronze.crm_prd_info
FROM '/Users/atishdhamala/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
DELIMITER ','
CSV HEADER;
	
select count(*) from bronze.crm_prd_info;
	

truncate table bronze.crm_sales_details;
COPY bronze.crm_sales_details
FROM '/Users/atishdhamala/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
DELIMITER ','
CSV HEADER;
	
select count(*) from bronze.crm_sales_details;

	
truncate table bronze.erp_px_cat_g1v2;
COPY bronze.erp_px_cat_g1v2
FROM '/Users/atishdhamala/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
DELIMITER ','
CSV HEADER;
	
select count(*) from bronze.erp_px_cat_g1v2;

truncate table bronze.erp_loc_a101;
COPY bronze.erp_loc_a101
FROM '/Users/atishdhamala/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
DELIMITER ','
CSV HEADER;
	
select count(*) from bronze.erp_loc_a101;
		
truncate table bronze.erp_cust_az12;
COPY bronze.erp_cust_az12
FROM '/Users/atishdhamala/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
DELIMITER ','
CSV HEADER;
	
select count(*) from bronze.erp_cust_az12;

--Creating the stored procedure
-- Drop the procedure if it already exists
DROP PROCEDURE IF EXISTS bronze.load_bronze;

-- Create the procedure
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    row_count INT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    load_duration INTERVAL;
    table_start_time TIMESTAMP;
    table_end_time TIMESTAMP;
    table_duration INTERVAL;
BEGIN
    -- Capture Start Time
    start_time := clock_timestamp();

    -- Set date style
    EXECUTE 'SET datestyle TO ''DMY''';

    -- Logging messages
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer - Start Time: %', start_time;
    RAISE NOTICE '================================================';

    -- CRM Tables
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- CRM Customer Info
    table_start_time := clock_timestamp();
   
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
   
    RAISE NOTICE '>> Inserting into Table: bronze.crm_cust_info';
    COPY bronze.crm_cust_info FROM '/Users/atishdhamala/sql-data-warehouse-project/datasets/source_crm/cust_info.csv' 
    DELIMITER ',' CSV HEADER;

    table_end_time := clock_timestamp();
    table_duration := table_end_time - table_start_time;
    SELECT COUNT(*) INTO row_count FROM bronze.crm_cust_info;
    RAISE NOTICE 'Inserted % row(s) into bronze.crm_cust_info | Duration: % seconds', row_count,EXTRACT(EPOCH FROM table_duration);

    -- CRM Product Info
    table_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> Inserting into Table: bronze.crm_prd_info';
    COPY bronze.crm_prd_info FROM '/Users/atishdhamala/sql-data-warehouse-project/datasets/source_crm/prd_info.csv' 
    DELIMITER ',' CSV HEADER;
   
    table_end_time := clock_timestamp();
    table_duration := table_end_time - table_start_time;
    SELECT COUNT(*) INTO row_count FROM bronze.crm_prd_info;
    RAISE NOTICE 'Inserted % row(s) into bronze.crm_prd_info | Duration: % seconds', row_count, EXTRACT(EPOCH FROM table_duration);

    -- CRM Sales Details
    table_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Inserting into Table: bronze.crm_sales_details';
    COPY bronze.crm_sales_details FROM '/Users/atishdhamala/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' 
    DELIMITER ',' CSV HEADER;

    table_end_time := clock_timestamp();
    table_duration := table_end_time - table_start_time;
    SELECT COUNT(*) INTO row_count FROM bronze.crm_sales_details;
    RAISE NOTICE 'Inserted % row(s) into bronze.crm_sales_details | Duration: % seconds', row_count, EXTRACT(EPOCH FROM table_duration);

    -- ERP Tables
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- ERP PX_CAT_G1V2
   	table_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting into Table: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2 FROM '/Users/atishdhamala/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv' 
    DELIMITER ',' CSV HEADER;

    table_end_time := clock_timestamp();
    table_duration := table_end_time - table_start_time;
    SELECT COUNT(*) INTO row_count FROM bronze.erp_px_cat_g1v2;
    RAISE NOTICE 'Inserted % row(s) into bronze.erp_px_cat_g1v2 | Duration: % seconds', row_count, EXTRACT(EPOCH FROM table_duration);

    -- ERP LOC_A101
    table_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Inserting into Table: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101 FROM '/Users/atishdhamala/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv' 
    DELIMITER ',' CSV HEADER;

    table_end_time := clock_timestamp();
    table_duration := table_end_time - table_start_time;
    SELECT COUNT(*) INTO row_count FROM bronze.erp_loc_a101;
    RAISE NOTICE 'Inserted % row(s) into bronze.erp_loc_a101 | Duration: % seconds', row_count, EXTRACT(EPOCH FROM table_duration);

    -- ERP CUST_AZ12
    table_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> Inserting into Table: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12 FROM '/Users/atishdhamala/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv' 
    DELIMITER ',' CSV HEADER;
 
    table_end_time := clock_timestamp();
    table_duration := table_end_time - table_start_time;
    SELECT COUNT(*) INTO row_count FROM bronze.erp_cust_az12;
    RAISE NOTICE 'Inserted % row(s) into bronze.erp_cust_az12 | Duration: % seconds', row_count, EXTRACT(EPOCH FROM table_duration) ;

    -- Capture End Time and Compute Duration
    end_time := clock_timestamp();
    load_duration := end_time - start_time;

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Bronze Layer Loading Completed!';
    RAISE NOTICE 'End Time: %', end_time;
    RAISE NOTICE 'Total Load Duration: % seconds', EXTRACT(EPOCH FROM load_duration);
    RAISE NOTICE '================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error: %', SQLERRM;
        ROLLBACK;
END $$;

--Calling the stored procedure
CALL bronze.load_bronze();
