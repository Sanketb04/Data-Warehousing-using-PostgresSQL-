-- Drop the procedure if it already exists
DROP PROCEDURE IF EXISTS silver.load_silver;

-- Create the procedure
CREATE OR REPLACE PROCEDURE silver.load_silver()
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
    RAISE NOTICE 'Loading Silver Layer - Start Time: %', start_time;
    RAISE NOTICE '================================================';
  
    -- CRM Tables
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- CRM Customer Info
    table_start_time := clock_timestamp();
    
   	-- Truncate and insert into crm_cust_info
    RAISE NOTICE '>> Truncating table crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE '>> Inserting values into silver.crm_cust_info';

    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) sub
    WHERE flag_last = 1;
  
    table_end_time := clock_timestamp();
    table_duration := table_end_time - table_start_time;
    SELECT COUNT(*) INTO row_count FROM silver.crm_cust_info;
    RAISE NOTICE 'Inserted % row(s) into silver.crm_cust_info | Duration: % seconds', row_count,EXTRACT(EPOCH FROM table_duration);

    -- CRM Product Info
    table_start_time := clock_timestamp();
   
    -- Truncate and insert into crm_prd_info
    RAISE NOTICE '>> Truncating table crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE '>> Inserting values into silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key, 7),
        prd_nm,
        COALESCE(prd_cost::int, 0),
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Roads'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            ELSE 'n/a'
        END,
        prd_start_dt,
        LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_end_dt) - 1
    FROM bronze.crm_prd_info;
   
    table_end_time := clock_timestamp();
    table_duration := table_end_time - table_start_time;
    SELECT COUNT(*) INTO row_count FROM silver.crm_prd_info;
    RAISE NOTICE 'Inserted % row(s) into silver.crm_prd_info | Duration: % seconds', row_count, EXTRACT(EPOCH FROM table_duration);

    -- CRM Sales Details
    table_start_time := clock_timestamp();
  
    -- Truncate and insert into crm_sales_details
    RAISE NOTICE '>> Truncating table crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE '>> Inserting values into silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
             ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD') END,
        CASE WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
             ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD') END,
        CASE WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
             ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD') END,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price * sls_quantity)
             THEN ABS(sls_price * sls_quantity)
             ELSE sls_sales END,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0
             THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price END
    FROM bronze.crm_sales_details;

    table_end_time := clock_timestamp();
    table_duration := table_end_time - table_start_time;
    SELECT COUNT(*) INTO row_count FROM silver.crm_sales_details;
    RAISE NOTICE 'Inserted % row(s) into silver.crm_sales_details | Duration: % seconds', row_count, EXTRACT(EPOCH FROM table_duration); 

    -- ERP Tables
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- ERP PX_CUST_AZ12
   	table_start_time := clock_timestamp();   
   
   -- Truncate and insert into erp_cust_az12
    RAISE NOTICE '>> Truncating table erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE '>> Inserting values into silver.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT 
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END,
        CASE WHEN bdate > CURRENT_DATE THEN NULL ELSE bdate END,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;
 
    table_end_time := clock_timestamp();
    table_duration := table_end_time - table_start_time;
    SELECT COUNT(*) INTO row_count FROM silver.erp_cust_az12;
    RAISE NOTICE 'Inserted % row(s) into silver.erp_cust_az12 | Duration: % seconds', row_count, EXTRACT(EPOCH FROM table_duration);   
  
   
    -- ERP_LOC_A101
   	table_start_time := clock_timestamp();    

    -- Truncate and insert into erp_loc_a101
    RAISE NOTICE '>> Truncating table erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE '>> Inserting values into silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT 
        REPLACE(cid, '-', ''),
        CASE 
            WHEN UPPER(TRIM(cntry)) IN ('US','USA','UNITED STATES','UNITED STATES OF AMERICA') THEN 'United States'
            WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;
   
    table_end_time := clock_timestamp();
    table_duration := table_end_time - table_start_time;
    SELECT COUNT(*) INTO row_count FROM silver.erp_loc_a101;
    RAISE NOTICE 'Inserted % row(s) into silver.erp_loc_a101 | Duration: % seconds', row_count, EXTRACT(EPOCH FROM table_duration);
   
    -- ERP_px_cat_g1v2
   	table_start_time := clock_timestamp(); 
   
    -- Truncate and insert into erp_px_cat_g1v2
    RAISE NOTICE '>> Truncating table erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting values into silver.erp_px_cat_g1v2';

    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT 
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
   
    table_end_time := clock_timestamp();
    table_duration := table_end_time - table_start_time;
    SELECT COUNT(*) INTO row_count FROM silver.erp_px_cat_g1v2;
    RAISE NOTICE 'Inserted % row(s) into silver.erp_px_cat_g1v2 | Duration: % seconds', row_count, EXTRACT(EPOCH FROM table_duration);

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error occurred: %', SQLERRM;
END $$;

-- Call the stored procedure
CALL silver.load_silver();

