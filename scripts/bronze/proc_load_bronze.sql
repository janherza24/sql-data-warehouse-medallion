CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $body$
DECLARE
    batch_start_time TIMESTAMP := NOW();
    -- Generamos un batch_id basado en la fecha actual (ej. 20260331)
    v_batch_id       INTEGER := CAST(TO_CHAR(NOW(), 'YYYYMMDD') AS INTEGER);
    base_path        TEXT := '/mnt/datasets/'; 
BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Iniciando Carga Capa Bronze - Batch ID: %', v_batch_id;
    RAISE NOTICE '==========================================';

    -- 1. CRM - CUSTOMER INFO
    RAISE NOTICE '>> Loading bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    EXECUTE format('COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date) 
                FROM %L WITH (FORMAT CSV, HEADER, DELIMITER '','', ENCODING ''UTF8'')', 
                base_path || 'source_crm/cust_info.csv');
    UPDATE bronze.crm_cust_info SET _batch_id = v_batch_id WHERE _batch_id IS NULL;

    -- 2. CRM - PRODUCT INFO
    RAISE NOTICE '>> Loading bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    EXECUTE format('COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt) 
                FROM %L WITH (FORMAT CSV, HEADER, DELIMITER '','', ENCODING ''UTF8'')', 
                base_path || 'source_crm/prd_info.csv');
    UPDATE bronze.crm_prd_info SET _batch_id = v_batch_id WHERE _batch_id IS NULL;

    -- 3. CRM - SALES DETAILS
    RAISE NOTICE '>> Loading bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    EXECUTE format('COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price) 
                FROM %L WITH (FORMAT CSV, HEADER, DELIMITER '','', ENCODING ''UTF8'')', 
                base_path || 'source_crm/sales_details.csv');
    UPDATE bronze.crm_sales_details SET _batch_id = v_batch_id WHERE _batch_id IS NULL;

    -- 4. ERP - CUSTOMER AZ12
    RAISE NOTICE '>> Loading bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    EXECUTE format('COPY bronze.erp_cust_az12 (cid, bdate, gen) 
                FROM %L WITH (FORMAT CSV, HEADER, DELIMITER '','', ENCODING ''UTF8'')', 
                base_path || 'source_erp/cust_az12.csv');
    UPDATE bronze.erp_cust_az12 SET _batch_id = v_batch_id WHERE _batch_id IS NULL;

    -- 5. ERP - LOCATION A101
    RAISE NOTICE '>> Loading bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    EXECUTE format('COPY bronze.erp_loc_a101 (cid, cntry) 
                FROM %L WITH (FORMAT CSV, HEADER, DELIMITER '','', ENCODING ''UTF8'')', 
                base_path || 'source_erp/loc_a101.csv');
    UPDATE bronze.erp_loc_a101 SET _batch_id = v_batch_id WHERE _batch_id IS NULL;

    -- 6. ERP - PRODUCT CATEGORY (Corregido: mantenimiento -> maintenance)
    RAISE NOTICE '>> Loading bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    EXECUTE format('COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance) 
                FROM %L WITH (FORMAT CSV, HEADER, DELIMITER '','', ENCODING ''UTF8'')', 
                base_path || 'source_erp/px_cat_g1v2.csv');
    UPDATE bronze.erp_px_cat_g1v2 SET _batch_id = v_batch_id WHERE _batch_id IS NULL;

    RAISE NOTICE '------------------------------------------';
    RAISE NOTICE 'Carga Bronze finalizada en % segundos', EXTRACT(EPOCH FROM (NOW() - batch_start_time));
    RAISE NOTICE '==========================================';
END;
$body$;