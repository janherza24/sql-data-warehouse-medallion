CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $body$
DECLARE
    batch_start_time TIMESTAMP := NOW();
    v_batch_id       INTEGER := CAST(TO_CHAR(NOW(), 'YYYYMMDD') AS INTEGER);
BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Iniciando Limpieza: Capa Silver';
    RAISE NOTICE '==========================================';

    -- 1. SILVER CUSTOMERS (Sin cambios, ya funcionaba)
    RAISE NOTICE '>> Procesando silver.customers...';
    TRUNCATE TABLE silver.customers;
    INSERT INTO silver.customers (
        customer_id, customer_number, first_name, last_name, 
        marital_status, gender, birth_date, country, create_date, _batch_id
    )
    SELECT
        c.cst_id,
        c.cst_key,
        TRIM(c.cst_firstname),
        TRIM(c.cst_lastname),
        CASE 
            WHEN UPPER(TRIM(c.cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(c.cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(c.cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(c.cst_gndr)) = 'M' THEN 'Male'
            ELSE COALESCE(e.erp_gender, 'n/a')
        END,
        e.erp_birth_date,
        COALESCE(l.erp_country, 'n/a'),
        CASE WHEN c.cst_create_date ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(c.cst_create_date AS DATE) ELSE NULL END,
        v_batch_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
        FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL
    ) c
    LEFT JOIN (
        SELECT 
            REPLACE(REPLACE(cid, 'NASAW000', ''), '-', '') AS join_key,
            CASE WHEN bdate ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(bdate AS DATE) ELSE NULL END AS erp_birth_date,
            CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female' 
                 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male' 
                 ELSE 'n/a' END AS erp_gender
        FROM bronze.erp_cust_az12
    ) e ON REPLACE(c.cst_key, 'AW000', '') = e.join_key
    LEFT JOIN (
        SELECT 
            REPLACE(REPLACE(cid, 'AW-000', ''), '-', '') AS join_key,
            CASE WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                 ELSE COALESCE(NULLIF(TRIM(cntry), ''), 'n/a') END AS erp_country
        FROM bronze.erp_loc_a101
    ) l ON REPLACE(c.cst_key, 'AW000', '') = l.join_key
    WHERE c.rn = 1;

    -- 2. SILVER PRODUCTS (CORREGIDO: Limpieza de prefijos en prd_key)
    RAISE NOTICE '>> Procesando silver.products...';
    TRUNCATE TABLE silver.products;
    INSERT INTO silver.products (
        product_id, product_number, product_name, category, 
        subcategory, cost, product_line, start_date, end_date, _batch_id
    )
    SELECT
        p.prd_id,
        -- Extraemos la parte final del código (ej: 'AC-HE-HL-U509' -> 'HL-U509')
        -- Se eliminan los primeros 6 caracteres que corresponden a los prefijos tipo 'XX-XX-'
        CASE 
            WHEN p.prd_key LIKE '__-__-%' THEN SUBSTRING(p.prd_key FROM 7) 
            ELSE p.prd_key 
        END AS product_number,
        TRIM(p.prd_nm),
        COALESCE(cat.cat, 'n/a'),
        COALESCE(cat.subcat, 'n/a'),
        COALESCE(NULLIF(TRIM(p.prd_cost), '')::NUMERIC, 0),
        CASE WHEN UPPER(TRIM(p.prd_line)) = 'M' THEN 'Mountain'
             WHEN UPPER(TRIM(p.prd_line)) = 'R' THEN 'Road'
             WHEN UPPER(TRIM(p.prd_line)) = 'S' THEN 'Other Sales'
             WHEN UPPER(TRIM(p.prd_line)) = 'T' THEN 'Touring'
             ELSE 'n/a' END,
        CASE WHEN p.prd_start_dt ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(p.prd_start_dt AS DATE) ELSE NULL END,
        CASE WHEN p.prd_end_dt ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(p.prd_end_dt AS DATE) ELSE NULL END,
        v_batch_id
    FROM bronze.crm_prd_info p
    LEFT JOIN bronze.erp_px_cat_g1v2 cat ON REPLACE(SUBSTRING(p.prd_key FROM 1 FOR 5), '-', '_') = cat.id;

    -- 3. SILVER SALES (Sin cambios)
    RAISE NOTICE '>> Procesando silver.sales...';
    TRUNCATE TABLE silver.sales;
    INSERT INTO silver.sales (
        order_number, product_number, customer_id, order_date, 
        shipping_date, due_date, sales_amount, quantity, price, _batch_id
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt ~ '^\d{8}$' THEN TO_DATE(sls_order_dt, 'YYYYMMDD') ELSE NULL END,
        CASE WHEN sls_ship_dt  ~ '^\d{8}$' THEN TO_DATE(sls_ship_dt, 'YYYYMMDD')  ELSE NULL END,
        CASE WHEN sls_due_dt   ~ '^\d{8}$' THEN TO_DATE(sls_due_dt, 'YYYYMMDD')   ELSE NULL END,
        NULLIF(TRIM(sls_sales), '')::NUMERIC,
        NULLIF(TRIM(sls_quantity), '')::INT,
        NULLIF(TRIM(sls_price), '')::NUMERIC,
        v_batch_id
    FROM bronze.crm_sales_details;

    RAISE NOTICE 'Limpieza Silver finalizada en % segundos', EXTRACT(EPOCH FROM (NOW() - batch_start_time));

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'ERROR EN CAPA SILVER: %', SQLERRM;
    RAISE;
END;
$body$;