/*
===============================================================================
BRONZE LAYER (RECONSTRUIDO CON DATA REAL)
===============================================================================
*/

-- 1. CRM: Customer Information (cst_create_date viene como YYYY-MM-DD)
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id             INTEGER,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(100),
    cst_lastname       VARCHAR(100),
    cst_marital_status VARCHAR(10),
    cst_gndr           VARCHAR(10),
    cst_create_date    VARCHAR(50),
    _load_timestamp    TIMESTAMP DEFAULT NOW(),
    _batch_id          INTEGER
);

-- 2. CRM: Product Information (prd_nm es largo)
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id          INTEGER,
    prd_key         VARCHAR(50),
    prd_nm          VARCHAR(150),
    prd_cost        VARCHAR(50),
    prd_line        VARCHAR(10),
    prd_start_dt    VARCHAR(50),
    prd_end_dt      VARCHAR(50),
    _load_timestamp TIMESTAMP DEFAULT NOW(),
    _batch_id       INTEGER
);

-- 3. CRM: Sales Transactions (Fechas vienen como YYYYMMDD)
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num     VARCHAR(50),
    sls_prd_key     VARCHAR(50),
    sls_cust_id     INTEGER,
    sls_order_dt    VARCHAR(50),
    sls_ship_dt     VARCHAR(50),
    sls_due_dt      VARCHAR(50),
    sls_sales       VARCHAR(50),
    sls_quantity    VARCHAR(50),
    sls_price       VARCHAR(50),
    _load_timestamp TIMESTAMP DEFAULT NOW(),
    _batch_id       INTEGER
);

-- 4. ERP: Customer Demographics (CID tiene prefijo 'NAS')
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid             VARCHAR(50),
    bdate           VARCHAR(50),
    gen             VARCHAR(50),
    _load_timestamp TIMESTAMP DEFAULT NOW(),
    _batch_id       INTEGER
);

-- 5. ERP: Customer Location (CNTRY tiene nombres largos como 'Australia')
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid             VARCHAR(50),
    cntry           VARCHAR(100),
    _load_timestamp TIMESTAMP DEFAULT NOW(),
    _batch_id       INTEGER
);

-- 6. ERP: Product Categories (MAINTENANCE es la 4ta columna del CSV)
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id              VARCHAR(50),
    cat             VARCHAR(100),
    subcat          VARCHAR(100),
    maintenance     VARCHAR(50),
    _load_timestamp TIMESTAMP DEFAULT NOW(),
    _batch_id       INTEGER
);