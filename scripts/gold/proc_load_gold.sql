CREATE OR REPLACE PROCEDURE gold.load_gold()
LANGUAGE plpgsql
AS $body$
DECLARE
    v_batch_id INTEGER;
    v_start_date DATE;
    v_end_date   DATE;
BEGIN
    v_batch_id := CAST(TO_CHAR(NOW(), 'YYYYMMDD') AS INTEGER);

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Iniciando Carga Capa Gold - Batch: %', v_batch_id;
    RAISE NOTICE '==========================================';

    -- Obtenemos el rango para la dimensión calendario
    SELECT COALESCE(MIN(order_date), '2010-01-01'::DATE) INTO v_start_date FROM silver.sales;
    SELECT COALESCE(MAX(order_date), CURRENT_DATE + INTERVAL '1 year') INTO v_end_date FROM silver.sales;

    -- 1. DIM DATE
    RAISE NOTICE '>> Poblando gold.dim_date...';
    TRUNCATE TABLE gold.dim_date CASCADE;
    INSERT INTO gold.dim_date (date_sk, full_date, year, month, day, month_name, day_name)
    SELECT
        CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER),
        d::DATE,
        EXTRACT(YEAR FROM d)::INT,
        EXTRACT(MONTH FROM d)::INT,
        EXTRACT(DAY FROM d)::INT,
        TO_CHAR(d, 'TMMonth'), 
        TO_CHAR(d, 'TMDay')
    FROM generate_series(v_start_date, v_end_date, INTERVAL '1 day') AS d;

    -- 2. DIM CUSTOMERS
    RAISE NOTICE '>> Cargando gold.dim_customers...';
    TRUNCATE TABLE gold.dim_customers RESTART IDENTITY CASCADE;
    INSERT INTO gold.dim_customers (
        customer_id, customer_number, first_name, last_name, 
        gender, country, create_date, _batch_id
    )
    SELECT 
        customer_id, customer_number, first_name, last_name, 
        gender, country, create_date, v_batch_id
    FROM silver.customers;

    -- 3. DIM PRODUCTS
    RAISE NOTICE '>> Cargando gold.dim_products...';
    TRUNCATE TABLE gold.dim_products RESTART IDENTITY CASCADE;
    INSERT INTO gold.dim_products (
        product_id, product_number, product_name, category, 
        subcategory, product_line, cost, _batch_id
    )
    SELECT 
        product_id, product_number, product_name, category, 
        subcategory, product_line, cost, v_batch_id
    FROM silver.products;

    -- 4. FACT SALES (CORREGIDO CON TRIM PROFUNDO)
    RAISE NOTICE '>> Cargando gold.fact_sales...';
    TRUNCATE TABLE gold.fact_sales CASCADE;

    INSERT INTO gold.fact_sales (
        order_number, customer_sk, product_sk, order_date_sk, 
        sales_amount, quantity, price, _batch_id
    )
    SELECT
        s.order_number,
        dc.customer_sk,
        dp.product_sk,
        dd.date_sk,
        s.sales_amount,
        s.quantity,
        s.price,
        v_batch_id
    FROM silver.sales s
    -- Join con Clientes (Usamos customer_id que es INTEGER, es más seguro)
    LEFT JOIN gold.dim_customers dc 
        ON s.customer_id = dc.customer_id 
        AND dc._is_current = TRUE
    -- Join con Productos (Aquí aplicamos TRIM en ambos lados para asegurar el match)
    LEFT JOIN gold.dim_products dp 
        ON TRIM(s.product_number) = TRIM(dp.product_number) 
        AND dp._is_current = TRUE
    -- Join con Fecha (Usando la SK de fecha para mayor velocidad)
    LEFT JOIN gold.dim_date dd 
        ON s.order_date = dd.full_date;

    RAISE NOTICE 'Carga Gold finalizada con éxito.';

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'ERROR EN CAPA GOLD: %', SQLERRM;
    RAISE;
END;
$body$;