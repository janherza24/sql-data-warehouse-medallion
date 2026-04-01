/*
===============================================================================
SILVER LAYER (INTEGRATED & CLEANED) - DDL
===============================================================================
*/

-- ============================================================================
-- Customers: Datos limpios de CRM + ERP
-- ============================================================================
DROP TABLE IF EXISTS silver.customers;
CREATE TABLE silver.customers (
    customer_id        INTEGER,
    customer_number    VARCHAR(50),
    first_name         VARCHAR(100), -- Ampliado para evitar truncado
    last_name          VARCHAR(100), -- Ampliado para evitar truncado
    marital_status     VARCHAR(50),
    gender             VARCHAR(50),
    birth_date         DATE,
    country            VARCHAR(100), -- 'United States' o 'Australia' caben bien
    create_date        DATE,
    _load_timestamp    TIMESTAMP DEFAULT NOW(),
    _batch_id          INTEGER
);

-- ============================================================================
-- Products: Datos limpios de CRM + Categorías de ERP
-- ============================================================================
DROP TABLE IF EXISTS silver.products;
CREATE TABLE silver.products (
    product_id         INTEGER,
    product_number     VARCHAR(50),
    product_name       VARCHAR(150), -- Ampliado para nombres descriptivos
    category           VARCHAR(100),
    subcategory        VARCHAR(100),
    cost               NUMERIC(18,2),
    product_line       VARCHAR(50),
    start_date         DATE,
    end_date           DATE,
    _load_timestamp    TIMESTAMP DEFAULT NOW(),
    _batch_id          INTEGER
);

-- ============================================================================
-- Sales: Transacciones normalizadas
-- ============================================================================
DROP TABLE IF EXISTS silver.sales;
CREATE TABLE silver.sales (
    order_number       VARCHAR(50),
    product_number     VARCHAR(50),
    customer_id        INTEGER,
    order_date         DATE,
    shipping_date      DATE,
    due_date           DATE,
    sales_amount       NUMERIC(18,2),
    quantity           INTEGER,
    price              NUMERIC(18,2),
    _load_timestamp    TIMESTAMP DEFAULT NOW(),
    _batch_id          INTEGER
);