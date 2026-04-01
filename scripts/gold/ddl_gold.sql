/*
===============================================================================
GOLD LAYER (STAR SCHEMA - PHYSICAL MODEL) - DDL
===============================================================================
*/

-- ============================================================================
-- Dimension: Customers (SCD Type 2 Ready)
-- ============================================================================
DROP TABLE IF EXISTS gold.dim_customers CASCADE;
CREATE TABLE gold.dim_customers (
    customer_sk       INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id       INTEGER,
    customer_number   VARCHAR(50),
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    gender            VARCHAR(50),
    country           VARCHAR(100),
    create_date       DATE,
    _start_date       DATE DEFAULT CURRENT_DATE,
    _end_date         DATE,
    _is_current       BOOLEAN DEFAULT TRUE,
    _batch_id         INTEGER 
);

-- ============================================================================
-- Dimension: Products (SCD Type 2 Ready)
-- ============================================================================
DROP TABLE IF EXISTS gold.dim_products CASCADE;
CREATE TABLE gold.dim_products (
    product_sk        INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id        INTEGER,
    product_number    VARCHAR(50),
    product_name      VARCHAR(150),
    category          VARCHAR(100),
    subcategory       VARCHAR(100),
    product_line      VARCHAR(50),
    cost              NUMERIC(18,2),
    _start_date       DATE DEFAULT CURRENT_DATE,
    _end_date         DATE,
    _is_current       BOOLEAN DEFAULT TRUE,
    _batch_id         INTEGER 
);

-- ============================================================================
-- Dimension: Date (Lookup Table)
-- ============================================================================
DROP TABLE IF EXISTS gold.dim_date CASCADE;
CREATE TABLE gold.dim_date (
    date_sk        INTEGER PRIMARY KEY, -- Formato YYYYMMDD
    full_date      DATE UNIQUE,
    year           INTEGER,
    month          INTEGER,
    day            INTEGER,
    month_name     VARCHAR(20),
    day_name       VARCHAR(20)
);

-- ============================================================================
-- Fact: Sales (Central Transaction Table)
-- ============================================================================
DROP TABLE IF EXISTS gold.fact_sales CASCADE;
CREATE TABLE gold.fact_sales (
    order_number   VARCHAR(50),
    customer_sk    INTEGER,
    product_sk     INTEGER,
    order_date_sk  INTEGER,
    sales_amount   NUMERIC(18,2),
    quantity       INTEGER,
    price          NUMERIC(18,2),
    _batch_id      INTEGER,

    CONSTRAINT fk_customer FOREIGN KEY (customer_sk) REFERENCES gold.dim_customers(customer_sk),
    CONSTRAINT fk_product  FOREIGN KEY (product_sk)  REFERENCES gold.dim_products(product_sk),
    CONSTRAINT fk_date     FOREIGN KEY (order_date_sk) REFERENCES gold.dim_date(date_sk)
);