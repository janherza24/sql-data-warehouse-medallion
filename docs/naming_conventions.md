# 📋 Naming Conventions - SQL Data Warehouse

To ensure consistency, readability, and scalability of the Data Warehouse, the following naming standards are applied across all stages of the pipeline (Medallion Architecture).

## 1. Databases and Schemas
*   **Database Name:** `datawarehouse` (Descriptive of the project scope).
*   **Medallion Schemas:**
    *   `bronze`: Raw data ingestion (Source-aligned).
    *   `silver`: Cleansed and normalized data (Anonymized/Validated).
    *   `gold`: Dimensional modeling and business-ready views (Curated).

## 2. Database Objects
*   **Tables:** Use `snake_case` and plural nouns (e.g., `crm_customers`, `erp_sales`).
*   **Views:** Prefix with `view_` or use `fact_`/`dim_` patterns in the Gold layer (e.g., `fact_sales`).
*   **Stored Procedures:** Prefix with `proc_` followed by the action (e.g., `proc_load_silver`).

## 3. Columns and Attributes
*   **Primary Keys (PK):** Consistently named `id` or `[table_name]_id` (e.g., `customer_id`).
*   **Foreign Keys (FK):** Must match the name of the PK from the source table for clarity.
*   **Dates:** Use the `_date` suffix (e.g., `sale_date`, `registration_date`).
*   **Amounts/Currency:** Use suffixes like `_amount` or clear names like `price`, `total_tax`.
*   **Booleans:** Prefix with `is_` or `has_` (e.g., `is_active`, `has_purchased`).

## 4. Files and Scripts
*   **SQL Scripts:** Prefix with numbers to indicate execution order (e.g., `01_setup.sql`, `02_transform.sql`).
*   **Configuration:** `.env` for local secrets and `docker-compose.yml` for infrastructure.
