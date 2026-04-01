# 📖 Data Catalog

This catalog describes the structure, business logic, and purpose of the data assets within the Data Warehouse.

## 🟢 Gold Layer (Business Intelligence)

### Table/View: `gold.fact_sales`
**Description:** A central fact table integrating ERP sales transactions with CRM customer demographics.
| Column | Type | Description | Source |
| :--- | :--- | :--- | :--- |
| `order_id` | INT | Unique identifier for the sales order. | ERP |
| `customer_id` | INT | Normalized Customer ID (Cleaned via Regex). | CRM/ERP |
| `full_name` | VARCHAR | Concatenated First Name and Last Name. | CRM |
| `sale_date` | DATE | The date the transaction occurred. | ERP |
| `amount` | DECIMAL | Total transaction value in USD. | ERP |
| `customer_segment` | VARCHAR | Business logic: VIP (>1k), Frequent (>500), Standard. | Logic |

---

## 🥈 Silver Layer (Normalized & Cleansed)

### Table: `silver.crm_customers`
**Description:** Standardized customer information.
*   **Transformations:** `INITCAP` applied to names, `LOWER` for emails, and `REGEXP_REPLACE` for ID consistency.

### Table: `silver.erp_sales`
**Description:** Validated sales transactions (filters out records where `amount <= 0`).

---

## 🥉 Bronze Layer (Raw Data)

### Table: `bronze.crm_customers`
**Description:** Direct ingestion from CRM CSV files. Contains raw IDs with prefixes (e.g., 'CUST-123').

### Table: `bronze.erp_sales`
**Description:** Direct ingestion from ERP CSV files. Contains raw transactional records.
