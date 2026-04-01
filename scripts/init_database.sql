/*
===============================================================================
Main Orchestration Script - Medallion Architecture
===============================================================================
*/

-- En scripts psql, usamos \echo para imprimir mensajes en consola
\echo '=========================================='
\echo 'Iniciando creación de esquemas...'
\echo '=========================================='

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

\echo 'Cargando estructuras de tablas (DDL)...'

\i /mnt/scripts/bronze/ddl_bronze.sql
\i /mnt/scripts/silver/ddl_silver.sql
\i /mnt/scripts/gold/ddl_gold.sql

\echo 'Cargando procedimientos almacenados...'

\i /mnt/scripts/bronze/proc_load_bronze.sql
\i /mnt/scripts/silver/proc_load_silver.sql
\i /mnt/scripts/gold/proc_load_gold.sql

\echo 'Ejecutando carga inicial del Data Warehouse...'

-- Aquí sí podemos usar RAISE NOTICE porque está dentro de un bloque DO
DO $$BEGIN
    RAISE NOTICE '>> Iniciando carga de capa BRONZE...';
    CALL bronze.load_bronze();

    RAISE NOTICE '>> Iniciando carga de capa SILVER...';
    CALL silver.load_silver();

    RAISE NOTICE '>> Iniciando carga de capa GOLD...';
    CALL gold.load_gold();

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'DATA WAREHOUSE INICIALIZADO CON ÉXITO';
    RAISE NOTICE '==========================================';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ERROR DURANTE LA INICIALIZACIÓN: %', SQLERRM;
    RAISE;
END$$;