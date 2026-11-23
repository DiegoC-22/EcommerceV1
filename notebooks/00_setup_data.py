-- Databricks notebook source
SELECT current_catalog(), current_schema(), current_user();

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.landing COMMENT 'Zona de aterrizaje de archivos crudos';
CREATE SCHEMA IF NOT EXISTS workspace.bronze  COMMENT 'Capa Bronze: datos crudos';
CREATE SCHEMA IF NOT EXISTS workspace.silver  COMMENT 'Capa Silver: datos limpios';
CREATE SCHEMA IF NOT EXISTS workspace.gold    COMMENT 'Capa Gold: datos para negocio';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.landing.raw_sales COMMENT 'CSV de ventas de las tiendas';

-- COMMAND ----------

SHOW SCHEMAS IN workspace;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ruta = "/Volumes/workspace/landing/raw_sales/prueba.txt"
-- MAGIC dbutils.fs.put(ruta, "hola volume", True)
-- MAGIC print(dbutils.fs.ls("/Volumes/workspace/landing/raw_sales/"))
-- MAGIC print(spark.read.text(ruta).collect())
-- MAGIC dbutils.fs.rm(ruta)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC catalog, schema, volume = "workspace", "landing", "raw_sales"
-- MAGIC raw_path = f"/Volumes/{catalog}/{schema}/{volume}"
-- MAGIC
-- MAGIC sales_day_1 = """transaction_id,customer_id,product_id,quantity,price_per_unit,transaction_timestamp,store_id,payment_method
-- MAGIC T001,C101,P001,2,10.50,2025-06-10T09:00:00Z,S01,Credit Card
-- MAGIC T002,C102,P003,1,25.00,2025-06-10T09:15:00Z,S02,Cash
-- MAGIC T003,C101,P002,3,5.75,2025-06-10T09:30:00Z,S01,Debit Card
-- MAGIC T004,C103,P001,1,10.50,2025-06-10T10:00:00Z,S03,Credit Card
-- MAGIC T005,C102,P004,2,12.00,2025-06-10T10:45:00Z,S02,Credit Card
-- MAGIC T006,C104,P005,1,50.00,2025-06-10T11:00:00Z,S01,Cash
-- MAGIC T007,C101,P003,1,25.00,2025-06-10T12:00:00Z,S03,Debit Card
-- MAGIC T008,C105,P002,4,5.75,2025-06-10T13:00:00Z,S01,Credit Card
-- MAGIC T009,C103,P006,1,75.00,2025-06-10T14:00:00Z,S02,Credit Card
-- MAGIC T010,C104,P001,3,10.50,2025-06-10T15:00:00Z,S01,Cash
-- MAGIC """
-- MAGIC
-- MAGIC dbutils.fs.put(f"{raw_path}/sales_day_1.csv", sales_day_1, True)
-- MAGIC print(dbutils.fs.ls(raw_path))