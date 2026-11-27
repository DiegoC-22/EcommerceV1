-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04_business_queries: preguntas de negocio sobre Gold
-- MAGIC
-- MAGIC Consultas de ejemplo sobre `workspace.gold.sales_daily`. Gold guarda el detalle fino (día, tienda, producto) y cada pregunta lo agrupa hacia arriba.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pregunta 1: ventas totales por tienda

-- COMMAND ----------

SELECT store_id, SUM(total_revenue) AS ingreso_total
FROM workspace.gold.sales_daily
GROUP BY store_id
ORDER BY ingreso_total DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pregunta 2: productos vendidos
-- MAGIC El orden por unidades y por ingreso puede diferir: por eso Gold guarda las tres medidas.

-- COMMAND ----------

SELECT product_id, SUM(total_units) AS unidades, SUM(total_revenue) AS ingreso
FROM workspace.gold.sales_daily
GROUP BY product_id
ORDER BY unidades DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Caso de control: tienda S01 el 10 de junio de 2025
-- MAGIC Resultado esperado: P001 (52.50, 5 unidades, 2 transacciones), P002 (40.25, 7, 2) y P005 (50.00, 1, 1).

-- COMMAND ----------

SELECT * FROM workspace.gold.sales_daily
WHERE store_id = 'S01' AND sale_date = '2025-06-10'
ORDER BY product_id;
