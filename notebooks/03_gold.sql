-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03_gold: tabla para el negocio
-- MAGIC
-- MAGIC Construye `workspace.gold.sales_daily` desde Silver, con granularidad **día + tienda + producto**.
-- MAGIC
-- MAGIC | Medida | Qué es |
-- MAGIC |---|---|
-- MAGIC | `total_revenue` | Suma de `total_amount` (dinero vendido) |
-- MAGIC | `total_units` | Suma de `quantity` (unidades vendidas) |
-- MAGIC | `transaction_count` | Número de transacciones |
-- MAGIC
-- MAGIC La tabla se **recalcula completa** con `CREATE OR REPLACE TABLE ... AS SELECT`: ejecutarla una o varias veces da el mismo resultado (idempotente). Solo cambia `calculated_at`.
-- MAGIC
-- MAGIC **Requisito:** haber ejecutado `02_silver`.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.sales_daily AS
SELECT
  sale_date,
  store_id,
  product_id,
  SUM(total_amount)               AS total_revenue,
  SUM(quantity)                   AS total_units,
  COUNT(DISTINCT transaction_id)  AS transaction_count,
  current_timestamp()             AS calculated_at
FROM workspace.silver.sales
GROUP BY sale_date, store_id, product_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reconciliación
-- MAGIC Agregar no debe crear ni perder dinero: los totales de Gold deben coincidir con los de Silver.
-- MAGIC Si no coinciden, la celda falla con un error (así un Job de la fase 5 se detendría).

-- COMMAND ----------

SELECT assert_true(
         s.ingreso = g.ingreso AND s.unidades = g.unidades AND s.transacciones = g.transacciones,
         'Gold no cuadra con Silver'
       ) AS gold_cuadra_con_silver,
       s.ingreso AS ingreso_silver, g.ingreso AS ingreso_gold,
       s.unidades AS unidades_silver, g.unidades AS unidades_gold,
       s.transacciones AS transacciones_silver, g.transacciones AS transacciones_gold
FROM (SELECT SUM(total_amount) AS ingreso, SUM(quantity) AS unidades,
             COUNT(DISTINCT transaction_id) AS transacciones
      FROM workspace.silver.sales) s
CROSS JOIN
     (SELECT SUM(total_revenue) AS ingreso, SUM(total_units) AS unidades,
             SUM(transaction_count) AS transacciones
      FROM workspace.gold.sales_daily) g;
