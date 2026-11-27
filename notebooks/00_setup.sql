-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00_setup: infraestructura base
-- MAGIC
-- MAGIC Crea los schemas y los Volumes que usa el pipeline Medallion.
-- MAGIC Es **idempotente** (`IF NOT EXISTS`): se puede ejecutar todas las veces que haga falta.
-- MAGIC
-- MAGIC | Objeto | Para qué sirve |
-- MAGIC |---|---|
-- MAGIC | `workspace.landing.raw_sales` (Volume) | Zona de aterrizaje de los CSV de ventas |
-- MAGIC | `workspace.bronze.checkpoints` (Volume) | Progreso del stream de Bronze |
-- MAGIC | `workspace.silver.checkpoints` (Volume) | Progreso del stream de Silver |

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.landing COMMENT 'Zona de aterrizaje de archivos crudos';
CREATE SCHEMA IF NOT EXISTS workspace.bronze  COMMENT 'Capa Bronze: datos crudos';
CREATE SCHEMA IF NOT EXISTS workspace.silver  COMMENT 'Capa Silver: datos limpios';
CREATE SCHEMA IF NOT EXISTS workspace.gold    COMMENT 'Capa Gold: datos para negocio';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.landing.raw_sales COMMENT 'CSV de ventas de las tiendas';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.bronze.checkpoints COMMENT 'Checkpoints del stream de Bronze';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.silver.checkpoints COMMENT 'Checkpoints del stream de Silver';

-- COMMAND ----------

SHOW SCHEMAS IN workspace;
