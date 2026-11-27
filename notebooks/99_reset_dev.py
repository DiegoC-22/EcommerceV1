# Databricks notebook source
# MAGIC %md
# MAGIC # 99_reset_dev: reiniciar el pipeline (SOLO DESARROLLO)
# MAGIC
# MAGIC **Este notebook es destructivo. No forma parte del pipeline ni debe incluirse en un Job.**
# MAGIC
# MAGIC Borra las tablas Bronze, Silver, cuarentena y Gold, y los checkpoints de Bronze y Silver.
# MAGIC **No** borra los archivos de `landing`: al volver a ejecutar `01_bronze`, se reingestan todos.
# MAGIC
# MAGIC Reglas para reiniciar:
# MAGIC - Borrar siempre juntos checkpoint y tabla de cada capa. Si queda un checkpoint sin su tabla (o al revés), el estado queda inconsistente.
# MAGIC - Silver y cuarentena se vacían siempre juntas: si Silver conserva datos y la cuarentena se llena de nuevo, los reprocesos aparecen como `duplicado_exacto`.
# MAGIC
# MAGIC Orden de ejecución tras el reinicio: `01_bronze`, `02_silver`, `03_gold`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.bronze.sales")
dbutils.fs.rm("/Volumes/workspace/bronze/checkpoints/sales", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver y cuarentena

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.silver.sales")
spark.sql("DROP TABLE IF EXISTS workspace.silver.sales_quarantine")
dbutils.fs.rm("/Volumes/workspace/silver/checkpoints/sales", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.gold.sales_daily")
