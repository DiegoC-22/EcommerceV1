# Databricks notebook source
# MAGIC %md
# MAGIC # 01_bronze: ingesta con Auto Loader
# MAGIC
# MAGIC Lee los CSV nuevos de `workspace.landing.raw_sales` y los guarda **sin limpiar** en `workspace.bronze.sales`.
# MAGIC
# MAGIC - Todas las columnas entran como texto (`StringType`): Bronze es una copia fiel y auditable de lo que llegó. La limpieza ocurre en Silver.
# MAGIC - Con esquema explícito, Spark enlaza las columnas del CSV **por posición**, así que el orden debe coincidir con el del archivo.
# MAGIC - Se añaden `source_file` (de qué archivo vino cada fila) e `ingestion_timestamp`.
# MAGIC - El checkpoint recuerda qué archivos ya se procesaron (se compara la ruta del archivo). `trigger(availableNow=True)` procesa lo pendiente y termina.
# MAGIC
# MAGIC **Requisito:** haber ejecutado `00_setup`. Para tener archivos, `00_generate_data`.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, col

landing_path    = "/Volumes/workspace/landing/raw_sales_s3/"
checkpoint_path = "/Volumes/workspace/bronze/checkpoints/sales"
target_table    = "workspace.bronze.sales"

schema = StructType([
    StructField("transaction_id",        StringType(), True),
    StructField("customer_id",           StringType(), True),
    StructField("product_id",            StringType(), True),
    StructField("quantity",              StringType(), True),
    StructField("price_per_unit",        StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("store_id",              StringType(), True),
    StructField("payment_method",        StringType(), True),
])

# COMMAND ----------

df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", "true")
      .schema(schema)
      .load(landing_path)
      .withColumn("source_file", col("_metadata.file_path"))
      .withColumn("ingestion_timestamp", current_timestamp()))

(df.writeStream
   .option("checkpointLocation", checkpoint_path)
   .trigger(availableNow=True)
   .toTable(target_table)
   .awaitTermination())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comprobación
# MAGIC Filas por archivo de origen.

# COMMAND ----------

display(spark.table(target_table).groupBy("source_file").count().orderBy("source_file"))
