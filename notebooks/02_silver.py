# Databricks notebook source
# MAGIC %md
# MAGIC # 02_silver: calidad de datos y cuarentena
# MAGIC
# MAGIC Lee `workspace.bronze.sales`, valida cada fila y la reparte en dos tablas:
# MAGIC
# MAGIC - `workspace.silver.sales`: filas válidas, con tipos correctos, `total_amount`, `sale_date` y el flag `is_anonymous_customer`.
# MAGIC - `workspace.silver.sales_quarantine`: filas rechazadas, con el dato crudo (texto) y el motivo en `quarantine_reason`.
# MAGIC
# MAGIC **Campos obligatorios:** `transaction_id`, `product_id`, `store_id`, `quantity`, `price_per_unit`, `transaction_timestamp`. **Opcionales:** `customer_id`, `payment_method`.
# MAGIC
# MAGIC **Orden de validación** (se registra el primer motivo que falle): campo obligatorio vacío, cantidad no numérica, precio no numérico, fecha inválida, cantidad negativa, cantidad cero. Después, duplicados: dentro del lote y contra lo que ya está en Silver (hash de las 8 columnas de negocio).
# MAGIC
# MAGIC **Criterio:** si el campo inválido participa en la medida (ingreso) o en la granularidad de Gold (día, tienda, producto), la fila va a cuarentena. Si es descriptivo, se conserva con un flag.
# MAGIC
# MAGIC **Importante:** `procesar_lote` escribe primero la cuarentena y después hace el `MERGE`. Los DataFrames se evalúan de forma diferida: si el `MERGE` fuera primero, la comparación contra Silver vería las filas recién insertadas y marcaría todo como duplicado.
# MAGIC
# MAGIC **Decisión de diseño sobre conflictos (limitación aceptada):** si dos versiones del mismo `transaction_id` con contenido distinto llegan en el **mismo lote**, ambas van a cuarentena. Si la primera ya estaba en Silver (lote anterior), el original se conserva y solo la nueva va a cuarentena. Por eso el resultado puede variar según cómo lleguen los lotes.
# MAGIC
# MAGIC **Requisito:** haber ejecutado `00_setup` y `01_bronze`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

COLUMNAS = ["transaction_id", "customer_id", "product_id", "quantity",
            "price_per_unit", "transaction_timestamp", "store_id", "payment_method"]

checkpoint_path = "/Volumes/workspace/silver/checkpoints/sales"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.silver.sales (
# MAGIC   transaction_id        STRING,
# MAGIC   customer_id           STRING,
# MAGIC   product_id            STRING,
# MAGIC   quantity              INT,
# MAGIC   price_per_unit        DECIMAL(10,2),
# MAGIC   total_amount          DECIMAL(12,2),
# MAGIC   transaction_timestamp TIMESTAMP,
# MAGIC   sale_date             DATE,
# MAGIC   store_id              STRING,
# MAGIC   payment_method        STRING,
# MAGIC   is_anonymous_customer BOOLEAN,
# MAGIC   row_hash              STRING,
# MAGIC   source_file           STRING,
# MAGIC   ingestion_timestamp   TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.silver.sales_quarantine (
# MAGIC   transaction_id        STRING,
# MAGIC   customer_id           STRING,
# MAGIC   product_id            STRING,
# MAGIC   quantity              STRING,
# MAGIC   price_per_unit        STRING,
# MAGIC   transaction_timestamp STRING,
# MAGIC   store_id              STRING,
# MAGIC   payment_method        STRING,
# MAGIC   source_file           STRING,
# MAGIC   ingestion_timestamp   TIMESTAMP,
# MAGIC   quarantine_reason     STRING,
# MAGIC   quarantined_at        TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1: clasificar cada fila
# MAGIC `try_cast` devuelve NULL cuando la conversión es imposible, en lugar de romper el pipeline.

# COMMAND ----------

def clasificar(df):
    # 1) quitar espacios y convertir "" en NULL
    for c in COLUMNAS:
        df = df.withColumn(c, F.expr(f"nullif(trim({c}), '')"))

    # 2) conversiones seguras: devuelven NULL si no se pueden convertir
    df = (df
        .withColumn("quantity_i", F.expr("try_cast(quantity AS INT)"))
        .withColumn("price_d",    F.expr("try_cast(price_per_unit AS DECIMAL(10,2))"))
        .withColumn("ts",         F.expr("try_cast(transaction_timestamp AS TIMESTAMP)")))

    # 3) motivo de rechazo: se registra el primero que falle, en el orden acordado
    obligatorio_nulo = (F.col("transaction_id").isNull() | F.col("product_id").isNull()
                        | F.col("store_id").isNull() | F.col("quantity").isNull()
                        | F.col("price_per_unit").isNull() | F.col("transaction_timestamp").isNull())

    motivo = (F.when(obligatorio_nulo, "campo_obligatorio_nulo")
               .when(F.col("quantity_i").isNull(), "cantidad_no_numerica")
               .when(F.col("price_d").isNull(), "precio_no_numerico")
               .when(F.col("ts").isNull(), "fecha_invalida")
               .when(F.col("quantity_i") < 0, "quantity_negativa")
               .when(F.col("quantity_i") == 0, "quantity_cero"))

    return df.withColumn("quarantine_reason", motivo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2: preparar los dos DataFrames de salida
# MAGIC Hash de contenido, deduplicación dentro del lote y comparación contra Silver.

# COMMAND ----------

def preparar(df):
    df = clasificar(df)

    df = df.withColumn(
        "row_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in COLUMNAS]), 256)
    )

    ok    = df.filter("quarantine_reason IS NULL")
    malas = df.filter("quarantine_reason IS NOT NULL")

    # --- deduplicación dentro del lote ---
    por_id   = Window.partitionBy("transaction_id")
    ordenado = Window.partitionBy("transaction_id").orderBy("ingestion_timestamp", "source_file")

    ok = (ok
        .withColumn("n_hashes", F.size(F.collect_set("row_hash").over(por_id)))
        .withColumn("rn", F.row_number().over(ordenado))
        .withColumn("quarantine_reason",
            F.when(F.col("n_hashes") > 1, "transaction_id_conflictivo")
             .when(F.col("rn") > 1, "duplicado_exacto"))
        .drop("n_hashes", "rn"))

    # --- comparación contra lo que ya está en Silver ---
    en_silver = (df.sparkSession.table("workspace.silver.sales")
                   .select("transaction_id", F.col("row_hash").alias("hash_silver")))

    ok = (ok.join(en_silver, "transaction_id", "left")
            .withColumn("quarantine_reason", F.coalesce(
                F.col("quarantine_reason"),
                F.when(F.col("hash_silver").isNotNull() & (F.col("hash_silver") == F.col("row_hash")), "duplicado_exacto")
                 .when(F.col("hash_silver").isNotNull() & (F.col("hash_silver") != F.col("row_hash")), "transaction_id_conflictivo")))
            .drop("hash_silver"))

    df = ok.unionByName(malas)

    validas    = df.filter("quarantine_reason IS NULL")
    rechazadas = df.filter("quarantine_reason IS NOT NULL")

    silver_df = validas.select(
        "transaction_id", "customer_id", "product_id",
        F.col("quantity_i").alias("quantity"),
        F.col("price_d").alias("price_per_unit"),
        (F.col("quantity_i") * F.col("price_d")).cast("decimal(12,2)").alias("total_amount"),
        F.col("ts").alias("transaction_timestamp"),
        F.to_date("ts").alias("sale_date"),
        "store_id", "payment_method",
        F.col("customer_id").isNull().alias("is_anonymous_customer"),
        "row_hash", "source_file", "ingestion_timestamp"
    )

    cuarentena_df = rechazadas.select(
        *COLUMNAS, "source_file", "ingestion_timestamp", "quarantine_reason",
        F.current_timestamp().alias("quarantined_at")
    )
    return silver_df, cuarentena_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3: escribir cada lote
# MAGIC Primero la cuarentena y después el `MERGE` (ver la nota del inicio).

# COMMAND ----------

def procesar_lote(batch_df, batch_id):
    silver_df, cuarentena_df = preparar(batch_df)
    sesion = batch_df.sparkSession

    # 1) primero la cuarentena: ve Silver tal como estaba antes de este lote
    cuarentena_df.write.mode("append").saveAsTable("workspace.silver.sales_quarantine")

    # 2) después el MERGE a Silver
    silver_df.createOrReplaceTempView("lote_silver")
    sesion.sql("""
        MERGE INTO workspace.silver.sales AS destino
        USING lote_silver AS origen
        ON destino.transaction_id = origen.transaction_id
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

(spark.readStream
    .table("workspace.bronze.sales")
    .writeStream
    .foreachBatch(procesar_lote)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
    .awaitTermination())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comprobación
# MAGIC Silver + cuarentena debe sumar las filas de Bronze (25 con los tres archivos de ejemplo).
# MAGIC
# MAGIC | Forma de ejecutar | Silver | Cuarentena |
# MAGIC |---|---|---|
# MAGIC | Desde cero, los 3 archivos en un solo lote | 17 | 8 (T017 dos veces como `transaction_id_conflictivo`) |
# MAGIC | Por días: días 1 y 2, y después el día 3 | 18 | 7 (el original de T017 se conserva) |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'silver' AS tabla, COUNT(*) AS filas FROM workspace.silver.sales
# MAGIC UNION ALL
# MAGIC SELECT 'cuarentena', COUNT(*) FROM workspace.silver.sales_quarantine;
