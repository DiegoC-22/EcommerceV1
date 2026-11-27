# Databricks notebook source
# MAGIC %md
# MAGIC # 00_generate_data: archivos de ejemplo
# MAGIC
# MAGIC Escribe tres CSV de ventas en el Volume de aterrizaje. Cada archivo simula la llegada de un día de ventas.
# MAGIC
# MAGIC | Archivo | Contenido |
# MAGIC |---|---|
# MAGIC | `sales_day_1.csv` | 10 filas limpias |
# MAGIC | `sales_day_2.csv` | 10 filas con casos sucios a propósito (duplicado, cantidad negativa, precio no numérico, fecha inválida, cliente vacío) |
# MAGIC | `sales_day_3.csv` | 5 filas: venta nueva, reenvío idéntico de T011, T017 con otro precio, `store_id` vacío y cliente anónimo |
# MAGIC
# MAGIC **Datos simulados.** Nunca subir datos reales de un empleador a Databricks Free Edition.
# MAGIC
# MAGIC Auto Loader no reprocesa un archivo con el mismo nombre: si cambias el contenido de un día ya ingerido, usa un nombre nuevo.

# COMMAND ----------

raw_path = "/Volumes/workspace/landing/raw_sales"

# COMMAND ----------

sales_day_1 = """transaction_id,customer_id,product_id,quantity,price_per_unit,transaction_timestamp,store_id,payment_method
T001,C101,P001,2,10.50,2025-06-10T09:00:00Z,S01,Credit Card
T002,C102,P003,1,25.00,2025-06-10T09:15:00Z,S02,Cash
T003,C101,P002,3,5.75,2025-06-10T09:30:00Z,S01,Debit Card
T004,C103,P001,1,10.50,2025-06-10T10:00:00Z,S03,Credit Card
T005,C102,P004,2,12.00,2025-06-10T10:45:00Z,S02,Credit Card
T006,C104,P005,1,50.00,2025-06-10T11:00:00Z,S01,Cash
T007,C101,P003,1,25.00,2025-06-10T12:00:00Z,S03,Debit Card
T008,C105,P002,4,5.75,2025-06-10T13:00:00Z,S01,Credit Card
T009,C103,P006,1,75.00,2025-06-10T14:00:00Z,S02,Credit Card
T010,C104,P001,3,10.50,2025-06-10T15:00:00Z,S01,Cash
"""

dbutils.fs.put(f"{raw_path}/sales_day_1.csv", sales_day_1, True)

# COMMAND ----------

sales_day_2 = """transaction_id,customer_id,product_id,quantity,price_per_unit,transaction_timestamp,store_id,payment_method
T011,C106,P001,2,10.50,2025-06-11T09:00:00Z,S01,Credit Card
T012,C107,P002,1,5.75,2025-06-11T09:20:00Z,S02,Cash
T012,C107,P002,1,5.75,2025-06-11T09:20:00Z,S02,Cash
T013,C108,P003,-2,25.00,2025-06-11T10:00:00Z,S01,Debit Card
T014,C109,P004,1,abc,2025-06-11T10:30:00Z,S03,Credit Card
T015,C110,P005,1,50.00,2025-13-45T11:00:00Z,S02,Cash
T016,,P001,1,10.50,2025-06-11T11:30:00Z,S01,Credit Card
T017,C111,P006,1,75.00,2025-06-11T12:00:00Z,S03,Credit Card
T018,C112,P002,3,5.75,2025-06-11T13:00:00Z,S01,Cash
T019,C106,P003,1,25.00,2025-06-11T14:00:00Z,S02,Debit Card
"""

dbutils.fs.put(f"{raw_path}/sales_day_2.csv", sales_day_2, True)

# COMMAND ----------

sales_day_3 = """transaction_id,customer_id,product_id,quantity,price_per_unit,transaction_timestamp,store_id,payment_method
T020,C113,P001,2,10.50,2025-06-12T09:00:00Z,S01,Credit Card
T011,C106,P001,2,10.50,2025-06-11T09:00:00Z,S01,Credit Card
T017,C111,P006,1,80.00,2025-06-11T12:00:00Z,S03,Credit Card
T021,C114,P002,1,5.75,2025-06-12T10:00:00Z,,Cash
T022,,P003,1,25.00,2025-06-12T11:00:00Z,S02,Debit Card
"""

dbutils.fs.put(f"{raw_path}/sales_day_3.csv", sales_day_3, True)

# COMMAND ----------

display(dbutils.fs.ls(raw_path))
