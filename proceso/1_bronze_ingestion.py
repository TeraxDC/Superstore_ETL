# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Ingesta a Capa Bronze
# MAGIC
# MAGIC **Objetivo:** Ingestar los datos crudos (raw) del dataset "Superstore" desde el Storage Account a la capa Bronze en formato Delta Lake.
# MAGIC **Lenguaje:** PySpark [cite: 17]
# MAGIC
# MAGIC - **Fuentes:** CSVs (Orders, Returns, People)
# MAGIC - **Destino:** Tablas Delta (bronze_orders, bronze_returns, bronze_people)
# MAGIC
# MAGIC Esta capa no aplica transformaciones, es un volcado 1:1 de la fuente.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1: Configurar Variables y Ubicaciones
# MAGIC
# MAGIC (Nota: Asumimos que tu Storage Account ya está montado en Databricks en una ruta como `/mnt/raw/superstore/`)

# COMMAND ----------

# Importar funciones necesarias
from pyspark.sql.functions import col, current_timestamp, lit

# Definir las rutas de entrada (donde están tus CSVs crudos)
raw_base_path = "/mnt/raw/superstore/"

# Definir los nombres de las tablas Delta de destino en Unity Catalog (o metastore) [cite: 4]
bronze_orders_table = "bronze_orders"
bronze_returns_table = "bronze_returns"
bronze_people_table = "bronze_people"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2: Ingestar "Orders.csv"

# COMMAND ----------

# Definir el esquema (opcional pero recomendado para robustez)
# Si prefieres inferir el esquema, usa: .option("inferSchema", "true")

# Leer el archivo CSV de Órdenes
orders_df = (
    spark.read
    .format("csv")
    .option("header", "true")      # El CSV tiene encabezados
    .option("inferSchema", "true") # Inferir tipos de datos automáticamente para Bronze
    .load(f"{raw_base_path}/Orders.csv")
)

# (Opcional) Añadir metadatos de ingesta
orders_df_with_metadata = orders_df.withColumn("_ingestion_timestamp", current_timestamp())

# Escribir la tabla en formato Delta Lake en la capa Bronze
(
    orders_df_with_metadata.write
    .format("delta")
    .mode("overwrite")  # Sobrescribir la tabla en cada ejecución (estrategia de ingesta)
    .saveAsTable(bronze_orders_table)
)

print(f"Tabla '{bronze_orders_table}' creada exitosamente en la capa Bronze.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 3: Ingestar "Returns.csv"

# COMMAND ----------

# Leer el archivo CSV de Devoluciones
returns_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{raw_base_path}/Returns.csv")
)

# Añadir metadatos de ingesta
returns_df_with_metadata = returns_df.withColumn("_ingestion_timestamp", current_timestamp())

# Escribir la tabla en formato Delta Lake
(
    returns_df_with_metadata.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(bronze_returns_table)
)

print(f"Tabla '{bronze_returns_table}' creada exitosamente en la capa Bronze.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 4: Ingestar "People.csv" (o "Region.csv" según el dataset)

# COMMAND ----------

# Leer el archivo CSV de Personas/Regiones
people_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{raw_base_path}/People.csv")
)

# Añadir metadatos de ingesta
people_df_with_metadata = people_df.withColumn("_ingestion_timestamp", current_timestamp())

# Escribir la tabla en formato Delta Lake
(
    people_df_with_metadata.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(bronze_people_table)
)

print(f"Tabla '{bronze_people_table}' creada exitosamente en la capa Bronze.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 5: Verificación
# MAGIC
# MAGIC Verifiquemos que las tablas se hayan creado correctamente en el catálogo.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ${bronze_orders_table} LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ${bronze_returns_table} LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ${bronze_people_table} LIMIT 10;