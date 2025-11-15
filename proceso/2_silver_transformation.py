# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Transformación a Capa Silver
# MAGIC
# MAGIC **Objetivo:** Tomar los datos crudos de la capa Bronze, limpiarlos, transformarlos y enriquecerlos.
# [cite_start]MAGIC **Lenguaje:** PySpark [cite: 17]
# MAGIC
# MAGIC - **Fuentes:** Tablas Delta (bronze_orders, bronze_returns, bronze_people)
# MAGIC - **Destino:** Tabla Delta (silver_orders_enriched)
# MAGIC
# MAGIC **Lógica de Transformación:**
# MAGIC 1.  **Limpieza:** Corregir tipos de datos (fechas, números) y estandarizar nombres de columnas.
# MAGIC 2.  **Enriquecimiento:**
# MAGIC     - Unir `bronze_orders` con `bronze_returns` para marcar órdenes devueltas.
# MAGIC     - Unir el resultado con `bronze_people` para añadir el manager regional.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1: Importar funciones y definir nombres

# COMMAND ----------

# Importar funciones necesarias de PySpark
from pyspark.sql.functions import col, to_date, lower, trim, when, lit, count, isnull, sum
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType, TimestampType

# Nombres de las tablas de origen (Bronze)
bronze_orders_table = "bronze_orders"
bronze_returns_table = "bronze_returns"
bronze_people_table = "bronze_people"

# Nombre de la tabla de destino (Silver)
silver_table_name = "silver_orders_enriched"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2: Leer las tablas de la capa Bronze

# COMMAND ----------

# Leer las tablas Delta de la capa Bronze en DataFrames
df_orders = spark.read.table(bronze_orders_table)
df_returns = spark.read.table(bronze_returns_table)
df_people = spark.read.table(bronze_people_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 3: Limpieza y Estandarización (df_orders)
# MAGIC
# MAGIC Aplicamos las siguientes limpiezas a la tabla de órdenes:
# MAGIC 1.  Renombrar columnas a un formato `snake_case` (ej. "Order ID" -> "order_id").
# MAGIC 2.  Convertir columnas de fecha ("Order Date", "Ship Date") de string a tipo `DateType`.
# MAGIC 3.  Asegurar tipos numéricos correctos para "Sales", "Quantity", "Discount", "Profit".

# COMMAND ----------

# Función para renombrar columnas a snake_case
def to_snake_case(df):
    new_columns = [col(c).alias(c.lower().replace(" ", "_").replace("-", "_")) for c in df.columns]
    return df.select(new_columns)

df_orders_renamed = to_snake_case(df_orders)

# Aplicar limpieza de tipos de datos
df_orders_cleaned = df_orders_renamed.select(
    col("row_id").cast(IntegerType()),
    col("order_id").cast(StringType()),
    to_date(col("order_date"), "M/d/yyyy").alias("order_date"), # Ajustar formato de fecha si es necesario
    to_date(col("ship_date"), "M/d/yyyy").alias("ship_date"),   # Ajustar formato de fecha si es necesario
    col("ship_mode").cast(StringType()),
    col("customer_id").cast(StringType()),
    col("customer_name").cast(StringType()),
    col("segment").cast(StringType()),
    col("country").cast(StringType()),
    col("city").cast(StringType()),
    col("state").cast(StringType()),
    col("postal_code").cast(StringType()),
    col("region").cast(StringType()),
    col("product_id").cast(StringType()),
    col("category").cast(StringType()),
    col("sub_category").cast(StringType()),
    col("product_name").cast(StringType()),
    col("sales").cast(DoubleType()),
    col("quantity").cast(IntegerType()),
    col("discount").cast(DoubleType()),
    col("profit").cast(DoubleType()),
    col("_ingestion_timestamp") # Mantener la columna de metadatos
)

# display(df_orders_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 4: Limpieza y Estandarización (df_returns y df_people)
# MAGIC
# MAGIC Limpiamos las otras dos tablas antes de unirlas.

# COMMAND ----------

# Limpieza de df_returns
df_returns_cleaned = (
    to_snake_case(df_returns)
    .withColumnRenamed("returned", "is_returned") # Renombrar para claridad
    .select(
        col("order_id").cast(StringType()),
        col("is_returned").cast(StringType())
    )
    # Filtrar solo las que sí fueron devueltas
    .where(lower(col("is_returned")) == "yes")
    .distinct() # Asegurarnos que solo haya un registro por orden devuelta
)

# Limpieza de df_people
df_people_cleaned = (
    to_snake_case(df_people)
    .withColumnRenamed("person", "regional_manager") # Renombrar para claridad
    .select(
        col("regional_manager").cast(StringType()),
        col("region").cast(StringType())
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 5: Enriquecimiento (Joins)
# MAGIC
# MAGIC Ahora, unimos las tablas limpias para crear nuestra tabla Silver.

# COMMAND ----------

# 1. Unir Órdenes (tabla principal) con Devoluciones (Left Join)
df_joined_returns = df_orders_cleaned.join(
    df_returns_cleaned,
    on="order_id",
    how="left"
)

# 2. Unir el resultado con Personas/Regiones (Left Join)
df_joined_people = df_joined_returns.join(
    df_people_cleaned,
    on="region",
    how="left"
)

# 3. Crear la columna final 'is_returned' (Boolean)
# Si 'is_returned' (de la tabla returns) no es nulo, significa que SÍ fue devuelta.
df_silver = df_joined_people.withColumn(
    "is_returned",
    when(col("is_returned") == "yes", lit(True)).otherwise(lit(False))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 6: Guardar la tabla en Capa Silver
# MAGIC
# MAGIC Guardamos el DataFrame limpio y enriquecido como una tabla Delta en la capa Silver.

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true") # Buena práctica si añadimos columnas después
    .saveAsTable(silver_table_name)
)

print(f"Tabla '{silver_table_name}' creada exitosamente en la capa Silver.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 7: Verificación
# MAGIC
# MAGIC Verifiquemos la tabla Silver final. Deberíamos ver las columnas `is_returned` y `regional_manager`.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   order_date,
# MAGIC   category,
# MAGIC   region,
# MAGIC   regional_manager, -- Columna enriquecida
# MAGIC   sales,
# MAGIC   profit,
# MAGIC   is_returned       -- Columna enriquecida
# MAGIC FROM ${silver_table_name}
# MAGIC LIMIT 20;