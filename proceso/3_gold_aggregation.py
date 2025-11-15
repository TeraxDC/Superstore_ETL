# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Agregación a Capa Gold
# MAGIC
# MAGIC **Objetivo:** Crear tablas agregadas (Data Marts) optimizadas para el análisis y la visualización (BI).
# MAGIC **Lenguaje:** PySpark
# MAGIC
# MAGIC - **Fuente:** Tabla Delta (silver_orders_enriched)
# MAGIC - **Destino:** Tablas Delta (gold_sales_by_category, gold_regional_performance)
# MAGIC
# MAGIC Estas tablas serán las que se conecten a Power BI[cite: 13].

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1: Importar funciones y definir nombres

# COMMAND ----------

# Importar funciones necesarias de PySpark
from pyspark.sql.functions import col, sum, avg, count, countDistinct, month, year, round

# Nombre de la tabla de origen (Silver)
silver_table_name = "silver_orders_enriched"

# Nombres de las tablas de destino (Gold)
gold_sales_by_category_table = "gold_sales_by_category"
gold_regional_performance_table = "gold_regional_performance"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2: Leer la tabla de la capa Silver

# COMMAND ----------

# Leer la tabla Silver limpia y enriquecida
df_silver = spark.read.table(silver_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 3: Crear Agregado 1: `gold_sales_by_category`
# MAGIC
# MAGIC Esta tabla responderá a: "¿Cuánto vendemos y ganamos por categoría y subcategoría?"

# COMMAND ----------

df_sales_by_category = (
    df_silver.groupBy("category", "sub_category")
    .agg(
        round(sum("sales"), 2).alias("total_sales"),
        round(sum("profit"), 2).alias("total_profit"),
        round(avg("discount"), 3).alias("average_discount"),
        countDistinct("order_id").alias("total_orders")
    )
    .orderBy(col("total_profit").desc())
)

# Guardar la primera tabla Gold
(
    df_sales_by_category.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_sales_by_category_table)
)

print(f"Tabla '{gold_sales_by_category_table}' creada exitosamente en la capa Gold.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 4: Crear Agregado 2: `gold_regional_performance`
# MAGIC
# MAGIC Esta tabla responderá a: "¿Cómo es el rendimiento (ventas, ganancia, devoluciones) por región y manager?"

# COMMAND ----------

df_regional_performance = (
    df_silver
    # Añadimos columnas de año y mes para análisis temporal
    .withColumn("order_year", year(col("order_date")))
    .withColumn("order_month", month(col("order_date")))
    .groupBy(
        "region",
        "regional_manager",
        "order_year",
        "order_month"
    )
    .agg(
        round(sum("sales"), 2).alias("total_sales"),
        round(sum("profit"), 2).alias("total_profit"),
        countDistinct("order_id").alias("total_orders"),
        # Contar cuántas órdenes fueron devueltas
        count(when(col("is_returned") == True, 1)).alias("returned_orders")
    )
    .orderBy(col("order_year").desc(), col("order_month").desc(), col("total_profit").desc())
)

# Guardar la segunda tabla Gold
(
    df_regional_performance.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_regional_performance_table)
)

print(f"Tabla '{gold_regional_performance_table}' creada exitosamente en la capa Gold.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 5: Verificación
# MAGIC
# MAGIC Verifiquemos las tablas Gold finales. Estas son las que conectarías a Power BI.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Revisar el primer agregado (Ventas por Categoría)
# MAGIC SELECT * FROM ${gold_sales_by_category_table} LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Revisar el segundo agregado (Rendimiento Regional)
# MAGIC SELECT * FROM ${gold_regional_performance_table} LIMIT 10;