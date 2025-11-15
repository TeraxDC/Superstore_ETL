# Databricks notebook source
# MAGIC %md
# MAGIC # Reversión de la Capa Silver (Rollback)
# MAGIC
# MAGIC **Objetivo:** Restaurar la tabla `silver_orders_enriched` a una versión estable anterior utilizando la característica **Time Travel** de Delta Lake.
# MAGIC
# MAGIC Un rollback es necesario si el último despliegue (la ejecución del notebook Silver) introdujo datos corruptos, nulos inesperados, o un error de lógica.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1: Definir la Tabla Clave

# COMMAND ----------

# Nombre de la tabla de la capa Silver que necesitamos restaurar
silver_table_name = "silver_orders_enriched"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2: Auditar el Historial de la Tabla (Opcional)
# MAGIC
# MAGIC Antes de restaurar, siempre es bueno verificar el historial para identificar la versión correcta a la que queremos volver.
# MAGIC
# MAGIC **Nota:** En un entorno real, la versión o el timestamp se pasarían como parámetros (widgets) al notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Muestra el historial de transacciones para la tabla Silver.
# MAGIC -- Busque la columna 'version' para el número de versión estable anterior.
# MAGIC DESCRIBE HISTORY ${silver_table_name};

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 3: Ejecutar la Reversión (Rollback)
# MAGIC
# MAGIC Asumiremos que la versión anterior estable es la `VERSION 1`. Debes reemplazar este número con la versión real que deseas restaurar (obtenida en el paso 2).

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- ADVERTENCIA: Esta operación borra el historial posterior a la versión especificada.
# MAGIC
# MAGIC -- Reemplazar '1' con la versión estable más reciente
# MAGIC RESTORE TABLE ${silver_table_name} TO VERSION AS OF 1; 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 4: Verificación Final
# MAGIC
# MAGIC Verificamos que la tabla haya sido restaurada correctamente, chequeando los últimos registros. Si el problema era de datos, la verificación mostraría el estado limpio.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM ${silver_table_name};
# MAGIC SELECT * FROM ${silver_table_name} LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC **Rollback Completado:** La tabla **`${silver_table_name}`** ha sido restaurada a la versión **1**.