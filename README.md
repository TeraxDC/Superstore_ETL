# 🚀 PROYECTO ETL - DATA LAKEHOUSE SUPERSTORE

## 🎯 1. Resumen y Objetivo del Proyecto

Este repositorio documenta la implementación de un **Pipeline ETL (Extracción, Transformación y Carga)** completo, diseñado y ejecutado en la plataforma **Azure Databricks**.

El proyecto utiliza el **Dataset Superstore** como fuente de datos y aplica la **Arquitectura Medallion (Bronze, Silver, Gold)** para demostrar las buenas prácticas en la ingeniería de datos, asegurando que los datos sean progresivamente limpiados, enriquecidos y optimizados para el consumo de Business Intelligence (BI).

### Tecnologías Clave:

| Tecnología | Rol |
| :--- | :--- |
| **Databricks** | Plataforma central para el desarrollo y ejecución del ETL. |
| **PySpark** | Lenguaje de programación para la manipulación y procesamiento distribuido de datos. |
| **Delta Lake** | Formato de almacenamiento utilizado en todas las capas para garantizar transacciones ACID y control de versiones. |
| **Unity Catalog** | Marco de gobernanza para la gestión centralizada de permisos y metadatos (contexto de implementación). |

---

## 🏗️ 2. Arquitectura de Datos (Medallion)

El pipeline está estructurado para que los datos fluyan secuencialmente a través de tres capas diferenciadas, cada una añadiendo valor y calidad al conjunto de datos:

| Capa | Propósito Principal | Formato / Nivel de Limpieza |
| :--- | :--- | :--- |
| **BRONZE (Raw)** | **Ingesta sin procesar.** Captura los datos crudos del Storage Account. | Datos sin modificar, solo se añade una marca de tiempo de ingesta. |
| **SILVER (Cleaned/Enriched)** | **Calidad y Enriquecimiento.** Limpieza de tipos de datos, estandarización y *joins* de lógica de negocio. | Datos limpios, mapeados a tipos de datos correctos (ej. `DateType`, `DoubleType`). |
| **GOLD (Consumed/Aggregated)** | **Data Marts.** Tablas agregadas y optimizadas, listas para reportes. | Agregaciones a nivel de negocio (ventas totales, ganancias por región/categoría). |

---

## 📁 3. Estructura del Repositorio

La organización del repositorio sigue la modularidad requerida para entornos de producción, facilitando el desarrollo, la seguridad y el despliegue continuo:
/Superstore_ETL
├── reversion/
│   └── rollback_scripts.sql       # Contiene scripts (SQL/PySpark) para revertir cambios de esquema o datos ante un fallo.
├── deploy/
│   └── pipeline.json              # Archivo de configuración para la orquestación (e.g., Azure Data Factory o Databricks Jobs).
├── seguridad/
│   └── grant_permissions.sql      # Scripts SQL para la administración de permisos (GRANTs) sobre las tablas Gold en Unity Catalog.
└── proceso/
    ├── 1_bronze_ingestion.py      # Notebook para la ingesta de datos crudos (Capa Bronze).
    ├── 2_silver_transformation.py # Notebook para la limpieza y enriquecimiento de datos (Capa Silver).
    └── 3_gold_aggregation.py      # Notebook para la agregación final de datos para BI (Capa Gold).


## 🐍 4. Flujo Detallado del Pipeline ETL (Notebooks PySpark)

### 4.1. `1_bronze_ingestion.py`
Este notebook gestiona la ingesta inicial desde la Capa Raw (Storage Account).

* **Fuentes:** `Orders.csv`, `Returns.csv`, `People.csv`.
* **Transformación Clave:** La única transformación es añadir la columna `_ingestion_timestamp` para el linaje de datos.
* **Destino:** Tablas Delta **`bronze_orders`**, **`bronze_returns`**, y **`bronze_people`**.

### 4.2. `2_silver_transformation.py`
Este es el paso central donde se aplica la lógica de negocio y se asegura la calidad del dato.

* **Estandarización:** Se aplica una función para convertir todos los nombres de columna a formato `snake_case`.
* **Validación de Tipos:** Se corrigen explícitamente las columnas de fecha (a `DateType`) y las métricas (Ventas, Ganancia) a `DoubleType` o `IntegerType`.
* **Lógica de Negocio (Enriquecimiento):**
    * **Devoluciones:** Se realiza un **`LEFT JOIN`** entre `bronze_orders` y `bronze_returns` para etiquetar cada orden con un booleano `is_returned`.
    * **Jerarquía:** Se realiza un segundo **`LEFT JOIN`** para adjuntar la columna `regional_manager` a cada fila de orden usando la columna `region`.
* **Destino:** Tabla Delta única **`silver_orders_enriched`**.

### 4.3. `3_gold_aggregation.py`
Esta capa crea los Data Marts de alto nivel para el consumo de BI.

* **Fuente:** Tabla `silver_orders_enriched`.
* **Transformación:** Aplicación de funciones de agregación (`sum`, `avg`, `countDistinct`) y funciones de tiempo (`year`, `month`).

#### Output 1: `gold_sales_by_category`
* **Agregación:** Por `category` y `sub_category`.
* **KPIs:** `total_sales`, `total_profit`, `average_discount`, `total_orders`.

#### Output 2: `gold_regional_performance`
* **Agregación:** Por `region`, `regional_manager`, año y mes (`order_year`, `order_month`).
* **KPIs:** `total_sales`, `total_profit`, y el conteo de órdenes devueltas (`returned_orders`).

---

## 💻 5. Instrucciones de Uso y Conexión BI

1.  **Ejecución:** Los notebooks deben ser ejecutados en el orden `1_bronze` -> `2_silver` -> `3_gold` dentro de un cluster de Databricks.
2.  **Conexión BI:** Las tablas finales (**`gold_sales_by_category`** y **`gold_regional_performance`**) están listas para ser consultadas directamente desde Power BI, aprovechando el conector nativo de Databricks, sin necesidad de pasos intermedios de exportación.