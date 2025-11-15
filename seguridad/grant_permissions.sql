-- Otorgar permisos de LECTURA en la capa GOLD a los analistas (Power BI)
GRANT SELECT ON TABLE gold_sales_by_category TO `analyst_group`;
GRANT SELECT ON TABLE gold_monthly_performance TO `analyst_group`;

-- Otorgar permisos de LECTURA en la capa SILVER a los científicos de datos
GRANT SELECT ON TABLE silver_orders_enriched TO `data_scientist_group`;

-- Los ingenieros de datos (tu rol) tendrían permisos de MODIFICACIÓN
GRANT MODIFY ON ALL TABLES IN SCHEMA default TO `data_engineer_group`;