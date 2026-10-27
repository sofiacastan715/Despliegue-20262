# Databricks notebook source
# MAGIC %md
# MAGIC ## Tipificación aeroaves

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW aeronaves_tipadas AS
# MAGIC SELECT DISTINCT
# MAGIC        UPPER(TRIM(aeronave_id)) AS aeronave_id,
# MAGIC        TRIM(modelo) AS modelo,
# MAGIC        TRIM(fabricante) AS fabricante,
# MAGIC        TRY_CAST(anio_fabricacion AS INT) AS anio_fabricacion,
# MAGIC        archivo_origen
# MAGIC FROM aerolinea.bronze.aeronaves;
# MAGIC
# MAGIC SELECT * FROM aeronaves_tipadas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aeropuertos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW aeropuertos_tipados AS
# MAGIC SELECT DISTINCT
# MAGIC        UPPER(TRIM(aeropuerto_id)) AS aeropuerto_id,
# MAGIC        TRIM(nombre) AS nombre, TRIM(ciudad) AS ciudad, TRIM(pais) AS pais,
# MAGIC        TRY_CAST(lat AS DOUBLE) AS lat,
# MAGIC        TRY_CAST(lon AS DOUBLE) AS lon,
# MAGIC        archivo_origen
# MAGIC FROM aerolinea.bronze.aeropuertos;
# MAGIC
# MAGIC SELECT * FROM aeropuertos_tipados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vuelos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vuelos_tipados AS
# MAGIC SELECT DISTINCT
# MAGIC        UPPER(TRIM(vuelo_id)) AS vuelo_id,
# MAGIC        TRY_CAST(fecha AS DATE) AS fecha,
# MAGIC        UPPER(TRIM(origen_id)) AS origen_id,
# MAGIC        UPPER(TRIM(destino_id)) AS destino_id,
# MAGIC        UPPER(TRIM(aeronave_id)) AS aeronave_id,
# MAGIC        LOWER(TRIM(estado)) AS estado,
# MAGIC        TRY_CAST(duracion_min AS INT) AS duracion_min,
# MAGIC        UPPER(TRIM(origen_id)) = UPPER(TRIM(destino_id)) AS ruta_por_revisar,
# MAGIC        archivo_origen
# MAGIC FROM aerolinea.bronze.vuelos;
# MAGIC
# MAGIC SELECT * FROM vuelos_tipados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mantenimientos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW mantenimientos_tipados AS
# MAGIC SELECT DISTINCT
# MAGIC        UPPER(TRIM(mantenimiento_id)) AS mantenimiento_id,
# MAGIC        UPPER(TRIM(aeronave_id)) AS aeronave_id,
# MAGIC        TRY_CAST(fecha AS DATE) AS fecha,
# MAGIC        LOWER(TRIM(tipo)) AS tipo,
# MAGIC        TRY_CAST(costo_usd AS DECIMAL(14,2)) AS costo_usd,
# MAGIC        TRY_CAST(duracion_hr AS DECIMAL(10,2)) AS duracion_hr,
# MAGIC        archivo_origen
# MAGIC FROM aerolinea.bronze.mantenimientos;
# MAGIC
# MAGIC SELECT * FROM mantenimientos_tipados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reglas del contenido

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vuelos_tipados AS
# MAGIC SELECT DISTINCT
# MAGIC        UPPER(TRIM(vuelo_id)) AS vuelo_id,
# MAGIC        TRY_CAST(fecha AS DATE) AS fecha,
# MAGIC        UPPER(TRIM(origen_id)) AS origen_id,
# MAGIC        UPPER(TRIM(destino_id)) AS destino_id,
# MAGIC        UPPER(TRIM(aeronave_id)) AS aeronave_id,
# MAGIC        LOWER(TRIM(estado)) AS estado,
# MAGIC        TRY_CAST(duracion_min AS INT) AS duracion_min,
# MAGIC        archivo_origen
# MAGIC FROM aerolinea.bronze.vuelos;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW control_contenido AS
# MAGIC SELECT 'aeronaves' AS conjunto, COUNT(*) AS errores
# MAGIC FROM aeronaves_tipadas
# MAGIC WHERE aeronave_id IS NULL OR aeronave_id = '' OR modelo IS NULL OR modelo = ''
# MAGIC    OR fabricante IS NULL OR fabricante = '' OR anio_fabricacion IS NULL
# MAGIC    OR anio_fabricacion < 1900 OR anio_fabricacion > YEAR(current_date())
# MAGIC UNION ALL
# MAGIC SELECT 'aeropuertos', COUNT(*) FROM aeropuertos_tipados
# MAGIC WHERE aeropuerto_id IS NULL OR aeropuerto_id = '' OR nombre IS NULL OR nombre = ''
# MAGIC    OR ciudad IS NULL OR ciudad = '' OR pais IS NULL OR pais = ''
# MAGIC    OR lat IS NULL OR lon IS NULL OR isnan(lat) OR isnan(lon)
# MAGIC    OR lat NOT BETWEEN -90 AND 90 OR lon NOT BETWEEN -180 AND 180
# MAGIC UNION ALL
# MAGIC SELECT 'vuelos', COUNT(*) FROM vuelos_tipados
# MAGIC WHERE vuelo_id IS NULL OR vuelo_id = '' OR fecha IS NULL
# MAGIC    OR aeronave_id IS NULL OR aeronave_id = ''
# MAGIC    OR origen_id IS NULL OR origen_id = '' OR destino_id IS NULL OR destino_id = ''
# MAGIC    OR estado IS NULL OR estado NOT IN ('a_tiempo','retrasado','cancelado')
# MAGIC    OR duracion_min < 0
# MAGIC    OR (estado <> 'cancelado' AND (duracion_min IS NULL OR duracion_min = 0))
# MAGIC UNION ALL
# MAGIC SELECT 'mantenimientos', COUNT(*) FROM mantenimientos_tipados
# MAGIC WHERE mantenimiento_id IS NULL OR mantenimiento_id = '' OR fecha IS NULL
# MAGIC    OR aeronave_id IS NULL OR aeronave_id = '' OR tipo IS NULL
# MAGIC    OR tipo NOT IN ('preventivo','correctivo','inspección')
# MAGIC    OR costo_usd IS NULL OR costo_usd < 0 OR duracion_hr IS NULL OR duracion_hr <= 0

# COMMAND ----------

contenido = spark.table("control_contenido")
display(contenido)
assert contenido.filter("errores > 0").count() == 0, "Revisar el contenido antes de publicar Silver."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conflictos por llave

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW control_llaves AS
# MAGIC SELECT 'aeronaves' AS conjunto, COUNT(*) AS errores FROM (
# MAGIC   SELECT aeronave_id FROM aeronaves_tipadas GROUP BY aeronave_id HAVING COUNT(*) > 1
# MAGIC ) a
# MAGIC UNION ALL
# MAGIC SELECT 'aeropuertos', COUNT(*) FROM (
# MAGIC   SELECT aeropuerto_id FROM aeropuertos_tipados GROUP BY aeropuerto_id HAVING COUNT(*) > 1
# MAGIC ) b
# MAGIC UNION ALL
# MAGIC SELECT 'vuelos', COUNT(*) FROM (
# MAGIC   SELECT vuelo_id FROM vuelos_tipados GROUP BY vuelo_id HAVING COUNT(*) > 1
# MAGIC ) c
# MAGIC UNION ALL
# MAGIC SELECT 'mantenimientos', COUNT(*) FROM (
# MAGIC   SELECT mantenimiento_id FROM mantenimientos_tipados GROUP BY mantenimiento_id HAVING COUNT(*) > 1
# MAGIC ) d

# COMMAND ----------

llaves = spark.table("control_llaves")
display(llaves)
assert llaves.filter("errores > 0").count() == 0, "Hay llaves con registros diferentes."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revisión de referencias y coherencia temporal

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW control_referencias AS
# MAGIC SELECT 'vuelo_aeronave' AS regla, COUNT(*) AS errores
# MAGIC FROM vuelos_tipados v LEFT ANTI JOIN aeronaves_tipadas a ON v.aeronave_id = a.aeronave_id
# MAGIC UNION ALL
# MAGIC SELECT 'vuelo_origen', COUNT(*)
# MAGIC FROM vuelos_tipados v LEFT ANTI JOIN aeropuertos_tipados a ON v.origen_id = a.aeropuerto_id
# MAGIC UNION ALL
# MAGIC SELECT 'vuelo_destino', COUNT(*)
# MAGIC FROM vuelos_tipados v LEFT ANTI JOIN aeropuertos_tipados a ON v.destino_id = a.aeropuerto_id
# MAGIC UNION ALL
# MAGIC SELECT 'mantenimiento_aeronave', COUNT(*)
# MAGIC FROM mantenimientos_tipados m LEFT ANTI JOIN aeronaves_tipadas a ON m.aeronave_id = a.aeronave_id
# MAGIC UNION ALL
# MAGIC SELECT 'fabricacion_posterior_vuelo', COUNT(*)
# MAGIC FROM vuelos_tipados v JOIN aeronaves_tipadas a ON v.aeronave_id = a.aeronave_id
# MAGIC WHERE a.anio_fabricacion > YEAR(v.fecha)
# MAGIC UNION ALL
# MAGIC SELECT 'fabricacion_posterior_mantenimiento', COUNT(*)
# MAGIC FROM mantenimientos_tipados m JOIN aeronaves_tipadas a ON m.aeronave_id = a.aeronave_id
# MAGIC WHERE a.anio_fabricacion > YEAR(m.fecha)

# COMMAND ----------

referencias = spark.table("control_referencias")
display(referencias)
assert referencias.filter("errores > 0").count() == 0, "Revisar las referencias o los años de fabricación."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Observaciones visibles

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rutas_por_revisar FROM vuelos_tipados WHERE ruta_por_revisar;
# MAGIC SELECT vuelo_id, fecha, origen_id, destino_id, estado
# MAGIC FROM vuelos_tipados WHERE ruta_por_revisar ORDER BY vuelo_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rutas_por_revisar FROM vuelos_tipados WHERE ruta_por_revisar;
# MAGIC SELECT vuelo_id, fecha, origen_id, destino_id, estado
# MAGIC FROM vuelos_tipados WHERE ruta_por_revisar ORDER BY vuelo_id LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publicación Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE aerolinea.silver.aeronaves USING DELTA AS SELECT * FROM aeronaves_tipadas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE aerolinea.silver.aeropuertos USING DELTA AS SELECT * FROM aeropuertos_tipados

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE aerolinea.silver.vuelos USING DELTA AS SELECT * FROM vuelos_tipados

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE aerolinea.silver.mantenimientos USING DELTA AS SELECT * FROM mantenimientos_tipados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'vuelos' AS conjunto, COUNT(*) AS registros FROM aerolinea.silver.vuelos
# MAGIC UNION ALL SELECT 'aeronaves', COUNT(*) FROM aerolinea.silver.aeronaves
# MAGIC UNION ALL SELECT 'aeropuertos', COUNT(*) FROM aerolinea.silver.aeropuertos
# MAGIC UNION ALL SELECT 'mantenimientos', COUNT(*) FROM aerolinea.silver.mantenimientos

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingesta a capa Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC USE aerolinea.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- tabla silver de aeronave 
# MAGIC create or replace table aeronaves using delta as select * from aeronaves_tipadas;
# MAGIC
# MAGIC -- tabla silver de aeropuertos
# MAGIC create or replace table aeropuertos using delta as select * from aeropuertos_tipados;
# MAGIC
# MAGIC -- tabla silver de vuelos
# MAGIC create or replace table vuelos using delta as select * from vuelos_tipados;
# MAGIC
# MAGIC -- tabla silver de mantenimientos
# MAGIC create or replace table mantenimientos using delta as select * from mantenimientos_tipados;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as rutas_revisadas from vuelos where ruta_por_revisar = false; 