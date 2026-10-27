# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS aerolinea;
# MAGIC USE CATALOG aerolinea;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creación de esquemas arquitectura medallion

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS aerolinea.fuentes;
# MAGIC CREATE SCHEMA IF NOT EXISTS aerolinea.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS aerolinea.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS aerolinea.gold;
# MAGIC CREATE VOLUME IF NOT EXISTS aerolinea.fuentes.landing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creación de volumenes 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS aerolinea.fuentes.landing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación de esquemas y volumen

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN aerolinea;
# MAGIC SHOW VOLUMES IN aerolinea.fuentes;