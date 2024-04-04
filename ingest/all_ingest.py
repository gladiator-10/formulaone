# Databricks notebook source
dbutils.notebook.run("circuit_ingest",0)

# COMMAND ----------

dbutils.notebook.run("constructors_ingest",0)

# COMMAND ----------

dbutils.notebook.run("drivers_ingest",0)

# COMMAND ----------

dbutils.notebook.run("laptimes_ingest",0)

# COMMAND ----------

dbutils.notebook.run("pitstops_ingest",0)

# COMMAND ----------

dbutils.notebook.run("qualifying_ingest",0)

# COMMAND ----------

dbutils.notebook.run("race_ingest",0)

# COMMAND ----------

dbutils.notebook.run("results_ingest",0)
