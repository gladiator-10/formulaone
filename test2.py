# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.parquet("/mnt/silver/qualifying")
df1 = spark.read.parquet("/mnt/silver/races")

df2 = df.join(df1,"race_id","inner")
df2 = df2.where(col("year")==2020)
df2.select("race_id","driver_id","q1").orderBy("q1").display()

# COMMAND ----------


