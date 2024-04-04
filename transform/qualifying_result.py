# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

driver_df = spark.read.parquet("/mnt/silver/drivers")
qualifying_df = spark.read.parquet("/mnt/silver/qualifying")
constructor_df = spark.read.parquet("/mnt/silver/constructors")

# COMMAND ----------

qualifying_df1 = qualifying_df.select("driver_id","constructor_id",col("q1").alias("qualifying1"),col("q2").alias("qualifying2"), col("q3").alias("qualifying3"))
driver_df1 = driver_df.withColumn("driver",concat("forename",lit(" "),"surname")).select("driver_id","number","driver")
constructor_df1 = constructor_df.select("constructor_id",col("name").alias("team"))

# COMMAND ----------

result_df = qualifying_df1.join(driver_df1,"driver_id","left").join(constructor_df1,"constructor_id","left")

# COMMAND ----------

result_col_list = ["driver","number","team","qualifying1","qualifying2","qualifying3"]
result_df = result_df.select(result_col_list)

# COMMAND ----------

result_df.write.mode('overwrite').parquet("/mnt/gold/qualifying")
