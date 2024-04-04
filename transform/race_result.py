# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

driver_df = spark.read.parquet("/mnt/silver/drivers")
results_df = spark.read.parquet("/mnt/silver/results")
constructor_df = spark.read.parquet("/mnt/silver/constructors")

# COMMAND ----------

driver_df1 = driver_df.withColumn("driver",concat("forename",lit(" "),"surname")).select("driver_id","number","driver")
results_df1 = results_df.select("driver_id","constructor_id","grid","fastest_lap","time","points")
constructor_df1 = constructor_df.select("constructor_id",col("name").alias("team"))

# COMMAND ----------

final_df = results_df1.join(driver_df1,"driver_id","left").join(constructor_df1,"constructor_id","left")
final_df.display()

# COMMAND ----------

results_col_list = ["driver","number","team","grid","fastest_lap","time","points"]
final_df = final_df.select(results_col_list)

# COMMAND ----------

final_df.write.mode('overwrite').parquet("/mnt/gold/race")
