# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

results_df = spark.read.parquet("/mnt/silver/results")
races_df = spark.read.parquet("/mnt/silver/races")
driver_df = spark.read.parquet("/mnt/silver/drivers")
constructors_df = spark.read.parquet("/mnt/silver/constructors")

# COMMAND ----------

results_df1 = results_df.select("race_id","driver_id","constructor_id","points")
races_df1 = races_df.select("race_id","year")
driver_df1 = driver_df.withColumn("name",concat("forename",lit(" "),"surname")).select("driver_id","name")
constructors_df1 = constructors_df.select("constructor_id","constructor_ref")

# COMMAND ----------

res_df = results_df1.join(races_df1, "race_id", "left")
res1_df = res_df.groupBy("driver_id", "year").agg(sum("points").alias("pts"))

res2_df = res1_df.groupBy("year").agg(max("pts").alias("pts"))

res3_df = res1_df.alias("res1").join(res2_df.alias("res2"), (col("res1.pts") == col("res2.pts")) & (col("res1.year") == col("res2.year")), "inner")
res3_df.select(col("res1.driver_id"), col("res1.year"), col("res1.pts"))

res3_df.join(driver_df1,"driver_id","inner").select("driver_id","name",col("res1.year"),col("res1.pts")).orderBy(col("res1.year")).display()


# COMMAND ----------

res_df = results_df1.join(races_df1, "race_id", "left")
res1_df = res_df.groupBy("constructor_id", "year").agg(sum("points").alias("pts"))

res2_df = res1_df.groupBy("year").agg(max("pts").alias("pts"))

res3_df = res1_df.alias("res1").join(res2_df.alias("res2"), (col("res1.pts") == col("res2.pts")) & (col("res1.year") == col("res2.year")), "inner")
res3_df.select(col("res1.constructor_id"), col("res1.year"), col("res1.pts"))

res3_df.join(constructors_df1,"constructor_id","inner").select("constructor_id","constructor_ref",col("res1.year"),col("res1.pts")).orderBy(col("res1.year")).display()
