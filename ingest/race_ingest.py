# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

input_schema = StructType(
    [
        StructField("raceId", IntegerType()),
        StructField("year", IntegerType()),
        StructField("round", IntegerType()),
        StructField("circuitId", IntegerType()),
        StructField("name", StringType()),
        StructField("date", DateType()),
        StructField("time", StringType()),
        StructField("url", StringType()),
        StructField("fp1_date", DateType()),
        StructField("fp1_time", StringType()),
        StructField("fp2_date", DateType()),
        StructField("fp2_time", StringType()),
        StructField("fp3_date", DateType()),
        StructField("fp3_time", StringType()),
        StructField("quali_date", DateType()),
        StructField("quali_time", StringType()),
        StructField("sprint_date", DateType()),
        StructField("sprint_time", StringType()),
    ]
)

# COMMAND ----------

df = create_dataframe_from_csv("/mnt/bronze/races.csv", input_schema)
df.display()
df.printSchema()

# COMMAND ----------

# Rename Column

df = df.withColumnRenamed("circuitId", "circuit_id")
df = df.withColumnRenamed("raceId", "race_id")

# Add date column in df

cur_date = current_dt()

df = df.withColumn("ingest_date", lit(cur_date))
df.display()

# COMMAND ----------

df.write.mode("overwrite").parquet("/mnt/silver/races")

# COMMAND ----------


