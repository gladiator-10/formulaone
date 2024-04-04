# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

input_schema = StructType(
    [
        StructField("raceId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("stop", IntegerType()),
        StructField("lap", IntegerType()),        
        StructField("time", StringType()),
        StructField("duration", StringType()),
        StructField("milliseconds", IntegerType())
    ]
)

# COMMAND ----------

df = create_dataframe_from_multiline_json("/mnt/bronze/pit_stops.json",input_schema)
df.display()
df.printSchema()

# COMMAND ----------

#Rename Column

df = df.withColumnRenamed("raceId","race_id")
df = df.withColumnRenamed("driverId","driver_id")

#Add date column in df

cur_date = current_dt()

df = df.withColumn("ingest_date",lit(cur_date))
df.display()

# COMMAND ----------

df.write.mode('overwrite').parquet("/mnt/silver/pitstops")
