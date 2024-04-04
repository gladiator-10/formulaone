# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

input_schema = StructType(
    [
        StructField("resultId", IntegerType()),
        StructField("raceId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("constructorId", IntegerType()),
        StructField("number", IntegerType()),
        StructField("grid", IntegerType()),
        StructField("position", IntegerType()),
        StructField("positionText", StringType()),
        StructField("positionOrder", IntegerType()),
        StructField("points", FloatType()),
        StructField("laps", IntegerType()),
        StructField("time", StringType()),
        StructField("milliseconds", IntegerType()),
        StructField("fastestLap", IntegerType()),
        StructField("rank", IntegerType()),
        StructField("fastestLapTime", StringType()),
        StructField("fastestLapSpeed", StringType()),
        StructField("statusId", IntegerType())
    ]
)

# COMMAND ----------

df = create_dataframe_from_json("/mnt/bronze/results.json",input_schema)
df.display()
df.printSchema()

# COMMAND ----------

#Rename Column

df = df.withColumnRenamed("resultId","result_id")
df = df.withColumnRenamed("raceId","race_id")
df = df.withColumnRenamed("driverId","driver_id")
df = df.withColumnRenamed("constructorId","constructor_id")
df = df.withColumnRenamed("positionText","position_text")
df = df.withColumnRenamed("positionOrder","position_order")
df = df.withColumnRenamed("fastestLapTime","fastest_lap_time")
df = df.withColumnRenamed("fastestLapSpeed","fastest_lap_speed")
df = df.withColumnRenamed("fastestLap","fastest_lap")

#Add date column in df

cur_date = current_dt()

df = df.withColumn("ingest_date",lit(cur_date))
df.display()

# COMMAND ----------

df.write.mode('overwrite').parquet("/mnt/silver/results")
