# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

input_schema = StructType(
    [
        StructField("qualifyId", IntegerType()),
        StructField("raceId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("constructorId", IntegerType()),
        StructField("number", IntegerType()),
        StructField("position", IntegerType()),        
        StructField("q1", StringType()),
        StructField("q2", StringType()),
        StructField("q3", StringType())
    ]
)

# COMMAND ----------

df = create_dataframe_from_multiline_json("/mnt/bronze/qualifying",input_schema)
df.display()
df.printSchema()

# COMMAND ----------

#Rename Column

df = df.withColumnRenamed("qualifyId","qualify_id")
df = df.withColumnRenamed("raceId","race_id")
df = df.withColumnRenamed("driverId","driver_id")
df = df.withColumnRenamed("constructorId","constructor_id")

#Add date column in df

cur_date = current_dt()

df = df.withColumn("ingest_date",lit(cur_date))
df.display()

# COMMAND ----------

df.write.mode('overwrite').parquet("/mnt/silver/qualifying")
