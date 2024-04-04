# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

input_schema = StructType(
    [
        StructField("driverId", IntegerType()),
        StructField("driverRef", StringType()),
        StructField("number", IntegerType()),
        StructField("code", StringType()),
        StructField(
            "name",
            StructType(
                [
                    StructField("forename", StringType()),
                    StructField("surname", StringType()),
                ]
            ),
        ),
        StructField("dob", DateType()),
        StructField("nationality", StringType()),
        StructField("url", StringType()),
    ]
)

# COMMAND ----------

df = create_dataframe_from_json("/mnt/bronze/drivers.json", input_schema)
df.display()
df.printSchema()

# COMMAND ----------

df = df.withColumn("forename",df['name']['forename']).withColumn("surname",df['name']['surname'])

# COMMAND ----------

# Rename Column

df = df.withColumnRenamed("driverId", "driver_id")
df = df.withColumnRenamed("driverRef", "driver_ref")

# Add date column in df

cur_date = current_dt()

df = df.withColumn("ingest_date", lit(cur_date))
df.display()

# COMMAND ----------

df.write.mode("overwrite").parquet("/mnt/silver/drivers")
