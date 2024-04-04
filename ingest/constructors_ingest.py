# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

input_schema = StructType(
    [
        StructField("constructorId", IntegerType()),
        StructField("constructorRef", StringType()),
        StructField("name", StringType()),
        StructField("nationality", StringType()),
        StructField("url", StringType()),
    ]
)

# COMMAND ----------

df = create_dataframe_from_json("/mnt/bronze/constructors.json",input_schema)
df.display()
df.printSchema()

# COMMAND ----------

#Rename Column

df = df.withColumnRenamed("constructorId","constructor_id")
df = df.withColumnRenamed("constructorRef","constructor_ref")

#Add date column in df

cur_date = current_dt()

df = df.withColumn("ingest_date",lit(cur_date))
df.display()

# COMMAND ----------

df.write.mode('overwrite').parquet("/mnt/silver/constructors")
