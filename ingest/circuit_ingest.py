# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC 1. Read CSV File
# MAGIC 2. Apply Schema for it
# MAGIC 3. Rename and Add column Based on Requirement

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

input_schema = StructType(
    [
        StructField("circuitId", IntegerType()),
        StructField("circuitRef", StringType()),
        StructField("name", StringType()),
        StructField("location", StringType()),
        StructField("country", StringType()),
        StructField("lat", FloatType()),
        StructField("lng", FloatType()),
        StructField("alt", IntegerType()),
        StructField("url", StringType()),
    ]
)

# COMMAND ----------

df = create_dataframe("/mnt/bronze/circuits.csv",input_schema)
df.display()
df.printSchema()

# COMMAND ----------

#Rename Column

df = df.withColumnRenamed("circuitId","circuit_id")
df = df.withColumnRenamed("circuitRef","circuit_ref")

#Add date column in df

cur_date = datetime.today().strftime('%Y-%m-%d')

df = df.withColumn("ingest_date",lit(cur_date))
df.display()

# COMMAND ----------

df.write.parquet("/mnt/silver/circuits")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/silver"))
