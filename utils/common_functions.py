# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)

from datetime import datetime
from pyspark.sql.functions import lit

# COMMAND ----------

#Create Dataframe

def create_dataframe(input_path, input_schema):
    """
    This Function will create dataframe from input csv file and input schema
    """

    df = spark.read.csv(input_path, schema = input_schema, header = True)
    return df
