# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
    DateType
)

from datetime import datetime
# from pyspark.sql.functions import lit, col, concat
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

#Create Dataframe

def create_dataframe_from_csv(input_path, input_schema, header_status = True):
    """
    This Function will create dataframe from input csv file and input schema
    """

    df = spark.read.csv(input_path, schema = input_schema, header = header_status)
    return df

# COMMAND ----------

def create_dataframe_from_json(input_path, input_schema):
    """
    This Function will create dataframe from input json file and input schema
    """

    df = spark.read.json(input_path, schema = input_schema)
    return df

# COMMAND ----------

def create_dataframe_from_multiline_json(input_path, input_schema):
    """
    This Function will create dataframe from input multiline json file and input schema
    """

    df = spark.read.json(input_path, schema = input_schema, multiLine = True)
    return df

# COMMAND ----------

#Current Date

def current_dt():
    """
    This function will return current date in 'YYYY-MM-DD' format
    """

    curr_dt = datetime.today().strftime("%Y-%m-%d")
    return curr_dt
