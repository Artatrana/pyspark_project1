from pyspark.sql import SparkSession

def read_csv(spark: SparkSession, path: str):
    return spark.read.option("header", True).csv(path)
