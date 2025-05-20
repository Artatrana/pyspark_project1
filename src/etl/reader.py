from pyspark.sql import SparkSession


# this will read a .csv file
def read_csv(spark: SparkSession, path: str):
    return spark.read.option("header", True).csv(path)
