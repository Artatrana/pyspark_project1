from src.etl.reader import read_csv
from src.etl.transformer import transform_data
from src.etl.writer import write_parquet
from pyspark.sql import SparkSession

import os
os.environ['JAVA_HOME'] = '/path/to/your/java/home'
os.environ['SPARK_HOME'] = '/path/to/spark'

def main():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("PySpark ETL Job") \
        .getOrCreate()

    # Read
    input_path = "data/input/sample_data.csv"
    df = read_csv(spark, input_path)

    # Transform
    transformed_df = transform_data(df)

    # Write
    output_path = "data/output/processed_data.parquet"
    write_parquet(transformed_df, output_path)

    spark.stop()


if __name__ == "__main__":
    main()