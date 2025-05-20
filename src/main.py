from pyspark.sql import SparkSession
from src.utils.config_loader import load_config
from src.utils.config_loader import load_config
from src.etl.reader import read_csv
from src.etl.transformer import transform_data
from src.etl.writer import write_parquet
from src.utils.logger import get_logger


def main():
    logger = get_logger("Main")
    spark = SparkSession.builder.appName("PySparkProjectTest1").getOrCreate()
    # spark = SparkSession.builder \
    #     .appName("PySparkProjectTest1") \
    #     .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    #     .config("spark.hadoop.fs.local.impl.disable.cache", "true") \
    #     .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    #     .getOrCreate()

    config = load_config("config/config.yaml")
    logger.info("Loaded config: %s", config)

    df = read_csv(spark, config["input_path"])
    logger.info("Read input data")

    transformed_df = transform_data(df)
    logger.info("Transformed data")

    write_parquet(transformed_df, config["output_path"])
    logger.info("Wrote output data")

    spark.stop()

if __name__ == "__main__":
    main()

