from pyspark.sql import DataFrame

def write_parquet(df:DataFrame, path: str):
    df.write.mode("overwrite").parquet(path)

