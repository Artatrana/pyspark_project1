from pyspark.sql import functions as F

def transform_data(df):
    return (
        df.withColumn("amount_double",F.col("amount").cast("double"))
        .groupBy("category")
        .agg(F.sum("amount_double").alias("total_amount"))
    )