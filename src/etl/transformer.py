from pyspark.sql.functions import col, when, upper, current_timestamp
from pyspark.sql.types import DateType

def transform_data(df):
    return (
        df.withColumn("created_at", col("created_at").cast(DateType()))
          .withColumn(
              "age_group",
              when(col("age") < 30, "Young")
              .when((col("age") >= 30) & (col("age") <= 40), "Adult")
              .otherwise("Senior")
          )
          .withColumn("country", upper(col("country")))
          .withColumn("record_inserted_at", current_timestamp())
    )