from pyspark.sql import DataFrame
from pyspark.sql.functions import (
col, when, upper, current_timestamp,row_number, avg, count, sum as _sum
)
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

def transform_user_data(df: DataFrame) -> DataFrame:
    """
    Performs data transformation on a user-level dataset.
    Adds age groups, standardizes country, computes activity rank per user,
    and aggregates metrics per country.
    :param df:
    :return:
    """
    # Step 1: Basic transformations
    df_transformed = (
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

    # Step 2: Window function — rank users by last activity
    window_spec = Window.partitionBy("user_id").orderBy(col("last_active_date").desc())

    df_with_rank = df_transformed.withColumn("activity_rank", row_number().over(window_spec))

    # Step 3: Aggregation — compute country-level stats
    df_country_agg = (
        df_with_rank.groupBy("country")
        .agg(
            count("*").alias("num_users"),
            avg("age").alias("avg_age"),
            _sum("purchase_amount").alias("total_purchase")
        )
        .orderBy("country")
    )

    return df_country_agg