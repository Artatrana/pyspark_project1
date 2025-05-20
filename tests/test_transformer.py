import unittest
from pyspark.sql import SparkSession
from src.etl.transformer import transform_data

class TransformerTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()
        data = [("Books", "10"), ("Books", "15"), ("Electronics", "20")]
        self.df = self.spark.createDataFrame(data,["category", "amount"])

    def test_transform_data(self):
        result_df = transform_data(self.df)
        result = {row["category"]: row["total_amount"] for row in result_df.collect()}
        self.assertEquals(result["Books"],25.0)
        self.assertEquals(result["Electronics"], 20.0)

    def tearDown(self):
        self.spark.stop()



