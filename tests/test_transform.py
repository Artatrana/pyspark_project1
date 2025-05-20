import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DateType
from src.etl.transformer import transform_data


class TestTransformData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TransformDataTest").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transform_data(self):
        # Sample input data
        input_data = [
            (1, "Alice", 34, "usa", "2024-01-01"),
            (2, "Bob", 28, "canada", "2024-01-03"),
            (3, "Charlie", 45, "uk", "2024-01-05"),
            (4, "Diana", 25, "germany", "2024-01-07")
        ]

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("country", StringType(), True),
            StructField("created_at", StringType(), True)
        ])

        input_df = self.spark.createDataFrame(data=input_data, schema=schema)

        # Transform
        result_df = transform_data(input_df)

        # Collect relevant columns for testing
        result_rows = result_df.select("id", "age_group", "country").collect()

        # Expected values
        expected_rows = [
            (1, "Adult", "USA"),
            (2, "Young", "CANADA"),
            (3, "Senior", "UK"),
            (4, "Young", "GERMANY")
        ]

        # Validate output
        for actual, expected in zip(result_rows, expected_rows):
            self.assertEqual(actual.id, expected[0])
            self.assertEqual(actual.age_group, expected[1])
            self.assertEqual(actual.country, expected[2])

        # Validate created_at is converted to DateType
        self.assertIsInstance(result_df.schema["created_at"].dataType, DateType)

        # Validate record_inserted_at column exists
        self.assertIn("record_inserted_at", result_df.columns)


if __name__ == "__main__":
    unittest.main()
