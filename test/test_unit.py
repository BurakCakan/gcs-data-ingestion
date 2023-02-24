import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from datetime import datetime, timezone
from src.sparkUtils import convert_millis_to_datetime

class TestSparkUtils:
    
    @pytest.fixture(scope="class")
    def spark(self):
        return (
            SparkSession.builder.appName("TestSparkUtils")
            .master("local")
            .getOrCreate()
        )

    @pytest.fixture(scope="class")
    def test_data(self, spark):
        data = [
            (1, 1676548408566941),
            (2, 1676548408566941),
            (3, None)
           # (4, 1639000000000),
        ]
        return spark.createDataFrame(data, ["id", "millis"])

    def test_convert_millis_to_datetime(self, spark, test_data):

        df=test_data
        result = df.withColumn("created_ts", convert_millis_to_datetime("millis", "GMT+1")).select("created_ts")

        expected = spark.createDataFrame(
            [
                (1, "2023-02-16T10:53:28.000Z"),
                (2, "2023-02-16T10:53:28.000Z"),
                (3,None)
            ],
            ["id","ts"]).withColumn("new_ts",to_timestamp("ts")).select("new_ts")
        
        assert result.collect() == expected.collect()

