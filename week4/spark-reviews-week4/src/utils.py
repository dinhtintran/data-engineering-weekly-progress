from pyspark.sql import SparkSession

def create_spark(app_name: str = "spark-reviews-week4") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark