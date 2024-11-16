"""
Loads data into Spark and writes to Delta tables
"""

from pyspark.sql import SparkSession


def load():
    """Loads data into Delta tables."""
    spark = SparkSession.builder.appName("TransformLoad").getOrCreate()
    file_path = "dbfs:/FileStore/tables/ds_salaries.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Drop existing table if any
    spark.sql("DROP TABLE IF EXISTS ds_salaries_delta")

    # Save as Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("ds_salaries_delta")
    print("Data loaded into Delta table 'ds_salaries_delta'.")
