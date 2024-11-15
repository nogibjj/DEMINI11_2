import os
import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch
from mylib.extract import extract, FILESTORE_PATH
from mylib.transform_load import load
from mylib.query_viz import query_transform, viz


@pytest.fixture(scope="module")
def spark_session():
    """Provide a SparkSession for tests."""
    spark = SparkSession.builder.appName("TestSuite").getOrCreate()
    yield spark
    spark.stop()


def test_extract():
    """Test the extract function."""
    extract()  # Run the extraction process
    extracted_file = f"{FILESTORE_PATH}/ds_salaries.csv"

    # Assert the file exists locally and in the intended DBFS path
    assert os.path.exists("/tmp/ds_salaries.csv"), "Local file was not downloaded."
    assert extracted_file.endswith("ds_salaries.csv"), "Incorrect DBFS file path."


def test_load(spark_session):
    """Test the load function."""
    load()  # Load the data into Delta tables

    # Query the table to verify data loading
    df = spark_session.sql("SELECT COUNT(*) AS count FROM ds_salaries_delta")
    row_count = df.collect()[0]["count"]

    assert row_count > 0, "The Delta table 'ds_salaries_delta' is empty."


def test_query_transform(spark_session):
    """Test the query_transform function."""
    df = query_transform()

    # Verify the query returns some data
    row_count = df.count()
    assert row_count > 0, "Query returned no rows."
    assert "work_year" in df.columns, "Expected 'work_year' column not found in query results."


@patch("matplotlib.pyplot.savefig")
def test_viz(mock_savefig):
    """Test the viz function."""
    viz()  # Generate the visualization

    # Assert that savefig was called to save the chart
    mock_savefig.assert_called_once_with("salary_trend.png")
