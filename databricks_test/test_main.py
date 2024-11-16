# Databricks notebook source
#!pip install -r ../requirements.txt

# COMMAND ----------

import requests
from dotenv import load_dotenv

import os

import base64

FILESTORE_PATH = "dbfs:/FileStore/tables/"
CSV_URL = "https://raw.githubusercontent.com/SamanthaSmiling/stats/refs/heads/main/ds_salaries.csv"
SERVER_HOSTNAME = os.getenv("SERVER_HOSTNAME")
load_dotenv()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

load_dotenv()
access_token = os.getenv("ACCESS_TOKEN")
job_id = os.getenv("JOB_ID")
server_h = os.getenv("SERVER_HOSTNAME")

# COMMAND ----------

url = f'https://{server_h}/api/2.0/jobs/run-now'

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
}

# COMMAND ----------

data = {
    'job_id': job_id
}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200:
    print('Job run successfully triggered')
else:
    print(f'Error: {response.status_code}, {response.text}')

# COMMAND ----------

def upload_to_dbfs(local_file, dbfs_path):
    """Uploads a local file to Databricks FileStore."""
    # Open and read the local file as bytes
    with open(local_file, "rb") as f:
        content = f.read()
    
    # Create the file in DBFS
    handle_response = requests.post(
        f"https://{SERVER_HOSTNAME}/api/2.0/dbfs/create",
        headers=headers,
        json={"path": dbfs_path, "overwrite": True}
    )
    if handle_response.status_code != 200:
        print(f"Error creating file in DBFS: {handle_response.text}")
        return
    
    handle = handle_response.json()["handle"]

    # Add the file content in base64-encoded blocks
    block_response = requests.post(
        f"https://{SERVER_HOSTNAME}/api/2.0/dbfs/add-block",
        headers=headers,
        json={
            "handle": handle,
            "data": base64.b64encode(content).decode("utf-8")
        }
    )
    if block_response.status_code != 200:
        print(f"Error adding file block in DBFS: {block_response.text}")
        return

    # Close the file handle
    close_response = requests.post(
        f"https://{SERVER_HOSTNAME}/api/2.0/dbfs/close",
        headers=headers,
        json={"handle": handle}
    )
    if close_response.status_code != 200:
        print(f"Error closing file in DBFS: {close_response.text}")
    else:
        print(f"File successfully uploaded to {dbfs_path}.")

# COMMAND ----------

def extract():
    """Downloads CSV file and uploads it to DBFS."""
    response = requests.get(CSV_URL)
    if response.status_code == 200:
        local_file = "/ds_salaries.csv"
        with open(local_file, "wb") as f:
            f.write(response.content)
        upload_to_dbfs(local_file, f"{FILESTORE_PATH}/ds_salaries.csv")
        print(f"File uploaded to {FILESTORE_PATH}/ds_salaries.csv.")
    else:
        print(f"Failed to download file: {response.status_code}")

# COMMAND ----------

extract()

# COMMAND ----------
"""
Loads data into Spark and writes to Delta tables
"""

from pyspark.sql import SparkSession


# COMMAND ----------

def load():
    """Loads data into Delta tables."""
    spark = SparkSession.builder.appName("TransformLoad").getOrCreate()
    file_path = "dbfs:/FileStore/mini_project11/ds_salaries.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Drop existing table if any
    spark.sql("DROP TABLE IF EXISTS ds_salaries_delta")

    # Save as Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("ds_salaries_delta")
    print("Data loaded into Delta table 'ds_salaries_delta'.")

# COMMAND ----------
load()

# COMMAND ----------
"""
Queries and visualizes data from Delta tables
"""

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


# COMMAND ----------
def query_transform():
    """Runs a predefined query on the Delta table."""
    spark = SparkSession.builder.appName("QueryTransform").getOrCreate()
    query = """
        SELECT work_year, job_title, AVG(salary_in_usd) AS avg_salary
        FROM ds_salaries_delta
        GROUP BY work_year, job_title
        ORDER BY work_year DESC, avg_salary DESC
    """
    df = spark.sql(query)
    df.show()
    return df

# COMMAND ----------

def viz():
    """Generates a bar chart visualization."""
    spark = SparkSession.builder.appName("Visualization").getOrCreate()
    query = """
        SELECT work_year, AVG(salary_in_usd) AS avg_salary
        FROM ds_salaries_delta
        GROUP BY work_year
        ORDER BY work_year
    """
    df = spark.sql(query).toPandas()

    # Bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(df["work_year"], df["avg_salary"], color="skyblue")
    plt.xlabel("Work Year")
    plt.ylabel("Average Salary (USD)")
    plt.title("Average Salary by Work Year")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("salary_trend.png")
    print("Visualization saved as 'salary_trend.png'.")

# COMMAND ----------
query_transform()

# COMMAND ----------

viz()



# COMMAND ----------

