"""
Queries and visualizes data from Delta tables
"""

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


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
