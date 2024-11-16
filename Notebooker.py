# Databricks notebook source
import os

os.environ["SERVER_HOSTNAME"] = "dbc-c95fb6bf-a65d.cloud.databricks.com"
os.environ["ACCESS_TOKEN"] = "dapi265dbf9df531b4323b162d622cb136e1"
os.environ["JOB_ID"] = "1004965230252512"


# COMMAND ----------

import os

print("SERVER_HOSTNAME:", os.getenv("SERVER_HOSTNAME"))
print("ACCESS_TOKEN:", os.getenv("ACCESS_TOKEN"))



# COMMAND ----------

!curl -X GET https://dbc-c95fb6bf-a65d.cloud.databricks.com/api/2.0/clusters/list \ -H "Authorization: Bearer dapi265dbf9df531b4323b162d622cb136e1"


# COMMAND ----------

!curl -X GET "https://dbc-c95fb6bf-a65d.cloud.databricks.com/api/2.0/jobs/run-now" -H "Authorization: Bearer dapi265dbf9df531b4323b162d622cb136e1"

