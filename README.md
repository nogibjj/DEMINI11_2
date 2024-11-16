[![CI](https://github.com/nogibjj/DEMINI11_2/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/DEMINI11_2/actions/workflows/cicd.yml)

## DEmini11 : Data Pipeline with Databricks

**Overview:**
The Data Extraction and Transformation Pipeline project aims to retrieve and process tennis match data from Kaggle datasets, which restored in gitlab and then DataBricks. 

**Key Components:**
1. **Data Extraction:**
   - Utilizes the `requests` library to fetch tennis match data from specified URLs.
   - Downloads and stores the data in the Databricks FileStore.

2. **Databricks Environment Setup:**
   - Establishes a connection to the Databricks environment using environment variables for authentication (SERVER_HOSTNAME and ACCESS_TOKEN).

3. **Data Transformation and Load**
    - Transform the csv file into a Spark dataframe which is then converted into a Delta Lake Table and stored in the Databricks environement

4. **Query Transformation and Vizulization:**
   - Defines a Spark SQL query to perform a predefined transformation on the retrieved data.
   - Uses the predifined transformation Spark dataframe to create vizualizations
   - ![image](https://github.com/user-attachments/assets/f279149d-7b0b-49dc-b5ad-3edb80aaca7e)


5. **File Path Checking for `make test`:**
   - Implements a function to check if a specified file path exists in the Databricks FileStore.
   - As the majority of the functions only work exclusively in conjunction with Databricks, the Github environment cannot replicate and do not have access to the data in the Databricks workspace. I have opted to test whether I can still connect to the Databricks API. 
   - Utilizes the Databricks API and the `requests` library.



**Preparation:**
1. Create a Databricks workspace 
2. Connect Github account to Databricks Workspace 
3. Create global init script for cluster start to store enviornment variables 
4. Create a Databricks cluster that supports Pyspark 
5. Clone repo into Databricks workspace 
6. Create a job on Databricks to build pipeline 
7. Extract task (Data Source): `mylib/extract.py`
8. Transform and Load Task (Data Sink): `mylib/transform_load.py`
9. Query and Viz Task: `mylib/query_viz.py`

## Job Run from Automated Trigger:
<img width="1077" alt="image" src="https://github.com/user-attachments/assets/4c11ee98-8dd7-4193-b15b-b2d6dd31ae16">


## Check format and test errors
1. Open codespaces or run repo locally with terminal open 
2. Format code `make format`
3. Lint code `make lint`

## Sample Viz from Query: 
![image](https://github.com/user-attachments/assets/d48e673b-bbb1-4afd-a0ab-f76b895ddf4f)
![image](https://github.com/user-attachments/assets/2593208f-1707-4881-b5d8-ecf352378453)
![image](https://github.com/user-attachments/assets/d03074c5-3b93-420b-a317-d4963a21b9fa)


## References

https://github.com/nogibjj/python-ruff-template 


