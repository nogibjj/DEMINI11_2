"""
Handles data extraction and storage in DBFS
"""

import requests
import os
from dotenv import load_dotenv
import base64



def check_filestore_path(path, headers, host="dbc-c95fb6bf-a65d.cloud.databricks.com"):
    """
    Check if a given path exists in the Databricks FileStore.
    Args:
        path (str): Path in Databricks FileStore.
        headers (dict): Authorization headers.
        host (str): Hostname for the Databricks workspace.

    Returns:
        bool: True if the path exists, False otherwise.
    """
    try:
        response = requests.get(
            f"https://{host}/api/2.0/dbfs/get-status?path={path}",
            headers=headers
        )
        response.raise_for_status()
        return response.json().get("is_dir", False)
    except requests.exceptions.RequestException as e:
        print(f"Error checking file path: {e}")
        return False


load_dotenv()
FILESTORE_PATH = "dbfs:/FileStore/tables/"
CSV_URL = "https://raw.githubusercontent.com/SamanthaSmiling/stats/refs/heads/main/ds_salaries.csv"
SERVER_HOSTNAME = os.getenv("SERVER_HOSTNAME")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}


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

def extract():
    """Downloads CSV file and uploads it to DBFS."""
    response = requests.get(CSV_URL)
    if response.status_code == 200:
        local_file = "FileStore/ds_salaries.csv"
        with open(local_file, "wb") as f:
            f.write(response.content)
        upload_to_dbfs(local_file, f"{FILESTORE_PATH}/ds_salaries.csv")
        print(f"File uploaded to {FILESTORE_PATH}/ds_salaries.csv.")
    else:
        print(f"Failed to download file: {response.status_code}")
