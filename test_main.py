import sys
from unittest.mock import MagicMock
import requests
from dotenv import load_dotenv
import os

sys.modules["dbutils"] = MagicMock()

"""
Test databricks fucntionaility
"""


DATBRICKS_HOST = os.getenv("DATBRICKS_HOST", "dbc-c95fb6bf-a65d.cloud.databricks.com")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", "dapi265dbf9df531b4323b162d622cb136e1")

headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/tables"
url = f"https://{server_h}/api/2.0"

# Function to check if a file path exists and auth settings still work
def check_filestore_path(path, headers):
    url = f"https://{DATBRICKS_HOST}/api/2.0/dbfs/get-status?path={path}"
    try:
        response = requests.get(url, headers=headers)
        return response.status_code == 200
    except Exception as e:
        print(f"Error checking file path: {e}")
        return False



# Test if the specified FILESTORE_PATH exists
def test_databricks():
    headers = {"Authorization": f"Bearer {access_token}"}
    assert check_filestore_path(FILESTORE_PATH, headers) is True


if __name__ == "__main__":
    test_databricks()
