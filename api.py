import re
import csv
import json
import logging
import requests
import pandas as pd
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

app = FastAPI()

INPUT_FILE = "/srv/nfs/compute/home/viyauser1/log_output.csv"
OUTPUT_FILE = "/srv/nfs/compute/home/viyauser1/hourly_counts.csv"
JOB_DEF_ID = "bde3569c-f025-4955-b24e-8a88477083f5"
SAS_SERVER = "http://trck1076843.trc.sas.com"
CLIENT_TOKEN = "eaa38662-5188-11f0-8610-0610c8c9ea11"
USERNAME = "viyauser1"
PASSWORD = "viyauser1"
CLIENT_ID = "python_c"
CLIENT_SECRET = "python_s"

# Configure logging
logging.basicConfig(filename="record.log", level=logging.INFO, format="%(asctime)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

# Endpoint to return the contents of the .env file
@app.get("/.env", response_class=PlainTextResponse)
async def get_env_file():
    try:
        with open(".env", "r") as f:
            env_content = f.read()
        return env_content
    except FileNotFoundError:
        return ".env file not found."

# Endpoint to update hour_count
@app.post("/hourUpdate")
async def record_json():
    INPUT_FILE = "/srv/nfs/compute/home/viyauser1/log_output.csv"
    OUTPUT_FILE = "/srv/nfs/compute/home/viyauser1/hourly_counts.csv"
    hour_count(INPUT_FILE, OUTPUT_FILE)
    
    JOB_DEF_ID = "bde3569c-f025-4955-b24e-8a88477083f5"
    SAS_SERVER = "http://trck1076843.trc.sas.com"
    CLIENT_TOKEN = "eaa38662-5188-11f0-8610-0610c8c9ea11"
    USERNAME = "viyauser1"
    PASSWORD = "viyauser1"
    CLIENT_ID = "python_c"
    CLIENT_SECRET = "python_s"
    
    client_access_token = get_client_token(SAS_SERVER, CLIENT_TOKEN, CLIENT_ID)
    register_client(SAS_SERVER, CLIENT_ID, CLIENT_SECRET, client_access_token)
    access_token = get_access_token(SAS_SERVER, CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD)
    execute_job(JOB_DEF_ID, SAS_SERVER, access_token)

# Endpoint to record JSON object to logs
@app.post("/record")
async def record_json(request: Request):
    data = await request.json()
    logging.info(json.dumps(data))
    
    log_to_csv()
    INPUT_FILE = "/srv/nfs/compute/home/viyauser1/log_output.csv"
    OUTPUT_FILE = "/srv/nfs/compute/home/viyauser1/hourly_counts.csv"
    hour_count(INPUT_FILE, OUTPUT_FILE)
    
    JOB_DEF_ID = "bde3569c-f025-4955-b24e-8a88477083f5"
    SAS_SERVER = "http://trck1076843.trc.sas.com"
    CLIENT_TOKEN = "eaa38662-5188-11f0-8610-0610c8c9ea11"
    USERNAME = "viyauser1"
    PASSWORD = "viyauser1"
    CLIENT_ID = "python_c"
    CLIENT_SECRET = "python_s"
    
    client_access_token = get_client_token(SAS_SERVER, CLIENT_TOKEN, CLIENT_ID)
    register_client(SAS_SERVER, CLIENT_ID, CLIENT_SECRET, client_access_token)
    access_token = get_access_token(SAS_SERVER, CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD)
    execute_job(JOB_DEF_ID, SAS_SERVER, access_token)
    return {"status": "recorded", "data": data}

def log_to_csv():
    # Read from the source file
    with open("record.log", "r", encoding="utf-8") as f:
        log_data = f.readlines()

    # Parse log entries
    entries = []
    for line in log_data:
        match = re.match(r'^(.*) - ({.*})$', line.strip())
        if match:
            timestamp = match.group(1)
            json_str = match.group(2).replace('\\', '\\\\')  # Escape backslashes
            json_data = json.loads(json_str)
            entries.append([timestamp, json_data["user"], json_data["namespace_path"]])

    # Write to CSV
    with open("/srv/nfs/compute/home/viyauser1/log_output.csv", "w", newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "user", "namespace_path"])
        writer.writerows(entries)

def execute_job(JOB_DEF_ID, SAS_SERVER, access_token):

    url = SAS_SERVER + "/jobExecution/jobs"

    payload={"name": "auto_k8s_info log update execution",
            "description": "Execution of auto_k8s_info log update",
            "jobDefinitionUri": "/jobDefinitions/definitions/" + JOB_DEF_ID
            }

    headers = {
    'Authorization': 'Bearer ' + access_token,
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=json.dumps(payload), verify=False).json()

def hour_count(INPUT_FILE, OUTPUT_FILE):
    # Read the CSV and parse timestamp
    df = pd.read_csv(INPUT_FILE, parse_dates=["timestamp"])

    # Round timestamps down to the start of the hour
    df["hour"] = df["timestamp"].dt.floor("h")

    # Get min timestamp and current time (rounded to hour)
    min_time = df["hour"].min()
    now = datetime.now().replace(minute=0, second=0, microsecond=0)

    # Create a complete hourly range
    all_hours = pd.date_range(start=min_time, end=now, freq="h")

    # Count occurrences per hour
    hourly_counts = df["hour"].value_counts().sort_index()

    # Build result DataFrame
    result_df = pd.DataFrame({"hour": all_hours})
    result_df["count"] = result_df["hour"].map(hourly_counts).fillna(0).astype(int)

    # Save to CSV
    result_df.to_csv(OUTPUT_FILE, index=False)

def get_client_token(SAS_SERVER, CLIENT_TOKEN, CLIENT_ID):
  url = SAS_SERVER + "/SASLogon/oauth/clients/consul?callback=false&serviceId=" + CLIENT_ID
  payload = {}
  headers = {
    "X-Consul-Token": CLIENT_TOKEN
  }
  response = requests.request("POST", url, headers=headers, data = payload, verify=False).json()
  client_access_token = response["access_token"]
  print(f"client_access_token: {client_access_token}")
  return client_access_token

def register_client(SAS_SERVER, CLIENT_ID, CLIENT_SECRET, client_access_token):

  url = SAS_SERVER + "/SASLogon/oauth/clients"

  payload = {"CLIENT_ID": CLIENT_ID, 
            "CLIENT_SECRET": CLIENT_SECRET,
            "scope": ["SASAdministrators", "uaa.admin"], 
            "resource_ids": "none", 
            "authorities": ["uaa.none"], 
            "authorized_grant_types": ["PASSWORD"],
            "access_token_validity": 36000}

  headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + client_access_token
  }
  response = requests.post(url, headers = headers, json = payload, verify=False)
  print(response)


## Get final token for further calls

def get_access_token(SAS_SERVER, CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD):
  url = SAS_SERVER + "/SASLogon/oauth/token"

  data = {
      'grant_type': 'PASSWORD',
      'USERNAME': USERNAME,
      'PASSWORD': PASSWORD
  }

  headers = {'Accept': 'application/json'}

  response = requests.post(url, headers=headers, data=data, auth=(CLIENT_ID, CLIENT_SECRET), verify=False).json()

  access_token = response["access_token"]
  print(f"access_token: {access_token}")

  return access_token