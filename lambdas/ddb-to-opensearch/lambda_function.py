import boto3
import requests
import os
from requests.auth import HTTPBasicAuth
from requests_aws4auth import AWS4Auth

region: str = "us-east-1"
service: str = "es"
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                   region, service, session_token=credentials.token)
basicauth = HTTPBasicAuth(os.getenv("OS_USER"), os.getenv("OS_PASSWORD"))

host: str = "https://search-csgy9223a-hw1-dining-wt7iphrqwwnp6pzz4i37djulsq.us-east-1.es.amazonaws.com"
index: str = "restaurants"
datatype: str = "_doc"
url: str = host + "/" + index + "/" + datatype + "/"

headers: dict = {"Content-Type": "application/json"}

# TODO update with newly created credentials

def try_old_image(record):
    try:
        return record['dynamodb']['OldImage']['Cuisine']['S']
    except:
        print(f"OldImage failed for {record}")
        return None


def lambda_handler(event, context):
    print(f"event:\n{event}")
    deleted: int = 0
    inserted: int = 0
    for record in event['Records']:
        print(f"record: {record}")
        id = None
        cuisine = None
        try:
            id: str = record['dynamodb']['Keys']['id']['S']
        except:
            print(f"Unable to parse id from record\n{record}")
            
        try:
            cuisine: str = record['dynamodb']['NewImage']['Cuisine']['S']
        except Exception as e:
            print(f"unable to parse cusine, e {e}")
            cuisine = try_old_image(record)

        if not id:
            continue

        if record['eventName'] == 'REMOVE':
            r = requests.delete(url + id, auth=basicauth)
            deleted += 1
            if r.status_code != 200:
                print(f"{r.status_code} status returned from DEL {url + id}")

        else:
            document = {"id": id, "Cuisine": cuisine }
            r = requests.post(url, json=document, headers=headers, auth=basicauth)
            inserted += 1
            if r.status_code != 200:
                print(f"{r.status_code} returned from POST {url} - json: {document}")
    
    return f"inserted {inserted}, deleted: {deleted}"