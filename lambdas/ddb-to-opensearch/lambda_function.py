import boto3
import requests
from requests_aws4auth import AWS4Auth

region: str = "us-east-1"
service: str = "es"
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                   region, service, session_token=credentials.token)

host: str = "https://search-csgy9223a-hw1-dining-wt7iphrqwwnp6pzz4i37djulsq.us-east-1.es.amazonaws.com/"
index: str = "restaurants"
datatype: str = "_doc"
url: str = host + "/" + index + "/" + datatype + "/"

headers: dict = {"Content-Type": "application/json"}


def handler(event, context):
    for record in event['Records']:
        deleted: int = 0
        inserted: int = 0
        id = None
        cuisine = None
        try:
            id: str = record['dynamodb']['Keys']['id']['S']
            cuisine: str = record['dyanmodb']['Keys']['Cuisine']['S']
        except:
            print(f"Unable to parse id or cuisine from record\n{record}")

        if not id or not cuisine:
            continue

        if record['eventName'] == 'REMOVE':
            r = requests.delete(url + id, auth=awsauth)
            deleted += 1
            if r.status_code != 200:
                print(f"{r.status_code} status returned from DEL {url + id}")

        else:
            document = {"id": id, "Cuisine": cuisine}
            r = requests.post(url, json=document, headers=headers)
            inserted += 1
            if r.status_code != 200:
                print(f"{r.status_code} returned from POST {url} - json: {document}")
