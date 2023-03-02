import boto3
from botocore.exceptions import ClientError
import os
import logging
import requests
from requests.auth import HTTPBasicAuth
from typing import Optional

logger = logging.getLogger(__name__)

REGION: str = "us-east-1"
CLUSTER_HOST: str = "https://search-csgy9223a-hw1-dining-wt7iphrqwwnp6pzz4i37djulsq.us-east-1.es.amazonaws.com"
INDEX: str = "restaurants"
PORT: int = 443


class SesWrapper:
    def __init__(self, ses_resource) -> None:
        self.ses_resource = ses_resource

    def publish(self, address, message) -> str:
        SENDER = "AWS SES <ab7289@nyu.edu>"

        SUBJECT: str = "DiningConcierge SES"
        CHARTSET = "UTF-8"
        try:
            response = self.ses_resource.send_email(
                Destination={
                    "ToAddresses": [
                        address
                    ],
                },
                Message={
                    "Body": {
                        "Text": {
                            "Charset": CHARTSET,
                            "Data": message,
                        }
                    },
                    "Subject": {
                        "Charset": CHARTSET,
                        "Data": SUBJECT,
                    }
                },
                Source=SENDER
            )
        except ClientError as e:
            logger.error("There was an error sending email: %s",
                         e.response["Error"]["Message"])
            raise
        else:
            return response["MessageId"]


class RestaurantTable:

    TABLE_NAME: str = "yelp-restaurants"

    """Encapsulates an Amazon DynamoDB table of restaurant data."""

    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        self.table = None

    def exists(self, table_name):
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.
        :param table_name: The name of the table to check.
        :return: True when the table exists; otherwise, False.
        """
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists

    def get_restaurant(self, id):
        try:
            response = self.table.get_item(Key={"id": id})
        except ClientError as err:
            logger.error(
                "Couldn't retrieve restaurant %s from Table %s. Here's why: %s: %s",
                id, self.table.name,
                err.response["Error"]["Code"], err.response["Error"]["Message"]
            )
            return None
        else:
            return response['Item']


def get_query(cuisine: str):
    """
    Constructs a GraphQL Query used by OpenSearch

    :param cuisine: the cuisine to search for
    """
    query: dict = {
        "query": {
            "term": {
                "Cuisine": {
                    "value": cuisine
                }
            }
        }
    }
    return query


def handle_os_response(response, attributes):
    """
    Handles parsing out the response from OpenSearch

    :param response: the requests response from OpenSearch
    :param attributes: the SQS attributes originall sent
    """
    print(
        f"handle_os_response: response: {response}\nattributes: {attributes}")
    hits_obj = response["hits"]
    hit_count: int = hits_obj["total"]["value"]
    if hit_count == 0:
        logger.error("No hits retrieved")
        send_error(attributes)
    else:
        top_hit = hits_obj["hits"][0]
        logger.info("top hit: %s", top_hit)
        logger.info("id: %s", top_hit['_source']['id'])
        top_id: str = top_hit['_source']['id']
        suggestion: dict = query_db(top_id)
        if suggestion is not None:
            send_message(suggestion, attributes)
        else:
            send_error(attributes)


def send_message(restaurant: dict, attributes: dict) -> None:
    """
    uses the information parsed to send a message to the user

    :param restaurant: the dictionary representing a restaurant returned from dynamodb
    :param attributes: the request attributes retrieved from the SQS queue
    """
    print(f"send_message: {restaurant}, {attributes}")
    ses = SesWrapper(boto3.client("ses"))

    phone: str = attributes["phone"]["stringValue"]
    count: int = attributes["count"]["stringValue"]
    cuisine: str = attributes["cuisine"]["stringValue"]
    date: str = attributes["date"]["stringValue"]
    location: str = attributes["location"]["stringValue"]
    time: str = attributes["time"]["stringValue"]
    email: str = attributes["email"]["stringValue"]

    print(
        f"params: phone: {phone}\ncount: {count}\ncuisine: {cuisine}\ndate: {date}\ntime: {time}")

    rest_name: str = restaurant.get("name")
    loc_obj: dict = restaurant.get("location")
    location_display_name: str = loc_obj["display_address"][0]

    print(
        f"restaurant suggestion: name: {rest_name}, address: {location_display_name}")

    message: str = (
        f"Hello! Here are my {cuisine} restaurant suggestions for {count} "
        f"people, for {date} at {time}: {rest_name}, located at {location_display_name}.\n"
        "Hope you enjoy the suggestions!."
    )

    print(f"Message to send:\n{message}")

    msg_id: str = ses.publish(email, message)
    logger.info("Sent error message.\nMessageId: %s\nBody: %s",
                msg_id, message)


def send_error(attributes: dict) -> None:
    """
    Sends a generic error message to the user

    :param attributes: the request attributes retrieved from the SQS queueu
    """
    print(f"send_error: {attributes}")
    ses = SesWrapper(boto3.client("ses"))

    phone: str = attributes["phone"]["stringValue"]
    count: int = attributes["count"]["stringValue"]
    cuisine: str = attributes["cuisine"]["stringValue"]
    date: str = attributes["date"]["stringValue"]
    location: str = attributes["location"]["stringValue"]
    time: str = attributes["time"]["stringValue"]
    email: str = attributes["email"]["stringValue"]

    message: str = (
        f"Hi there, unfortunately we don't appear to have any suggestions "
        f"for {cuisine} in {location}, for {count} guests on {date} at "
        f"{time}. Please try again when more restaurants have been indexed."
    )

    msg_id: str = ses.publish(email, message)
    logger.info("Sent error message.\nMessageId: %s\nBody: %s",
                msg_id, message)


def query_db(top_id: str) -> Optional[dict]:
    """
    Queries dynamodb for the restaurant represented by the
    id retrieved from OpenSearch

    :param top_id: the unique id of a restaurant
    """
    rest_table = RestaurantTable(boto3.resource("dynamodb"))
    if not rest_table.exists(RestaurantTable.TABLE_NAME):
        logger.error("Unable to connect to the database")

    result = rest_table.get_restaurant(top_id)
    # print(f"returned result {result} from db")
    return result


def lambda_handler(event, context):
    for record in event['Records']:

        attributes = record["messageAttributes"]
        cuisine: str = attributes["cuisine"]["stringValue"]

        os_query = get_query(cuisine)

        headers = {"Content-Type": "application/json"}
        url: str = CLUSTER_HOST + "/" + INDEX + "/" + "_search"

        auth = HTTPBasicAuth(os.getenv("OS_USER"), os.getenv("OS_PASSWORD"))
        response = requests.get(url,
                                headers=headers,
                                json=os_query,
                                auth=auth)

        handle_os_response(response.json(), attributes)
        # print(f"OpenSearch response\n{response}")
