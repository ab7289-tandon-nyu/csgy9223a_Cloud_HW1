
import boto3
from botocore.exceptions import ClientError
import logging
import requests
import os

logger = logging.getLogger(__name__)


class YelpAPI:
    def __init__(self, base_url: str = "https://api.yelp.com/v3/") -> None:
        self.base_url: str = base_url

        self.headers: dict = {"Accept": "Application/json"}
        self.get_businesses_api: str = "businesses/search"
        self.get_buisness_api: str = "businesses/{}"

    def get_businesses(self, location: str, cuisine: str, limit: int) -> dict:
        api_key = os.getenv("YELP_API_KEY")

        params = {
            "location": location.replace(" ", "+"),
            "term": cuisine.replace(" ", "+"),
            "limit": limit,
        }

        url: str = self.base_url + self.get_businesses_api

        auth = {"Authorization": f"Bearer {api_key}"}

        result: list = []

        if limit > 50:
            # need to paginate
            params["limit"] = 50  # max limit for yelp API is 50
            params["offset"] = 0

            while params["offset"] < limit:
                logger.info(
                    f"calling with offset: {params['offset']}, limit: {limit}")
                response = requests.get(url, params=params, headers={
                                        **self.headers, **auth})
                response.raise_for_status()
                response_obj: dict = response.json()
                result.extend(response_obj.get("businesses"))
                params["offset"] += 50

        else:
            response = requests.get(url, params=params, headers={
                                    **self.headers, **auth})
            response.raise_for_status()
            response: dict = response.json()
            result = response.get("businesses")

        return result


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

    def write_batch(self, restaurants):
        """
        Fills an Amazon DynamoDB table with the specified data, using the Boto3
        Table.batch_writer() function to put the items in the table.
        Inside the context manager, Table.batch_writer builds a list of
        requests. On exiting the context manager, Table.batch_writer starts sending
        batches of write requests to Amazon DynamoDB and automatically
        handles chunking, buffering, and retrying.
        :param movies: The data to put in the table. Each item must contain at least
                       the keys required by the schema that was specified when the
                       table was created.
        """
        try:
            with self.table.batch_writer() as writer:
                for restaurant in restaurants:
                    writer.put_item(Item=restaurant)
        except ClientError as err:
            logger.error(
                "Couldn't load data into table %s. Here's why: %s: %s", self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def add_restaurant(self, restaurant: dict):
        """
        Adds a restaurant to the table.
        """
        try:
            self.table.put_item(
                Item=restaurant)
        except ClientError as err:
            logger.error(
                "Couldn't add restaurant %s to table %s. Here's why: %s: %s",
                restaurant.get("name"), self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    # snippet-end:[python.example_code.dynamodb.PutItem]
