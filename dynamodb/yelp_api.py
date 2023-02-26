import requests
import json
import logging
import os
import click
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from schema import YelpAPI, RestaurantTable

logging = logging.getLogger(__name__)

@click.command()
@click.option("--persist", "-p", type=bool, default=False, help="Toggle whether to persist the results in Dynamodb")
@click.option("--limit", "-l", default=1000)
@click.option("--cuisine", "-c", required=True, help="The desired cuisine")
@click.option("--location", "-loc", required=True, help="The location to search in", default="New York City")
def main(location: str, cuisine: str, limit: int, persist: bool):
    """
    Retrieves business from the Yelp API.

    :param location: '--location', '-loc'
    :param cuisine: '--cuisine', '-c'
    :param limit: '--limit', '-l'
    """
    click.echo(f"Querying Yelp for:\nlocation: {location}\ncuisine: {cuisine}\nlimit: {limit}")
    api = YelpAPI()

    response = api.get_businesses(location, cuisine, limit)
    businesses: list = response.get("businesses")
    click.echo(f"Retrieved {len(businesses)} buisnesses.")

    persisted: bool = False
    if persist:
        persisted = persist_businesses(businesses)
        click.echo(f"Successfully persisted: {persisted}")

    return 0 if not persist or (persist and persisted) else 1

def persist_businesses(biz_list: list) -> bool:
    rest_table = RestaurantTable(boto3.resource("dynamodb"))
    return rest_table.exists(RestaurantTable.TABLE_NAME)
        


if __name__ == "__main__":
    main()

