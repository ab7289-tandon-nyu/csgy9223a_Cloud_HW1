import json
import logging
import time
from copy import deepcopy
from datetime import datetime

from decimal import Decimal
import click
import boto3

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
    click.echo(
        f"Querying Yelp for:\nlocation: {location}\ncuisine: {cuisine}\nlimit: {limit}")
    api = YelpAPI()

    businesses = api.get_businesses(location, cuisine, limit)

    click.echo(f"Retrieved {len(businesses)} buisnesses.")

    persisted: bool = False
    if persist:
        persisted = persist_businesses(convert(businesses, location, cuisine))
        click.echo(f"Successfully persisted: {persisted}")

    return 0 if not persist or (persist and persisted) else -1


def convert(rest_list: list, location: str, cuisine: str) -> list:

    def mapper(item):
        item['location_ref'] = deepcopy(item['location'])
        item['Location'] = location
        item['Cuisine'] = cuisine
        item['insertedAtTimestamp'] = str(datetime.timestamp(datetime.now()))
        return item

    rest_list = list(map(mapper, rest_list))
    # converts floats to decimal so they work with the DynamoDB api
    new_list = json.loads(json.dumps(rest_list), parse_float=Decimal)
    return new_list


def persist_businesses(biz_list: list) -> bool:
    rest_table = RestaurantTable(boto3.resource("dynamodb"))
    if not rest_table.exists(RestaurantTable.TABLE_NAME):
        click.echo("Unable to connect to the database")
        return False

    click.echo(f"Writing {len(biz_list)} items to the database")

    limit: int = 50
    it: int = 0
    batch: list = []
    if len(biz_list) < limit:
        batch = biz_list
        biz_list.clear()
    else:
        batch = biz_list[:limit]
        del biz_list[:limit]

    while len(batch) != 0:
        click.echo(f"writing batch {it}")
        click.echo(f"batch: {len(batch)}, list: {len(biz_list)}\n")

        time.sleep(5)
        try:
            rest_table.write_batch(batch)
        except:
            click.echo(f"error writing batch: {it}\n")
            time.sleep(30)
        batch = []
        batch = biz_list[:limit]
        del biz_list[:limit]
        it += 1

    click.echo("batch write complete")
    return True


if __name__ == "__main__":
    main()
