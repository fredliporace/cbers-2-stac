"""generate_catalog_levels_to_be_upated"""

import logging
import os
from typing import List, Set

from cbers2stac.layers.common.utils import get_client

# Get rid of "Found credentials in environment variables" messages
logging.getLogger("botocore.credentials").disabled = True
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def get_catalog_levels(item: str) -> List[str]:
    """
    Return the levels to be updated given a STAC item key
    """
    levels = []
    levels.append("/".join(item.split("/")[:-1]))
    levels.append("/".join(item.split("/")[:-2]))
    levels.append("/".join(item.split("/")[:-3]))
    return levels


class GenerateCatalogLevelsToBeUpdated:
    """
    Reads DynamoDB input table for generated/updated STAC items
    and writes into output queue the catalog levels
    that must be updated.
    Items are removed input table after being processed.

    iterations: number of table scans to be performed
    """

    def __init__(  # pylint: disable=too-many-arguments
        self, input_table, output_table, queue, limit=1000, iterations=100,
    ):
        """
        Ctor.
        input_table(string): DynamoDB table with STAC items
        output_table(string): DynamoDB table with levels to be updated
        limit(int): number of items read from input table at each request.
        queue: URL for output SQS queue, where levels to be updated will be placed
        """
        self._levels_to_be_updated: Set[str] = set()
        self._items = []
        self._input_table = input_table
        self._output_table = output_table
        self._queue: str = queue
        self._limit = limit
        self._iterations: int = iterations

    @property
    def items(self):
        """
        Getter
        """
        return self._items

    @property
    def levels_to_be_updated(self):
        """
        Getter
        """
        return self._levels_to_be_updated

    def __parse_items(self, items):
        """
        Receives a list of DynamoDB items, extracting the catalog
        levels for each one.
        """
        self._items.extend(items)
        for item in items:
            # print(item['stacitem']['S'])
            for level in get_catalog_levels(item["stacitem"]["S"]):
                self._levels_to_be_updated.add(level)

    def process(self):
        """
        Main processing
        """
        response = get_client("dynamodb").scan(
            TableName=self._input_table, Limit=self._limit
        )
        self.__parse_items(response["Items"])
        iterations = 1
        while "LastEvaluatedKey" in response and iterations <= self._iterations:
            response = get_client("dynamodb").scan(
                TableName=self._input_table,
                Limit=self._limit,
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            self.__parse_items(response["Items"])
            iterations += 1
        # print(self._levels_to_be_updated)
        LOGGER.info("Number of stacitems: %d", len(self._items))
        LOGGER.info(
            "Number of levels to be updated: %d", len(self._levels_to_be_updated)
        )
        LOGGER.info("Start sending SQS messages")
        # Update catalog level table and send prefix to catalog update queue
        entries = []
        for level in self._levels_to_be_updated:

            # This is only executed if the table is defined, currently
            # not used
            if self._output_table:
                response = get_client("dynamodb").put_item(
                    TableName=self._output_table, Item={"catalog_level": {"S": level}}
                )

            entries.append({"Id": str(len(entries)), "MessageBody": level})
            # get_client("sqs").send_message(QueueUrl=self._queue,
            #                         MessageBody=level)
            if len(entries) == 10:
                get_client("sqs").send_message_batch(
                    QueueUrl=self._queue, Entries=entries
                )
                entries.clear()
        if entries:
            get_client("sqs").send_message_batch(QueueUrl=self._queue, Entries=entries)
            entries.clear()

        # Remove processed items
        LOGGER.info("Start deleting stacitems")
        for item in self._items:
            response = get_client("dynamodb").delete_item(
                TableName=self._input_table,
                Key={"stacitem": {"S": item["stacitem"]["S"]}},
            )
        LOGGER.info("Finished")


def handler(event, context):  # pylint: disable=unused-argument
    """Lambda entry point
    Event keys:
    """

    gcl = GenerateCatalogLevelsToBeUpdated(
        input_table=os.environ["CATALOG_UPDATE_TABLE"],
        output_table=os.environ.get("CATALOG_LEVELS_UPDATE_TABLE"),
        queue=os.environ["CATALOG_PREFIX_UPDATE_QUEUE"],
        iterations=16,
    )
    gcl.process()
