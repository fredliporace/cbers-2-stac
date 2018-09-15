"""generate_catalog_levels_to_be_upated"""

import os
import boto3

DB_CLIENT = boto3.client('dynamodb')
SQS_CLIENT = boto3.client('sqs')

def get_catalog_levels(item):
    """
    Return the levels to be updated given a STAC item key
    """
    levels = list()
    levels.append('/'.join(item.split('/')[:-1]))
    levels.append('/'.join(item.split('/')[:-2]))
    levels.append('/'.join(item.split('/')[:-3]))
    return levels

class GenerateCatalogLevelsToBeUpdated():
    """
    Reads DynamoDB input table for generated/updated STAC items
    and writes into output DynamoDB table the catalog levels
    that must be updated.
    Items are removed input table after being processed.
    """

    def __init__(self, input_table, output_table, queue, limit=1000):
        """
        Ctor.
        input_table(string): DynamoDB table with STAC items
        output_table(string): DynamoDB table with levels to be updated
        limit(int): number of items read from input table at each request.
        """
        self._levels_to_be_updated = set()
        self._items = list()
        self._input_table = input_table
        self._output_table = output_table
        self._queue = queue
        self._limit = limit

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
            #print(item['stacitem']['S'])
            for level in get_catalog_levels(item['stacitem']['S']):
                self._levels_to_be_updated.add(level)

    def process(self):
        """
        Main processing
        """
        response = DB_CLIENT.scan(
            TableName=self._input_table,
            Limit=self._limit)
        self.__parse_items(response['Items'])
        while 'LastEvaluatedKey' in response:
            response = DB_CLIENT.scan(
                TableName=self._input_table,
                Limit=self._limit,
                ExclusiveStartKey=response['LastEvaluatedKey'])
            self.__parse_items(response['Items'])
        #print(self._levels_to_be_updated)
        #print(self._items)
        # Update catalog level table and send prefix to catalog update queue
        for level in self._levels_to_be_updated:

            if self._output_table:
                response = DB_CLIENT.put_item(
                    TableName=self._output_table,
                    Item={
                        'catalog_level': {"S": level}})

            SQS_CLIENT.send_message(QueueUrl=self._queue,
                                    MessageBody=level)

        # Remove processed items
        for item in self._items:
            response = DB_CLIENT.delete_item(
                TableName=self._input_table,
                Key={
                    'stacitem': {"S": item['stacitem']['S']}})

def handler(event, context):
    """Lambda entry point
    Event keys:
    """

    gcl = GenerateCatalogLevelsToBeUpdated(input_table=os.environ['CATALOG_UPDATE_TABLE'],
                                           output_table=\
                                           os.environ.get('CATALOG_LEVELS_UPDATE_TABLE'),
                                           queue=os.environ['CATALOG_PREFIX_UPDATE_QUEUE'])
    gcl.process()
