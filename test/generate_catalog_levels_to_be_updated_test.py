"""generate_catalog_levels_to_be_updated_test"""

import datetime
import os

# import boto3
import pytest

# from botocore.exceptions import EndpointConnectionError

# Region is required for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

# Dummy credentials
os.environ["AWS_ACCESS_KEY_ID"] = "foobar"
os.environ["AWS_SECRET_ACCESS_KEY"] = "foobar"

# from cbers2stac.generate_catalog_levels_to_be_updated.code import (  # pylint: disable=wrong-import-position
#     GenerateCatalogLevelsToBeUpdated,
# )

# def __init__(self, *args, **kwargs):
#     """Ctor"""

#     super(GenerateCatalogLevelsToBeUpdatedTest,
#           self).__init__(*args, **kwargs)

#     # DynamoDB
#     self.dres_ = None
#     self.db_client_ = None
#     self.table_ = None
#     self.dynamodb_endpoint_ = 'http://localhost:4569'
#     self.table_name_ = "CatalogUpdateTable"

#     # SQS
#     self.sqs_endpoint_ = 'http://localhost:4576'
#     self.sqs_client_ = None
#     self.prefix_update_queue_url_ = None

# def dynamodb_setup():
#     """dynamodb_setup"""

#     # We only try to start DynamoDB if there is
#     # no connection to the server.
#     if not self.dres_:
#         self.dres_ = boto3.resource('dynamodb',
#                                     endpoint_url=self.dynamodb_endpoint_)
#         self.db_client_ = boto3.client('dynamodb',
#                                        endpoint_url=self.dynamodb_endpoint_)
#     try:
#         tables = list(self.dres_.tables.all())
#         #print(tables)
#     except EndpointConnectionError:
#         # Skipping localstack setup when running in CircleCI
#         if 'CI' not in os.environ.keys():
#             infra.start_infra(asynchronous=True,
#                               apis=['dynamodb'])
#             tables = list(self.dres_.tables.all())

#         print(list(self.dres_.tables.all()))

#     if not any(x for x in tables if x.name == self.table_name_):
#         # Create table if not exists
#         self.table_ = self.dres_.create_table(
#             TableName=self.table_name_,
#             KeySchema=[
#                 {
#                     'AttributeName': 'stacitem',
#                     'KeyType': 'HASH'
#                 }
#             ],
#             AttributeDefinitions=[
#                 {
#                     'AttributeName': 'stacitem',
#                     'AttributeType': 'S'
#                 }
#             ],
#             ProvisionedThroughput={
#                 'ReadCapacityUnits': 1000,
#                 'WriteCapacityUnits': 1000,
#             }
#         )
#         print("Table status:", self.table_.table_status)
#     else:
#         self.table_ = self.dres_.Table(self.table_name_)

# def sqs_setup():
#     """sqs_setup"""

#     if not self.sqs_client_:
#         self.sqs_client_ = boto3.client('sqs',
#                                         endpoint_url=self.sqs_endpoint_)
#     try:
#         queues = self.sqs_client_.list_queues()
#     except EndpointConnectionError:
#         # Skipping localstack setup when running in CircleCI
#         if 'CI' not in os.environ.keys():
#             infra.start_infra(asynchronous=True,
#                               apis=['sqs'])
#             queues = self.sqs_client_.list_queues()
#     # Make sure there are no queues
#     self.assertFalse('' in queues)

#     # Create queues for testing
#     response = self.sqs_client_.\
#         create_queue(QueueName='CatalogPrefixUpdateQueue')
#     self.prefix_update_queue_url_ = response['QueueUrl']

# def setUp():
#     """setUp"""
#     self.sqs_setup()
#     self.dynamodb_setup()

# def tearDown():
#     if 'CI' not in os.environ.keys():
#         infra.stop_infra()


TEST_TABLE_NAME = "CatalogUpdateTable"


@pytest.mark.dynamodb_table_args(
    {
        **{
            "TableName": TEST_TABLE_NAME,
            "KeySchema": [{"AttributeName": "stacitem", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "stacitem", "AttributeType": "S"}
            ],
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 1000,
                "WriteCapacityUnits": 1000,
            },
        }
    }
)
def test_process(dynamodb_table):
    """process_test"""

    db_table = dynamodb_table

    stacitems = [
        "CBERS4/AWFI/141/123/CBERS_4_AWFI_20160506_141_123_L2.json",
        "CBERS4/AWFI/182/123/CBERS_4_AWFI_20170128_182_123_L4.json",
        "CBERS4/PAN10M/163/121/CBERS_4_PAN10M_20171020_163_121_L2.json",
        "CBERS4/PAN10M/163/122/CBERS_4_PAN10M_20171020_163_122_L2.json",
        "CBERS4/PAN10M/163/123/CBERS_4_PAN10M_20171020_163_123_L2.json",
        "CBERS4/PAN10M/163/124/CBERS_4_PAN10M_20171020_163_124_L2.json",
        "CBERS4/PAN10M/163/125/CBERS_4_PAN10M_20171020_163_125_L2.json",
        "CBERS4/PAN10M/163/126/CBERS_4_PAN10M_20171020_163_126_L2.json",
        "CBERS4/PAN10M/167/127/CBERS_4_PAN10M_20170912_167_127_L2.json",
        "CBERS4/PAN5M/111/082/CBERS_4_PAN5M_20180325_111_082_L2.json",
        "CBERS4/PAN5M/158/130/CBERS_4_PAN5M_20170217_158_130_L4.json",
        "CBERS4/MUX/098/097/CBERS_4_MUX_20150507_098_097_L2.json",
        "CBERS4/MUX/161/136/CBERS_4_MUX_20161027_161_136_L2.json",
    ]

    for stacitem in stacitems:
        db_table.put_item(
            Item={"stacitem": stacitem, "datetime": str(datetime.datetime.now())}
        )

    assert len(db_table.scan()["Items"]) == len(stacitems)

    # gcl = GenerateCatalogLevelsToBeUpdated(
    #     input_table=TEST_TABLE_NAME,
    #     output_table=None,
    #     queue=self.prefix_update_queue_url_,
    #     iterations=11,
    #     sqs_client=self.sqs_client_,
    #     db_client=self.db_client_)
    # gcl.process()

    # # Table should be empty after call
    # self.assertEqual(len(self.db_client_.scan(TableName=self.table_name_,
    #                                           Limit=20)['Items']), 0)
    # all_messages = list()
    # while True:
    #     messages = self.sqs_client_.\
    #         receive_message(QueueUrl=self.prefix_update_queue_url_,
    #                         MaxNumberOfMessages=10)
    #     try:
    #         all_messages.append([msg['Body'] for msg in messages['Messages']])
    #     except KeyError:
    #         break
    #     handles = [{'Id': msg['MessageId'],
    #                 'ReceiptHandle': msg['ReceiptHandle']} \
    #                for msg in messages['Messages']]
    #     self.sqs_client_.delete_message_batch(QueueUrl=self.prefix_update_queue_url_,
    #                                           Entries=handles)
    # flat = [item for sublist in all_messages for item in sublist]
    # self.assertEqual(len(flat), 25)
