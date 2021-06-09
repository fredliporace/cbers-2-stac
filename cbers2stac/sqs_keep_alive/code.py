"""sqs_keep_alive"""

import os

import boto3

SQS_CLIENT = boto3.client("sqs")


def sqs_keep_alive(sqs_csl):
    """
    Get attributes from SQS to keep them active for Cloudwatch
    Input:
      sqs_list: comma separated list of SQS URLs
    """

    for sqs_list in sqs_csl.split(","):
        SQS_CLIENT.get_queue_attributes(
            QueueUrl=sqs_list, AttributeNames=["ApproximateNumberOfMessages"]
        )


def handler(event, context):  # pylint: disable=unused-argument
    """
    Lambda entry point
    """

    return sqs_keep_alive(sqs_csl=os.environ["QUEUES_URLS"])
