"""populate_reconcile_queue"""

import os

import boto3

SQS_CLIENT = boto3.client("sqs")
S3_CLIENT = boto3.client("s3")


def populate_queue_with_subdirs(bucket, prefix, queue):
    """
    Populate queue with messages containing S3 keys from
    'prefix', gruped by the first occurrence of '/' after
    'prefix'.
    Input:
      bucket(string): ditto
      prefix(string): ditto.
      queue(string): queue url
    """

    dirs = S3_CLIENT.list_objects_v2(
        Bucket=bucket, Prefix=prefix, Delimiter="/", RequestPayer="requester"
    )

    assert not dirs["IsTruncated"]
    for dir_key in dirs["CommonPrefixes"]:
        SQS_CLIENT.send_message(QueueUrl=queue, MessageBody=dir_key["Prefix"])


def handler(event, context):  # pylint: disable=unused-argument
    """Lambda entry point
    Event keys:
      prefix(string): Common prefix of S3 keys to be sent to queue
    """

    return populate_queue_with_subdirs(
        bucket=os.environ["CBERS_PDS_BUCKET"],
        prefix=event["prefix"],
        queue=os.environ["RECONCILE_QUEUE"],
    )
