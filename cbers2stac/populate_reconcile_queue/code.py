"""populate_reconcile_queue"""

import json
import logging
import os

from cbers2stac.layers.common.utils import get_client

# Get rid of "Found credentials in environment variables" messages
logging.getLogger("botocore.credentials").disabled = True
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def populate_queue_with_subdirs(bucket: str, prefix: str, queue: str):
    """
    Populate queue with messages containing S3 keys from
    'prefix', grouped by the first occurrence of '/' after
    'prefix'. If is required that the prefix ends with '/', which
    means that all the subdirs will be scanned.

    Input:
      bucket(string): ditto
      prefix(string): ditto.
      queue(string): queue url
    """

    # No reason to run the function without scanning subdirs
    assert prefix[-1] == "/"

    dirs = get_client("s3").list_objects_v2(
        Bucket=bucket, Prefix=prefix, Delimiter="/", RequestPayer="requester"
    )

    assert not dirs["IsTruncated"]
    for dir_key in dirs["CommonPrefixes"]:
        LOGGER.info(dir_key["Prefix"])
        get_client("sqs").send_message(
            QueueUrl=queue,
            MessageBody=json.dumps({"bucket": bucket, "prefix": dir_key["Prefix"]}),
        )


def handler(event, context):  # pylint: disable=unused-argument
    """Lambda entry point
    Event keys:
      prefix(string): Common prefix of S3 keys to be sent to queue
    """

    return populate_queue_with_subdirs(
        bucket=event["bucket"],
        prefix=event["prefix"],
        queue=os.environ["RECONCILE_QUEUE"],
    )
