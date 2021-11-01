"""Reindex STAC items from bucket to ES."""

import json
import logging
import os

from cbers2stac.layers.common.utils import get_client, get_resource

# Get rid of "Found credentials in environment variables" messages
logging.getLogger("botocore.credentials").disabled = True
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def send_stac_items_to_queue(bucket: str, queue: str, prefix: str) -> None:
    """send_stac_items_to_queue.

    Args:
      filter_params are passed directly to list_objects_v2 paginator.
    """
    sqs_queue = get_resource("sqs").Queue(queue)
    s3_client = get_client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    for result in paginator.paginate(
        Bucket=bucket, PaginationConfig={"PageSize": 1000}, Prefix=prefix
    ):
        assert result["KeyCount"] <= 1000
        entries = [
            {
                "Id": str(index),
                "MessageBody": json.dumps(
                    {
                        "Message": json.dumps(
                            json.loads(
                                s3_client.get_object(Bucket=bucket, Key=obj["Key"])[
                                    "Body"
                                ]
                                .read()
                                .decode("utf-8")
                            )
                        )
                    }
                ),
            }
            for index, obj in enumerate(result["Contents"])
            if obj["Key"].split("/")[-1] not in ["catalog.json", "collection.json"]
        ]
        # Send items to queue at max 10 at a time
        chunk_size = 10
        chunked_entries = [
            entries[i * chunk_size : (i + 1) * chunk_size]
            for i in range((len(entries) + chunk_size - 1) // chunk_size)
        ]
        for chunk in chunked_entries:
            response = sqs_queue.send_messages(Entries=chunk)
            assert len(response["Successful"]) == len(chunk)


def populate_queue_with_subdirs(bucket: str, prefix: str, queue: str) -> None:
    """
    Populate queue with messages containing S3 keys from
    'prefix', grouped by the first occurrence of '/' after
    'prefix'. If is required that the prefix ends with '/', which
    means that all the subdirs will be scanned.

    Input:
      bucket(string): STAC bucket
      prefix(string): ditto.
      queue(string): queue url
    """

    # No reason to run the function without scanning subdirs
    assert prefix[-1] == "/"

    dirs = get_client("s3").list_objects_v2(
        Bucket=bucket, Prefix=prefix, Delimiter="/",
    )

    # Paging is not supported here
    assert not dirs["IsTruncated"]
    for dir_key in dirs["CommonPrefixes"]:
        LOGGER.info(dir_key["Prefix"])
        get_client("sqs").send_message(QueueUrl=queue, MessageBody=dir_key["Prefix"])


# def populate_queue_with_stac_items(bucket: str, prefix: str, suffix:str , queue: str) -> None:
#     """
#     Populate queue with STAC items to be indexed. The items are obtained
#     from bucket/prefix/*/suffix
#     """
#     suffix = r".*" + suffix
#     files = get_client("s3").list_objects_v2(
#         Bucket=bucket, Prefix=prefix, RequestPayer="requester"
#     )

#     while True:
#         for file in files["Contents"]:
#             if re.search(suffix, file["Key"]):
#                 # print(file['Key'])
#                 message = dict()
#                 message["Message"] = json.dumps(
#                     {
#                         "Records": [
#                             {"s3": {"object": {"key": file["Key"], "reconcile": 1}}}
#                         ]
#                     }
#                 )
#                 get_client("sqs").send_message(
#                     QueueUrl=queue, MessageBody=json.dumps(message)
#                 )
#         if not files["IsTruncated"]:
#             break
#         files = get_client("s3").list_objects_v2(
#             Bucket=bucket,
#             Prefix=prefix,
#             ContinuationToken=files["NextContinuationToken"],
#             RequestPayer="requester",
#         )


# def handler(event, context):  # pylint: disable=unused-argument
#     """Lambda entry point
#     Event keys:
#       filter_params: **kwargs to list_objects_v2, typically {"Prefix":"CBERS4/AWFI/"}
#     """

#     send_stac_items_to_queue(
#         bucket=os.environ["CBERS_STAC_BUCKET"],
#         queue=os.environ["insert_into_elasticsearch_queue_url"],
#         filter_params=event["filter_params"],
#     )


def consume_stac_reconcile_queue_handler(
    event, context
):  # pylint: disable=unused-argument
    """Lambda entry point.
    Event keys:
      prefix(string): Common prefix of STAC items to be sent to queue
      queue(string): URL of the queue, optional. If set a single message is read
    """

    if "queue" in event:
        # Consume payload data from job queue, one job only
        response = get_client("sqs").receive_message(QueueUrl=event["queue"])
        receipt_handle = response["Messages"][0]["ReceiptHandle"]
        send_stac_items_to_queue(
            bucket=os.environ["CBERS_STAC_BUCKET"],
            prefix=response["Messages"][0]["Body"],
            queue=os.environ["insert_into_elasticsearch_queue_url"],
        )
        # r_params.append(json.loads(response['Messages'][0]['Body']))
        # print(json.dumps(response, indent=2))
        get_client("sqs").delete_message(
            QueueUrl=event["queue"], ReceiptHandle=receipt_handle
        )

    else:
        # Lambda called from SQS trigger
        for record in event["Records"]:
            send_stac_items_to_queue(
                bucket=os.environ["CBERS_STAC_BUCKET"],
                prefix=record["body"],
                queue=os.environ["insert_into_elasticsearch_queue_url"],
            )


def populate_stac_reconcile_queue_handler(
    event, context
):  # pylint: disable=unused-argument
    """Lambda entry point
    Event keys:
      prefix(string): Common prefix of S3 keys to be sent to queue
    """

    return populate_queue_with_subdirs(
        bucket=os.environ["CBERS_STAC_BUCKET"],
        prefix=event["prefix"],
        queue=os.environ["stac_reconcile_queue_url"],
    )
