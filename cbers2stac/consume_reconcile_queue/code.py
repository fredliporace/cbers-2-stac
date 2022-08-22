"""consume_reconcile_queue"""

import json
import os
import re

from cbers2stac.layers.common.utils import get_client


def populate_queue_with_quicklooks(bucket, prefix, suffix, queue):
    """
    Populate queue with items to be processed. The items are obtained
    from bucket/prefix/*/suffix
    """
    suffix = r".*" + suffix
    files = get_client("s3").list_objects_v2(
        Bucket=bucket, Prefix=prefix, RequestPayer="requester"
    )

    while True:
        for file in files["Contents"]:
            if re.search(suffix, file["Key"]):
                # print(file['Key'])
                message = {}
                message["Message"] = json.dumps(
                    {
                        "Records": [
                            {
                                "s3": {
                                    "bucket": {"name": bucket,},
                                    "object": {
                                        "key": file["Key"],
                                        # This is an artificial key to indicate that the item
                                        # should always be reconciled
                                        "reconcile": 1,
                                    },
                                }
                            }
                        ]
                    }
                )
                get_client("sqs").send_message(
                    QueueUrl=queue, MessageBody=json.dumps(message)
                )
        if not files["IsTruncated"]:
            break
        files = get_client("s3").list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            ContinuationToken=files["NextContinuationToken"],
            RequestPayer="requester",
        )


def handler(event, context):  # pylint: disable=unused-argument
    """Lambda entry point
    Event keys:
      prefix(string): Common prefix of S3 keys to be sent to queue
    """

    if "queue" in event:
        # Consume payload data from job queue, one job only
        response = get_client("sqs").receive_message(QueueUrl=event["queue"])
        receipt_handle = response["Messages"][0]["ReceiptHandle"]
        msgp = json.loads(response["Messages"][0]["Body"])
        populate_queue_with_quicklooks(
            bucket=msgp["bucket"],
            prefix=msgp["prefix"],
            suffix=r"\.(jpg|png)",
            queue=os.environ["NEW_SCENES_QUEUE"],
        )
        # r_params.append(json.loads(response['Messages'][0]['Body']))
        # print(json.dumps(response, indent=2))
        get_client("sqs").delete_message(
            QueueUrl=event["queue"], ReceiptHandle=receipt_handle
        )

    else:
        # Lambda called from SQS trigger
        for record in event["Records"]:
            msgp = json.loads(record["body"])
            populate_queue_with_quicklooks(
                bucket=msgp["bucket"],
                prefix=msgp["prefix"],
                suffix=r"\.(jpg|png)",
                queue=os.environ["NEW_SCENES_QUEUE"],
            )
