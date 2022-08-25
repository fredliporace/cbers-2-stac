"""
consume_reconcile_queue_test
"""

import json
from test.process_new_scene_queue_test import populate_bucket_test_case_1
from test.utils import check_queue_size

import pytest

from cbers2stac.consume_reconcile_queue.code import populate_queue_with_quicklooks


@pytest.mark.s3_bucket_args("cbers-stac")
@pytest.mark.sqs_queue_args("queue")
def test_populate_queue_with_subdirs(s3_bucket, sqs_queue):
    """
    test_populate_queue_with_subdirs
    """

    s3_client, _ = s3_bucket
    queue = sqs_queue

    populate_bucket_test_case_1(bucket="cbers-stac", s3_client=s3_client)

    populate_queue_with_quicklooks(
        bucket="cbers-stac", prefix="CBERS4/MUX/", queue=queue.url, suffix=r"\.jpg"
    )

    check_queue_size(sqs_queue, 218)

    message = sqs_queue.receive_messages(AttributeNames=["All"])
    msgdict = json.loads(message[0].body)
    msg = json.loads(msgdict["Message"])
    obj = msg["Records"][0]["s3"]["bucket"]
    # Check for new bucket key
    assert obj["name"] == "cbers-stac"
