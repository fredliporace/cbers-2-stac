"""reindex_stac_items_test."""

import pathlib
from test.utils import check_queue_size

import pytest

from cbers2stac.reindex_stac_items.code import send_stac_items_to_queue


@pytest.mark.s3_bucket_args("cbers-stac")
@pytest.mark.sqs_queue_args("queue")
def test_send_stac_items_to_queue(s3_bucket, sqs_queue):
    """test_send_stac_items_to_queue."""

    s3_client, s3_resource = s3_bucket  # pylint: disable=unused-variable
    queue = sqs_queue

    fixture_prefix = "test/fixtures/cbers_stac_bucket_structure/"
    paths = pathlib.Path(fixture_prefix).rglob("*")
    jsons = [str(f.relative_to(fixture_prefix)) for f in paths if "json" in str(f)]
    for jfile in jsons:
        s3_client.upload_file(
            Filename=fixture_prefix + "/" + jfile, Bucket="cbers-stac", Key=jfile
        )

    send_stac_items_to_queue(
        bucket="cbers-stac", queue=queue.url, prefix="CBERS4/AWFI/001/",
    )
    check_queue_size(sqs_queue, 4)

    send_stac_items_to_queue(
        bucket="cbers-stac", queue=queue.url, prefix="CBERS4/AWFI/"
    )
    # We must consider the 4 messages added in the previous test
    check_queue_size(sqs_queue, 52 + 4)
