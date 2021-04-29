"""
populate_reconcile_queue_test
"""

import pathlib
from test.utils import check_queue_size

import pytest

from cbers2stac.populate_reconcile_queue.code import populate_queue_with_subdirs


@pytest.mark.s3_bucket_args("cbers-stac")
@pytest.mark.sqs_queue_args("queue")
def test_populate_queue_with_subdirs(s3_bucket, sqs_queue):
    """
    test_populate_queue_with_subdirs
    """

    s3_client, s3_resource = s3_bucket  # pylint: disable=unused-variable
    queue = sqs_queue

    fixture_prefix = "test/fixtures/cbers_pds_bucket_structure/"
    paths = pathlib.Path(fixture_prefix).rglob("*")
    jpegs = [str(f.relative_to(fixture_prefix)) for f in paths if "jpg" in str(f)]
    for jpeg in jpegs:
        s3_client.upload_file(
            Filename=fixture_prefix + "/" + jpeg, Bucket="cbers-stac", Key=jpeg
        )

    populate_queue_with_subdirs(
        bucket="cbers-stac", prefix="CBERS4/MUX/", queue=queue.url
    )

    # Not all subdirs contain .jpg files, MUX/058 and MUX/059 do
    # not contain and hence are not uploaded to the test S3 bucket
    check_queue_size(sqs_queue, 6)
