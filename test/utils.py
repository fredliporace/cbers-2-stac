"""utils for testing"""

import os
import sys

from retry import retry


def is_circleci() -> bool:
    """True if executing under circleci"""
    twd = os.environ.get("TOX_WORK_DIR")
    if twd is not None:
        return twd == "/home/runner/work/cbers04aonaws/cbers04aonaws/.tox"
    return False


@retry(AssertionError, tries=10, delay=10)
def check_bucket_files(s3_client, bucket_name, files_no) -> None:
    """Helper to retry checking the number of files in bucket"""
    objs = s3_client.list_objects(Bucket=bucket_name)
    # Key may does not exist initially when bucket is empty
    assert objs.get("Contents")
    assert len(objs["Contents"]) == files_no


@retry(AssertionError, tries=5, delay=2)
def check_queue_size(queue, messages_no) -> None:
    """Helper to retry checking the queue size"""
    queue.load()
    assert int(queue.attributes["ApproximateNumberOfMessages"]) == messages_no


def lambda_response_ok(response: dict) -> bool:
    """
    Check lambda function response, printing body if
    the call failed
    """
    # import pdb; pdb.set_trace()
    failed = response.get("FunctionError")
    if failed:
        print(response["Payload"].read().decode("utf-8"), file=sys.stderr)
    return failed is None
