"""stac_endpoint"""

import json
import logging

from cbers2stac.layers.common.utils import get_api_stac_root

# Get rid of "Found credentials in environment variables" messages
logging.getLogger("botocore.credentials").disabled = True
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event, context):  # pylint: disable=unused-argument
    """
    Lambda entry point
    """

    LOGGER.info(event)
    retmsg = {
        "statusCode": "200",
        "body": json.dumps(get_api_stac_root(event, item_search=True), indent=2),
        "headers": {"Content-Type": "application/json",},
    }

    return retmsg
