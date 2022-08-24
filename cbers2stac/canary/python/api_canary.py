# This is based on AWS blueprint code
# pylint: disable-all

import http.client
import json
import os
import urllib.parse

from aws_synthetics.common import synthetics_logger as logger
from aws_synthetics.selenium import synthetics_webdriver as syn_webdriver


def verify_request(method, url, post_data=None, headers={}):
    parsed_url = urllib.parse.urlparse(url)
    user_agent = str(syn_webdriver.get_canary_user_agent_string())
    if "User-Agent" in headers:
        headers["User-Agent"] = " ".join([user_agent, headers["User-Agent"]])
    else:
        headers["User-Agent"] = "{}".format(user_agent)

    logger.info(
        "Making request with Method: '%s' URL: %s: Data: %s Headers: %s"
        % (method, url, json.dumps(post_data), json.dumps(headers))
    )

    if parsed_url.scheme == "https":
        conn = http.client.HTTPSConnection(parsed_url.hostname, parsed_url.port)
    else:
        conn = http.client.HTTPConnection(parsed_url.hostname, parsed_url.port)

    conn.request(method, url, post_data, headers)
    response = conn.getresponse()
    logger.info("Status Code: %s " % response.status)
    logger.info("Response Headers: %s" % json.dumps(response.headers.as_string()))

    if not response.status or response.status < 200 or response.status > 299:
        try:
            logger.error("Response: %s" % response.read().decode())
        finally:
            if response.reason:
                conn.close()
                raise Exception("Failed: %s" % response.reason)
            else:
                conn.close()
                raise Exception("Failed with status code: %s" % response.status)

    response_text = response.read().decode()
    logger.info("Response: %s" % response_text)
    logger.info("HTTP request successfully executed")
    conn.close()
    return json.loads(response_text)


def main():

    url1 = os.environ["ENDPOINT_URL"]
    method1 = "GET"
    postData1 = ""
    headers1 = {}
    resp = verify_request(method1, url1, None, headers1)
    assert resp["features"][0]["stac_version"] == "1.0.0"
    logger.info("Canary successfully executed")


def handler(event, context):
    logger.info("Selenium Python API canary")
    main()
