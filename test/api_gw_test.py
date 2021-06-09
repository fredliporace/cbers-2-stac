"""api_gw_test"""

# Remove warnings when using pytest fixtures
# pylint: disable=redefined-outer-name

import json
from test.conftest import ENDPOINT_URL

# warning disabled, this is used as a pylint fixture
from test.elasticsearch_test import (  # pylint: disable=unused-import
    es_client,
    populate_es_test_case_1,
)
from urllib.parse import urlencode

import boto3
import pytest
import requests


def to_localstack_url(api_id: str, url: str):
    """
    Converts a API GW url to localstack
    """
    return url.replace("4566", f"4566/restapis/{api_id}").replace(
        "dev", "dev/_user_request_"
    )


def api_gw_lambda_integrate_deploy(
    api_client,
    api: dict,
    api_resource: dict,
    lambda_func: dict,
    http_method: str = "GET",
) -> str:
    """
    Integrate lambda with api gw method and deploy api.
    Return the invokation URL
    """
    lambda_integration_arn = (
        "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
        f"{lambda_func['FunctionArn']}/invocations"
    )
    api_client.put_integration(
        restApiId=api["id"],
        resourceId=api_resource["id"],
        httpMethod=http_method,
        type="AWS",
        integrationHttpMethod="POST",
        uri=lambda_integration_arn,
    )
    api_client.create_deployment(
        restApiId=api["id"], stageName="dev",
    )
    return f"http://localhost:4566/restapis/{api['id']}/dev/_user_request_{api_resource['path']}"


@pytest.fixture
def api_gw_method(request):
    """api gw for testing"""

    marker = request.node.get_closest_marker("api_gw_method_args")
    put_method_args = marker.args[0]["put_method_args"]
    put_method_response_args = marker.args[0]["put_method_response_args"]

    api = None

    def fin():
        """fixture finalizer"""
        if api:
            api_client.delete_rest_api(restApiId=api["id"])

    # Hook teardown (finalizer) code
    request.addfinalizer(fin)

    api_client = boto3.client("apigateway", endpoint_url=ENDPOINT_URL)
    api = api_client.create_rest_api(name="testapi")
    root_resource_id = api_client.get_resources(restApiId=api["id"])["items"][0]["id"]
    api_resource = api_client.create_resource(
        restApiId=api["id"], parentId=root_resource_id, pathPart="test"
    )
    api_client.put_method(
        restApiId=api["id"],
        resourceId=api_resource["id"],
        authorizationType="NONE",
        **put_method_args,
    )
    api_client.put_method_response(
        restApiId=api["id"],
        resourceId=api_resource["id"],
        statusCode="200",
        **put_method_response_args,
    )
    return api_client, api, api_resource


@pytest.mark.api_gw_method_args(
    {
        "put_method_args": {"httpMethod": "GET",},
        "put_method_response_args": {"httpMethod": "GET",},
    }
)
@pytest.mark.lambda_function_args(
    {
        "name": "stac_endpoint",
        "handler": "code.handler",
        "environment": {"CBERS_STAC_BUCKET": "bucket",},
        "timeout": 30,
        "layers": (
            {
                "output_dir": "./test",
                "layer_dir": "./cbers2stac/layers/common",
                "tag": "common",
            },
        ),
    }
)
def test_root(api_gw_method, lambda_function):
    """
    test_root_endpoint
    """

    # Based on
    # https://stackoverflow.com/questions/58859917/creating-aws-lambda-integrated-api-gateway-resource-with-boto3

    api_client, api, api_resource = api_gw_method
    lambda_client, lambda_func = lambda_function  # pylint: disable=unused-variable

    url = api_gw_lambda_integrate_deploy(api_client, api, api_resource, lambda_func)
    req = requests.get(url)
    assert req.status_code == 200


@pytest.mark.api_gw_method_args(
    {
        "put_method_args": {"httpMethod": "GET",},
        "put_method_response_args": {"httpMethod": "GET",},
    }
)
@pytest.mark.lambda_function_args(
    {
        "name": "elasticsearch",
        "handler": "es.stac_search_endpoint_handler",
        "environment": {},
        "timeout": 30,
        "layers": (
            {
                "output_dir": "./test",
                "layer_dir": "./cbers2stac/layers/common",
                "tag": "common",
            },
        ),
    }
)
def test_item_search_get(
    api_gw_method, lambda_function, es_client
):  # pylint: disable=too-many-locals,too-many-statements
    """
    test_item_search_get
    """

    api_client, api, api_resource = api_gw_method
    lambda_client, lambda_func = lambda_function  # pylint: disable=unused-variable
    # ES_ENDPOINT is set by lambda_function
    lambda_client.update_function_configuration(
        FunctionName=lambda_func["FunctionName"],
        Environment={"Variables": {"ES_PORT": "4571", "ES_SSL": "NO",}},
    )

    populate_es_test_case_1(es_client)

    # Empty GET, return all 2 items
    original_url = api_gw_lambda_integrate_deploy(
        api_client, api, api_resource, lambda_func
    )
    req = requests.get(original_url)
    assert req.status_code == 200, req.text
    fcol = json.loads(req.text)
    assert len(fcol["features"]) == 2

    # Single collection, return single item
    url = f"{original_url}?collections=CBERS4-MUX"
    req = requests.get(url)
    assert req.status_code == 200, req.text
    fcol = json.loads(req.text)
    assert len(fcol["features"]) == 1
    assert fcol["features"][0]["collection"] == "CBERS4-MUX"

    # Two collections, return all items
    url = f"{original_url}?collections=CBERS4-MUX,CBERS4-AWFI"
    req = requests.get(url)
    assert req.status_code == 200, req.text
    fcol = json.loads(req.text)
    assert len(fcol["features"]) == 2

    # Paging, no next case
    url = f"{original_url}"
    req = requests.get(url)
    assert req.status_code == 200, req.text
    fcol = json.loads(req.text)
    assert "links" not in fcol.keys()

    # Paging, next page
    url = f"{original_url}?limit=1"
    req = requests.get(url)
    assert req.status_code == 200, req.text
    fcol = json.loads(req.text)
    assert "links" in fcol.keys()
    assert len(fcol["links"]) == 1
    next_href = to_localstack_url(api["id"], fcol["links"][0]["href"])
    req = requests.get(next_href)
    assert req.status_code == 200, req.text
    fcol = json.loads(req.text)
    assert "links" not in fcol.keys()
    assert fcol["features"][0]["id"] == "CBERS_4_MUX_20170528_090_084_L2"

    # ids
    url = f"{original_url}?ids=CBERS_4_MUX_20170528_090_084_L2"
    req = requests.get(url)
    assert req.status_code == 200, req.text
    fcol = json.loads(req.text)
    assert len(fcol["features"]) == 1
    assert fcol["features"][0]["id"] == "CBERS_4_MUX_20170528_090_084_L2"

    # query extension
    url = f"{original_url}?"
    url += urlencode({"query": '{"cbers:data_type": {"eq":"L4"}}'})
    req = requests.get(url)
    assert req.status_code == 200, req.text
    fcol = json.loads(req.text)
    assert len(fcol["features"]) == 1
    assert fcol["features"][0]["id"] == "CBERS_4_AWFI_20170409_167_123_L4"


@pytest.mark.api_gw_method_args(
    {
        "put_method_args": {"httpMethod": "POST",},
        "put_method_response_args": {"httpMethod": "POST",},
    }
)
@pytest.mark.lambda_function_args(
    {
        "name": "elasticsearch",
        "handler": "es.stac_search_endpoint_handler",
        "environment": {},
        "timeout": 30,
        "layers": (
            {
                "output_dir": "./test",
                "layer_dir": "./cbers2stac/layers/common",
                "tag": "common",
            },
        ),
    }
)
def test_item_search_post(
    api_gw_method, lambda_function, es_client
):  # pylint: disable=too-many-locals
    """
    test_item_search_post
    """

    api_client, api, api_resource = api_gw_method
    lambda_client, lambda_func = lambda_function  # pylint: disable=unused-variable
    # ES_ENDPOINT is set by lambda_function
    lambda_client.update_function_configuration(
        FunctionName=lambda_func["FunctionName"],
        Environment={"Variables": {"ES_PORT": "4571", "ES_SSL": "NO",}},
    )

    populate_es_test_case_1(es_client)

    url = api_gw_lambda_integrate_deploy(
        api_client, api, api_resource, lambda_func, http_method="POST"
    )
    # POST with invalid bbox order, check error status code and message
    req = requests.post(
        url,
        data=json.dumps(
            {
                "collections": ["mycollection"],
                "bbox": [160.6, -55.95, -170, -25.89],
                "limit": 100,
                "datetime": "2019-01-01T00:00:00Z/2019-01-01T23:59:59Z",
            }
        ),
    )
    assert req.status_code == 400, req.text
    assert "First lon corner is not western" in req.text

    # Same as above with fixed bbox
    req = requests.post(
        url,
        data=json.dumps(
            {
                "collections": ["mycollection"],
                "bbox": [-170, -25.89, 160.6, -55.95],
                "limit": 100,
                "datetime": "2019-01-01T00:00:00Z/2019-01-01T23:59:59Z",
            }
        ),
    )
    assert req.status_code == 200, req.text

    # Paging, no next case
    req = requests.post(url)
    assert req.status_code == 200, req.text
    fcol = json.loads(req.text)
    assert "links" not in fcol.keys()

    # Paging, next page
    body = {"limit": 1}
    req = requests.post(url, data=json.dumps(body))
    assert req.status_code == 200, req.text
    fcol = json.loads(req.text)
    assert "links" in fcol.keys()
    assert len(fcol["links"]) == 1
    next_href = to_localstack_url(api["id"], fcol["links"][0]["href"])
    req = requests.post(
        next_href, data=json.dumps({**body, **fcol["links"][0]["body"]})
    )
    assert req.status_code == 200, req.text
    fcol = json.loads(req.text)
    assert "links" not in fcol.keys()
    assert fcol["features"][0]["id"] == "CBERS_4_MUX_20170528_090_084_L2"

    # ids
    body = {"ids": ["CBERS_4_MUX_20170528_090_084_L2"]}
    req = requests.post(url, data=json.dumps(body))
    assert req.status_code == 200, req.text
    fcol = json.loads(req.text)
    assert len(fcol["features"]) == 1
    assert fcol["features"][0]["id"] == "CBERS_4_MUX_20170528_090_084_L2"
