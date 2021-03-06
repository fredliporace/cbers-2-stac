"""api_gw_test"""

# Remove warnings when using pytest fixtures
# pylint: disable=redefined-outer-name

from test.conftest import ENDPOINT_URL

# warning disabled, this is used as a pylint fixture
from test.elasticsearch_test import es_client  # pylint: disable=unused-import

import boto3
import pytest
import requests

from cbers2stac.elasticsearch.es import create_document_in_index


def api_gw_lambda_integrate_deploy(
    api_client, api: dict, api_resource: dict, lambda_func: dict
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
        httpMethod="GET",
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
def test_root_endpoint(api_gw_method, lambda_function):
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
def test_search_endpoint(api_gw_method, lambda_function, es_client):
    """
    test_search_endpoint
    """

    api_client, api, api_resource = api_gw_method
    lambda_client, lambda_func = lambda_function  # pylint: disable=unused-variable
    # ES_ENDPOINT is set by lambda_function
    lambda_client.update_function_configuration(
        FunctionName=lambda_func["FunctionName"],
        Environment={"Variables": {"ES_PORT": "4571", "ES_SSL": "NO",}},
    )

    stac_items = list()
    with open("test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json", "r") as fin:
        stac_items.append(fin.read())
    with open("test/fixtures/ref_CBERS_4_AWFI_20170409_167_123_L4.json", "r") as fin:
        stac_items.append(fin.read())

    for stac_item in stac_items:
        create_document_in_index(es_client=es_client, stac_item=stac_item)

    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_AWFI_20170409_167_123_L4"
    )

    url = api_gw_lambda_integrate_deploy(api_client, api, api_resource, lambda_func)
    req = requests.get(url)
    assert req.status_code == 200, req.text
