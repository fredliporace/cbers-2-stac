"""api_gw_test"""

# Remove warnings when using pytest fixtures
# pylint: disable=redefined-outer-name

from test.conftest import ENDPOINT_URL

import boto3
import pytest
import requests


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
def test_root_endpoint(lambda_function):
    """
    test_root_endpoint
    """

    # Based on
    # https://stackoverflow.com/questions/58859917/creating-aws-lambda-integrated-api-gateway-resource-with-boto3

    lambda_client, lambda_func = lambda_function  # pylint: disable=unused-variable

    # Create REST api
    api_client = boto3.client("apigateway", endpoint_url=ENDPOINT_URL)
    api = api_client.create_rest_api(name="testapi")
    root_resource_id = api_client.get_resources(restApiId=api["id"])["items"][0]["id"]
    api_resource = api_client.create_resource(
        restApiId=api["id"], parentId=root_resource_id, pathPart="greeting"
    )
    api_client.put_method(
        restApiId=api["id"],
        resourceId=api_resource["id"],
        httpMethod="GET",
        authorizationType="NONE",
    )
    api_client.put_method_response(
        restApiId=api["id"],
        resourceId=api_resource["id"],
        httpMethod="GET",
        statusCode="200",
    )
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
    url = f"http://localhost:4566/restapis/{api['id']}/dev/_user_request_/greeting"
    req = requests.get(url)
    assert req.status_code == 200
