"""pytest conf"""

import os
import pathlib
import shutil
import tarfile
import tempfile
from zipfile import ZipFile, ZipInfo

import boto3
import botocore
import docker
import pytest

ENDPOINT_URL = "http://localhost:4566"


def lambda_filtered_paths(directory: str):
    """
    Return list of filepaths for lambda layers and functions. Unecessary
    files are filtered out.
    """
    paths = pathlib.Path(directory).rglob("*")
    return [
        f
        for f in paths
        if not any(pat in str(f) for pat in ["__pycache__", ".mypy_cache", "~"])
    ]


def create_lambda_layer_from_dir(
    output_dir: str, tag: str, layer_dir: str, prefix: str = ""
):
    """Build lambda layer from directory name"""
    layer_zip = f"{output_dir}/{tag}.zip"
    with ZipFile(layer_zip, "w") as zfile:
        paths = lambda_filtered_paths(layer_dir)
        for path in paths:
            # This checks got flycheck temporary files
            if path.exists():
                zfile.write(path, path if prefix == "" else prefix + "/" + str(path))


def create_lambda_layer_from_docker(output_dir: str, dockerfile: str, tag: str) -> None:
    """
    Build lambda layer from Dockerfile. It is assumed that the
    Dockerfile creates a ZIP file named package_localstack in the
    /tmp directory. This ZIP file contains all the code under the
    layers' python directory.
    """
    client = docker.from_env()

    build_result = client.images.build(
        path="./",
        dockerfile=dockerfile,
        tag=tag,
        # rm=True, # Uncomment to save space
    )
    assert len(build_result) == 2
    # print("Docker build logs:", file=sys.stderr)
    # for log_item in build_result[1]:
    #     print(f"\t{log_item}", file=sys.stderr)

    # Comments used while dedugging test within act, no
    # pdb... keeping for a while
    # result = subprocess.run(['pwd'], capture_output=True, text=True, check=True)
    # print(f"pwd: {result.stdout}", file=sys.stderr)

    # result = subprocess.run(['ls', '-l'], capture_output=True, text=True,
    #                        check=True)
    # print(f"ls: {result.stdout}", file=sys.stderr)

    # result = subprocess.run(['ls', '-l', 'tests'], capture_output=True, text=True,
    #                        check=True)
    # print(f"ls tests: {result.stdout}", file=sys.stderr)

    # Copy layer from docker container using temporary tarfile
    container = client.containers.run(image=tag, command="echo", detach=True)

    namedtarfile = tempfile.NamedTemporaryFile(delete=True)
    # print(namedtarfile.name, file=sys.stderr)
    bits, stat = container.get_archive(  # pylint: disable=unused-variable
        "/tmp/package_localstack.zip"
    )
    for chunk in bits:
        namedtarfile.write(chunk)

    with tarfile.open(namedtarfile.name) as tfile:
        tfile.extract(member="package_localstack.zip", path="./")
    os.rename("./package_localstack.zip", f"{os.path.abspath(output_dir)}/{tag}.zip")

    # Original way to copy the layer file from docker container
    # This does not work within act, the mounted volume is from the host
    # machine and not the docker container.
    # command = f"/bin/sh -c 'cp /tmp/package_localstack.zip /local/{tag}.zip'"
    # result = client.containers.run(
    #     image=tag,
    #     command=command,
    #     remove=True,
    #     volumes={os.path.abspath(output_dir): {"bind": "/local/", "mode": "rw"}},
    #     user=0,
    #     stdout=True,
    #     stderr=False
    # )

    # This uses docker cli utilities to copy the layer ZIP file to the
    # testing environment
    # Does not work within act, we do not have docker CLI utils
    # with open(os.path.abspath(output_dir) + f"/{tag}.zip", "wb") as layer_zip:
    #     result = subprocess.call(['docker', 'run', '--rm',
    #                               '--entrypoint', 'cat', tag,
    #                               '/tmp/package_localstack.zip'],
    #                              stdout=layer_zip)


@pytest.fixture(autouse=True)
def testing_env_var(monkeypatch):
    """Environment for testing"""

    # Set fake env to make sure we don't hit AWS services
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fsl")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "mao")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", "/tmp/noconfigheere")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/tmp/noconfighereeither")
    monkeypatch.setenv("LOCALSTACK_HOSTNAME", "localhost")
    monkeypatch.setenv("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")


@pytest.fixture
def s3_bucket(request):
    """S3 bucket for testing"""
    marker = request.node.get_closest_marker("s3_bucket_args")
    # Marker is mandatory, first argument is bucket name
    assert marker
    bucket_name = marker.args[0]
    s3_resource = None

    def fin():
        """fixture finalizer"""
        if s3_client:
            s3_resource.Bucket(bucket_name).objects.delete()
            s3_client.delete_bucket(Bucket=bucket_name)

    # Hook teardown (finalizer) code
    request.addfinalizer(fin)

    # Bucket creation
    s3_client = boto3.client("s3", endpoint_url=ENDPOINT_URL)
    s3_client.create_bucket(Bucket=bucket_name)
    s3_resource = boto3.resource("s3", endpoint_url=ENDPOINT_URL)

    return s3_client, s3_resource


@pytest.fixture
def sqs_queue(request):
    """SQS queue for testing"""
    marker = request.node.get_closest_marker("sqs_queue_args")
    # Marker is mandatory, first argument is queue name
    assert marker
    queue_name = marker.args[0]
    queue = None

    def fin():
        """fixture finalizer"""
        if queue:
            queue.delete()

    # Hook teardown (finalizer) code
    request.addfinalizer(fin)

    # Queue creation
    sqs_resource = boto3.resource("sqs", endpoint_url=ENDPOINT_URL)
    queue = sqs_resource.create_queue(
        QueueName=queue_name, Attributes={"VisibilityTimeout": "300"}
    )

    return queue


@pytest.fixture
def sns_topic(request):
    """SNS topic for testing"""

    sns_client = None

    def fin():
        """fixture finalizer"""
        if sns_client:
            sns_client.delete_topic(TopicArn=topic["TopicArn"])

    request.addfinalizer(fin)

    sns_client = boto3.client("sns", endpoint_url=ENDPOINT_URL)
    topic = sns_client.create_topic(Name="topic")
    return sns_client, topic


@pytest.fixture
def lambda_function(request):  # pylint: disable=too-many-locals
    """Lambda for testing"""

    marker = request.node.get_closest_marker("lambda_function_args")
    # Marker is mandatory, parameters passed as dict
    assert marker
    lambda_name = marker.args[0].get("name")
    lambda_handler = marker.args[0].get("handler")
    lambda_environment = marker.args[0].get("environment")
    lambda_timeout = marker.args[0].get("timeout")
    lambda_func = None
    deploy_bucket = "deploy-bucket"
    s3_client = None

    def fin():
        """fixture finalizer"""
        if s3_client:
            boto3.resource("s3", endpoint_url=ENDPOINT_URL).Bucket(
                deploy_bucket
            ).objects.delete()
            s3_client.delete_bucket(Bucket=deploy_bucket)

        if lambda_func:
            lambda_client.delete_function(FunctionName=lambda_name)

    # Hook teardown (finalizer) code
    request.addfinalizer(fin)

    # Create/update ZIP with layers
    lambda_layers = marker.args[0].get("layers")
    if lambda_layers:
        for layer in lambda_layers:
            if layer.get("dockerfile"):
                create_lambda_layer_from_docker(**layer)
            else:
                create_lambda_layer_from_dir(**layer)

    lambda_dir = f"cbers04aonaws/lambdas/{lambda_name}"
    lambda_zip = "tests/" + pathlib.PurePath(lambda_dir).name + ".zip"

    # If there are layers then we include the sources
    # from the lambda dir. If not we zip the complete
    # lambda directory

    # import pdb; pdb.set_trace()
    if not lambda_layers:
        with ZipFile(lambda_zip, "w") as zfile:
            paths = pathlib.Path(lambda_dir).rglob("*")
            for path in paths:
                # This checks got flycheck temporary files
                if path.exists():
                    zfile.write(path, path.relative_to(lambda_dir))
    else:
        # Start with first layer ZIP...
        shutil.copyfile(
            lambda_layers[0]["output_dir"] + "/" + lambda_layers[0]["tag"] + ".zip",
            lambda_zip,
        )
        # ... add the remaining layers ...
        with ZipFile(lambda_zip, "a") as zfile:
            for layer in lambda_layers[1:]:
                zfl = ZipFile(layer["output_dir"] + "/" + layer["tag"] + ".zip", "r")
                for fname in zfl.namelist():
                    zinfo = ZipInfo(filename=fname)
                    zinfo.external_attr = 0o755 << 16
                    zfile.writestr(zinfo, zfl.open(fname).read())
        # ... and then add the lambda function
        with ZipFile(lambda_zip, "a") as zfile:
            paths = lambda_filtered_paths(lambda_dir)
            for path in paths:
                zfile.write(path, path.relative_to(lambda_dir))

    # Deploy bucket creation
    s3_client = boto3.client("s3", endpoint_url=ENDPOINT_URL)
    s3_client.create_bucket(Bucket=deploy_bucket)

    # Copy ZIP to deploy bucket
    s3_client.upload_file(
        Filename=lambda_zip, Bucket=deploy_bucket, Key=lambda_zip.split("/")[-1]
    )

    # This was used when lambda was deployed directly from ZIP file
    # with open(lambda_zip, "rb") as zfile:
    #     zipped_code = zfile.read()

    lambda_client = boto3.client(
        "lambda",
        endpoint_url=ENDPOINT_URL,
        config=botocore.config.Config(retries={"max_attempts": 0}),
    )
    lambda_func = lambda_client.create_function(
        FunctionName=lambda_name,
        Runtime="python3.7",
        Handler=f"{lambda_name}.{lambda_handler}",
        Role="role",
        # See above (deploy from ZIP file)
        # Code=dict(ZipFile=zipped_code),
        Code={"S3Bucket": deploy_bucket, "S3Key": lambda_zip.split("/")[-1]},
        Environment={"Variables": lambda_environment},
        Timeout=lambda_timeout,
    )
    # print(lambda_func, file=sys.stderr)
    return lambda_client, lambda_func


@pytest.fixture
def dynamodb_table(request):
    """DynamoDB table for testing"""

    marker = request.node.get_closest_marker("dynamodb_table_args")
    # Marker is mandatory, parameters passed as dict directly to dynamodb.create_table
    assert marker

    table = None

    db_resource = None

    def fin():
        """fixture finalizer"""
        if table:
            table.delete()

    # Hook teardown (finalizer) code
    request.addfinalizer(fin)

    # Create table
    db_resource = boto3.resource("dynamodb", endpoint_url=ENDPOINT_URL)
    table = db_resource.create_table(**marker.args[0])

    # Wait until the table exists.
    table.meta.client.get_waiter("table_exists").wait(
        TableName=marker.args[0].get("TableName")
    )

    return table


@pytest.fixture
def test_file(tmpdir_factory):
    """
    A test file created in a temporary directory
    """
    tmpdir = tmpdir_factory.mktemp("data")
    yield str(tmpdir) + "/testfile"
    shutil.rmtree(str(tmpdir))
