"""Setup cbers-2-stac."""

from setuptools import find_packages, setup

with open("README.md") as f:
    long_description = f.read()

inst_reqs = [
    "boto3",
    "jsonschema",
]

extra_reqs = {
    "dev": ["awscli", "awscli-local",],
    "test": [
        "importlib-metadata<2,>=0.12",  # This is required by tox 3.2.0
        "pytest",
        "pytest-cov",
        "pre-commit",
        "pylint",
        "pystac[validation]==0.5.6",
        "tox",
        "docker",
        "retry",
        # The packages below are used by lambdas and need to be installed locally
        # for testing to work
        # "elasticsearch==6.2.0",
        # "elasticsearch-dsl==6.2.0",
        # "aws-requests-auth==0.4.2",
        "elasticsearch",
        "elasticsearch-dsl",
        "aws-requests-auth",
        # Used in process_new_scene_queue
        "utm",
    ],
    "deploy": [
        "pydantic",
        "aws-cdk.core",
        "aws-cdk.aws-sqs",
        "aws-cdk.aws-sns",
        "aws-cdk.aws-sns-subscriptions",
        "aws-cdk.aws-cloudwatch",
        "aws-cdk.aws-cloudwatch-actions",
        "aws-cdk.aws-lambda",
        "aws-cdk.aws-s3",
        "aws-cdk.aws-iam",
        "aws-cdk.aws-dynamodb",
        "aws-cdk.aws-lambda-event-sources",
        "aws-cdk.aws-s3-deployment",
    ],
}

setup(
    name="cbers-2-stac",
    version="0.0.0",
    description=u"",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires="==3.7.9",
    author=u"Frederico Liporace (AMS Kepler)",
    author_email="liporace@amskepler.com",
    url="https://github.com/fredliporace/cbers-2-stac",
    packages=find_packages(exclude=["tests*"]),
    zip_safe=False,
    install_requires=inst_reqs,
    extras_require=extra_reqs,
)
