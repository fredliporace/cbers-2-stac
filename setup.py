"""Setup cbers-2-stac."""

from setuptools import find_packages, setup

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

inst_reqs = [
    "boto3",
    "jsonschema",
]

extra_reqs = {
    "dev": ["awscli", "awscli-local",],
    "test": [
        "pydantic[dotenv]",
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
        # Used in elasticsearch lambda. This needs to be <7.14.0 to avoid the
        #   "The client noticed that the server is not a supported distribution of Elasticsearch"
        #   error message.
        "elasticsearch>=7.0.0,<7.14.0",
        "elasticsearch-dsl>=7.0.0,<8.0.0",
        "aws-requests-auth",
        # Used in process_new_scene_queue lambda.
        "utm",
    ],
    "deploy": ["pydantic[dotenv]", "aws-cdk-lib>=2.0.0", "constructs>=10.0.0",],
}

ENTRY_POINTS = """
[console_scripts]
cb2stac-redrive-sqs=utils.redrive_sqs_queue:main
"""

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
    entry_points=ENTRY_POINTS,
)
