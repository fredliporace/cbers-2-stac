"""Setup cbers-2-stac."""

from setuptools import find_packages, setup

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

inst_reqs = [
    "boto3",
    "jsonschema",
]

extra_reqs = {
    "dev": [
        "awscli",
        "awscli-local",
    ],
    "test": [
        "pydantic",
        "pydantic-settings",
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
        # Changes here must also be reflected in pre-commit mypy additional dependencies
        "elasticsearch>=7.0.0,<7.14.0",
        "elasticsearch-dsl>=7.0.0,<8.0.0",
        # elasticsearch 7.13.4 requires urllib3<2,>=1.21.1
        "urllib3<2,>=1.21.1",
        "aws-requests-auth",
        # Used in process_new_scene_queue lambda.
        "utm",
    ],
    "deploy": ["pydantic", "pydantic-settings", "aws-cdk-lib", "constructs", "docker"],
}

ENTRY_POINTS = """
[console_scripts]
cb2stac-redrive-sqs=utils.redrive_sqs_queue:main
"""

setup(
    name="cbers-2-stac",
    version="0.0.0",
    description="STAC service for CBERS and Amazonia data on AWS",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.14",
    author="Frederico Liporace (Scitekno)",
    author_email="liporace@scitekno.com.br",
    url="https://github.com/fredliporace/cbers-2-stac",
    packages=find_packages(exclude=["tests*"]),
    zip_safe=False,
    install_requires=inst_reqs,
    extras_require=extra_reqs,  # type: ignore[arg-type]
    entry_points=ENTRY_POINTS,
)
