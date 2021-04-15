"""Setup cbers-2-stac."""

from setuptools import find_packages, setup

with open("README.md") as f:
    long_description = f.read()

inst_reqs = [
    "utm",
    "boto3",
    "jsonschema==3.2.0",
]

extra_reqs = {
    "dev": [
        "importlib-metadata<2,>=0.12",  # This is required by tox 3.2.0
        "pytest",
        "pytest-cov",
        "pre-commit",
        "pylint",
        "pystac[validation]==0.5.1",
        "tox",
        "docker",
        "retry",
        "awscli",
        "awscli-local",
        # The packages below are used by lambdas and need to be installed locally
        # for testing to work
        "elasticsearch==6.2.0",
        "elasticsearch-dsl==6.2.0",
        "aws-requests-auth==0.4.2",
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
