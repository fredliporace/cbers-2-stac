# cbers-2-stac

![CI](https://github.com/fredliporace/cbers-2-stac/actions/workflows/ci.yml/badge.svg?branch=dev)

Create and keep up-to-date a [STAC](https://github.com/radiantearth/stac-spec) static catalog for [CBERS-4/4A images on AWS](https://registry.opendata.aws/cbers/).

*NOTE* The dev branch is under major revamp to support STAC 1.0.0-rc-2. This documentation is not updated and should not be used for deployment.

The mechanism to include new items into the archive as soon as they are ingested is described in [this AWS blog post](https://aws.amazon.com/blogs/publicsector/keeping-a-spatiotemporal-asset-catalog-stac-up-to-date-with-sns-sqs/).

## STAC version and extensions

Supports STAC version 1.0.0.

The following extensions/features are available:

  * [Query](https://github.com/radiantearth/stac-spec/tree/v0.7.0/api-spec/extensions/query) API extension to filter on properties of items.
  * [EO](https://github.com/radiantearth/stac-spec/tree/v0.7.0/extensions/eo) extension to define some specific fields for items.
  * [WFS3 core endpoints](https://github.com/radiantearth/stac-spec/blob/v0.7.0/api-spec/api-spec.md).

## Live version

A live version of the stack is deployed to AWS and serve its contents in:

  * ```arn:aws:s3:::cbers-stac-0-7``` S3 bucket with static STAC content, including its [root catalog](https://cbers-stac-0-7.s3.amazonaws.com/catalog.json).
  * WFS3 [endpoint](https://stac.amskepler.com/v07/).
  * STAC [endpoint](https://stac.amskepler.com/v07/stac/).

# Development

## Install

```bash
$ git clone git@github.com:AMS-Kepler/cbers-2-stac.git
$ cd cbers-2-stac
$ pip install -e .[dev,test]
$ ./pip-on-lambdas.sh
```

## Git hooks

This repo is set to use `pre-commit` to run *isort*, *pylint*, *pydocstring*, *black* ("uncompromising Python code formatter") and mypy when committing new code.

```bash
$ pre-commit install
```

## Testing

Requires localstack up to execute tests:

```bash
$ cd test && docker-compose up # Starts localstack
```

### Check CI integration testing before pushing

[https://github.com/nektos/act](act) may be used to test github actions locally. At the project's root directory:

```bash
$ act -j tests
$ act -r -j tests # To keep docker containers' state
```

# Deployment to AWS

## Install local pip packages in lambdas directory

Some lambdas require extra pip packages to be installed in the lambda directory before deployment. To install these packages execute:

```bash
./pip-on-lambdas.sh
```

## CDK bootstrap

Deployment uses AWS CDK.

Requirements:
* node.js 10.3.0 or later is required (13.0.0 through 13.6.0 are not supported)
* AWS credentials configured

To install and check AWS CDK:
```bash
$ npm install -g aws-cdk
$ cdk --version

$ cdk bootstrap # Deploys the CDK toolkit stack into an AWS environment

# in specific region
$ cdk bootstrap aws://${AWS_ACCOUNT_ID}/eu-central-1
```

## Configuration

Create a ```.env``` file in the project root directory and configure your application. You should use ```.env_example``` as a guide, this file contains the documentation for all parameters.

If STAC_API is enabled you should review the Elasticsearch domain configuration which is currently [hard-coded](https://github.com/fredliporace/cbers-2-stac/blob/dev/stack/app.py#L536-L552), see this [issue](https://github.com/fredliporace/cbers-2-stac/issues/61).

After the configuration is completed check if CDK shows the configured stack:

```bash
$ cdk list
```

## Deployment

Deploy the stack, replacing ```cbers2stac-dev``` with your configured stack name:

```bash
$ cdk deploy cbers2stac-dev
```

If ```STACK_ENABLE_API``` is set in the configuration you should now create the Elasticsearch index. This needs to be executed only once, right after the first deploy that enables the API. The index is created by the lambda ```create_elastic_index_lambda```, which may be executed from the AWS console or awscli. The function requires no parameters.

The e-mail configured in ```STACK_OPERATOR_EMAIL``` receives execution alarms and while the first deploy is made it should receive a message requesting confirmation for the alarm topic subscription. Accept the request to receive the alarms.

The stack output shows the API endpoint, if configured.

?? topic for STAC documents.

## Reconciliation

The lambda ```populate_reconcile_queue_lambda``` may be used to reconcile the STAC catalog with the CBERS metadata catalog. The lambda payload is a prefix, all scenes under the prefix are queued and indexed again. Some examples are shown below.

To index all CBERS-4 MUX scenes with path 102 and row 83:
```json
{
  "prefix": "CBERS4/MUX/102/083/"
}
```

To index all CBERS-4A MUX scenes:
```json
{
  "prefix": "CBERS4A/MUX/"
}
```

To index all CBERS-4A MUX scenes with path 120:
```json
{
  "prefix": "CBERS4A/MUX/120"
}
```

The indexed documents are immediately available through the STAC API. The static catalogs are updated every 30 minutes. To update the static catalogs before that you may execute the ```generate_catalog_levels_to_be_updated_lambda``` lambda.
