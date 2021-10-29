# cbers-2-stac

![CI](https://github.com/fredliporace/cbers-2-stac/actions/workflows/ci.yml/badge.svg?branch=master)

Create and keep up-to-date a [STAC](https://github.com/radiantearth/stac-spec) static catalog and an API for [CBERS-4/4A images on AWS](https://registry.opendata.aws/cbers/).

## STAC version and extensions

Implements STAC version 1.0.0 and STAC-API version 1.0.0-beta.1.

## Live version

A live version of the stack is deployed to AWS and serve its contents in:

  * `arn:aws:s3:::cbers-stac-1-0-0` S3 bucket with static STAC content, including its [root catalog](https://cbers-stac-1-0-0.s3.amazonaws.com/catalog.json).
  * STAC API [endpoint](https://stac.amskepler.com/v100).
  * SNS topic for new scenes: `arn:aws:sns:us-east-1:769537946825:cbers2stac-prod-stacitemtopic4BCE3141-VI09VRB6LBEK`. This topic receive the complete STAC item for each ingested scene.

# Deployment to AWS

## Install local pip packages in lambdas directory

Some lambdas require extra pip packages to be installed in the lambda directory before deployment. To install these packages execute:

```bash
./pip-on-lambdas.sh
```

## CDK bootstrap

Deployment uses AWS CDK.

Requirements:

 * node.js 10.3.0 or later (13.0.0 through 13.6.0 are not supported)
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

If `STAC_ENABLE_API` is set you should review the Elasticsearch domain configuration which is currently [hard-coded](https://github.com/fredliporace/cbers-2-stac/blob/dev/stack/app.py#L536-L552), see this [issue](https://github.com/fredliporace/cbers-2-stac/issues/61).

After the configuration is completed check if CDK shows the configured stack:

```bash
$ cdk list
```

## Deployment

Deploy the stack, replacing ```cbers2stac-dev``` with your configured stack name:

```bash
$ cdk deploy cbers2stac-dev
```

The e-mail configured in ```STACK_OPERATOR_EMAIL``` receives execution alarms and while the first deploy is made it should receive a message requesting confirmation for the alarm topic subscription. Accept the request to receive the alarms.

The stack output shows the SNS topic for new scenes and the API endpoint, if configured:
```bash
 ✅  cbers2stac-prod

Outputs:
cbers2stac-prod.stacapiEndpointBED73CCA = https://....
cbers2stac-prod.stacitemtopicoutput = arn:aws:sns:us-east-1:...:...

```

### Creating the Elasticsearch index

If ```STACK_ENABLE_API``` is set in the configuration you should now create the Elasticsearch index. This needs to be executed only once, right after the first deploy that enables the API. The index is created by the lambda ```create_elastic_index_lambda```, which may be executed from the AWS console or awscli. The function requires no parameters.

It is recommended to change the cluster configuration to disable the automatic creation of indices. AFAIK this can't be done through CDK options, you need to directly access the domain configuration endpoint, see [example](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-creation).

## Reconciliation

The lambda ```populate_reconcile_queue_lambda``` may be used to reconcile the STAC catalog with the original CBERS metadata catalog. The lambda payload is a prefix, all scenes under the prefix are queued, converted to STAC and indexed again. Some examples are shown below.

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
  "prefix": "CBERS4A/MUX/120/"
}
```

The indexed documents are immediately available through the STAC API. The static catalogs are updated every 30 minutes. To update the static catalogs before that you may execute the ```generate_catalog_levels_to_be_updated_lambda``` lambda.

## Dead letter queues (DLQs)

The system makes extensive use of the SQS-lambda integration pattern. DLQs are defined to store messages representing failed jobs:

 * `reconcile-queue`: jobs representing the S3 prefixes that will be reconciled are queued here. Failed jobs are sent to `consume-reconcile-queue-dlq`.
 * `new-scenes-queue`: jobs representing a key for a scene to be converted to STAC and indexed. Failed jobs are sent to `process-new-scenes-queue-dlq`.

A tool is provided to move messages from SQS queues, this may be used to re-queue failed jobs:
```bash
./utils/redrive_sqs_queue.py --src-url=https://... --dst-url=https://... --messages-no=100
```

# Development

## Install

```bash
$ git clone git@github.com:fredliporace/cbers-2-stac.git
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

Check before if `/tmp/localstack/es_backup` directory exists, this is required to start the ES service.

```bash
$ cd test && docker-compose up # Starts localstack
$ pytest
```

### Check CI integration testing before pushing

[act](https://github.com/nektos/act) may be used to test github actions locally. At the project's root directory:

```bash
$ act -j tests
$ act -r -j tests # To keep docker container's state
```

# References

The mechanism to include new items into the archive as soon as they are ingested is described in [this AWS blog post](https://aws.amazon.com/blogs/publicsector/keeping-a-spatiotemporal-asset-catalog-stac-up-to-date-with-sns-sqs/).

# Acknowledgments

[Radiant Earth Foundation](https://www.radiant.earth/) supported the migration to STAC 1.0.0 final.
