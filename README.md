# cbers-2-stac

![CI](https://github.com/fredliporace/cbers-2-stac/actions/workflows/ci.yml/badge.svg?branch=master)

Create and keep up-to-date a [STAC](https://github.com/radiantearth/stac-spec) static catalog and API for [CBERS-4/4A](https://registry.opendata.aws/cbers/) and [Amazonia-1](https://registry.opendata.aws/amazonia/) images on AWS.

# STAC version and extensions

Implements STAC version 1.0.0 and STAC-API version 1.0.0-beta.1.

# Live version

A live version of the stack is deployed to AWS and serve its contents in:

  * `arn:aws:s3:::cbers-stac-1-0-0` S3 bucket with static STAC content, including its [root catalog](https://cbers-stac-1-0-0.s3.amazonaws.com/catalog.json).
  * STAC API [endpoint](https://stac.amskepler.com/v100) - please check this [**important notice**](https://github.com/fredliporace/cbers-2-stac/issues/77#issuecomment-1120330181).
  * SNS topic for new scenes: `arn:aws:sns:us-east-1:769537946825:cbers2stac-prod-stacitemtopic4BCE3141-VI09VRB6LBEK`. This topic receive the complete STAC item for each ingested scene.

# Deployment to AWS

## Install

```bash
$ git clone git@github.com:fredliporace/cbers-2-stac.git
$ cd cbers-2-stac
$ pip install -e .[dev,test,deploy]
```

Some lambdas require extra pip packages to be installed in the lambda directory before deployment. To install these packages execute:

```bash
./pip-on-lambdas.sh
```

## CDK bootstrap

Deployment uses AWS CDK.

Requirements:
* node: Use [nvm](https://heynode.com/tutorial/install-nodejs-locally-nvm/) to make sure a supported node is being used, tested with 16.16.0
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

If `STAC_ENABLE_API` is set you should review the Elasticsearch domain configuration, for which some parameters are [hard-coded](https://github.com/fredliporace/cbers-2-stac/blob/dev/stack/app.py#L536-L552), see this [issue](https://github.com/fredliporace/cbers-2-stac/issues/61).

After the configuration is completed check if CDK shows the configured stack:

```bash
$ cdk list
```

## Deployment

Deploy the stack, replacing `cbers2stac-dev` with your configured stack name:

```bash
$ cdk deploy cbers2stac-dev
```

The e-mail configured in `STACK_OPERATOR_EMAIL` receives execution alarms and while the first deploy is made it should receive a message requesting confirmation for the alarm topic subscription. Accept the request to receive the alarms.

The stack output shows the SNS topic for new scenes and the API endpoint, if configured:
```bash
 ✅  cbers2stac-prod

Outputs:
cbers2stac-prod.stacapiEndpointBED73CCA = https://....
cbers2stac-prod.stacitemtopicoutput = arn:aws:sns:us-east-1:...:...

```

## Creating the Elasticsearch index

If ```STACK_ENABLE_API``` is set in the configuration you should now create the Elasticsearch index. This needs to be executed only once, right after the first deploy that enables the API. The index is created by the lambda ```create_elastic_index_lambda```, which may be executed from the AWS console or awscli. The function requires no parameters.

It is recommended to change the cluster configuration to disable the automatic creation of indices. AFAIK this can't be done through CDK options, you need to directly access the domain configuration endpoint, see [example](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-creation).

## Populate the catalog

Once create the stack automatically listen to the SNS topics that publish new quicklooks. The stac item is automatically created as soon as the message is received.

### Reconciliation from INPE's original metadata

The lambda `populate_reconcile_queue_lambda` may be used to reconcile the STAC catalog with the original CBERS metadata catalog. This is typically used to populate a new STAC instance with data from the open datasets.

The lambda payload is a bucket name and a prefix, all directories under the prefix are queued. Each directory is then scanned separately by a distinct lambda instance, all XMLs are converted to static STAC documents and indexed again, if the API is configured.

Some examples are shown below.

To index all CBERS-4 MUX scenes with path 102 and row 83.
```json
{
  "bucket": "cbers-pds",
  "prefix": "CBERS4/MUX/102/083/"
}
```

To index all CBERS-4A MUX scenes:
```json
{
  "bucket": "cbers-pds",
  "prefix": "CBERS4A/MUX/"
}
```

To index all CBERS-4A MUX scenes with path 120:
```json
{
  "bucket": "cbers-pds",
  "prefix": "CBERS4A/MUX/120/"
}
```

The indexed documents are immediately available through the STAC API. The static catalogs are updated every 30 minutes. To update the static catalogs before that you may execute the ```generate_catalog_levels_to_be_updated_lambda``` lambda.

### Reconciliation from STAC static catalog

The lambda ```reindex_stac_items_lambda``` may be used to reconcile the STAC API service with the static catalog. The lambda payload are the parameters to be passed to `list_objects_v2`, all STAC items under the prefix are queued and re-indexed. Some examples are shown below.

To index all CBERS-4 AWFI scenes with path 1 and row 27:
```json
{
  "prefix": "CBERS4/AWFI/001/"
}
```

## Operation

### SQS-Lambda and dead letter queues (DLQs)

The system makes extensive use of the SQS-lambda integration pattern. DLQs are defined to store messages representing failed jobs:

 * `reconcile_queue`: jobs representing the S3 prefixes that will be reconciled are queued here. Consumed by `consume_reconcile_queue_lambda`. Failed jobs are sent to `consume_reconcile_queue_dlq`.
 * `new_scenes_queue`: jobs representing a key for a scene to be converted to STAC and indexed. Consumed by `process_new_scene_lambda`. Failed jobs are sent to `process_new_scenes_queue_dlq`.
 * `insert_into_elasticsearch_queue`: jobs representing a STAC item. This queue subscribes to `stac_item_topic` and `reconcile_stac_item_topic`, receiving the STAC itemas as notifications. Consumed by `insert_into_elastic_lambda`. Failed jobs (for now) are sent to `dead_letter_queue`.

Failed lambda executions from other queues are sent to the general `dead_letter_queue`.

A tool is provided to move messages from SQS queues, this may be used to re-queue failed jobs:
```bash
cb2stac-redrive-sqs --src-url=https://... --dst-url=https://... --messages-no=100
```

The jobs may also be re-queued using the new `Start DLQ redrive` now available from the AWS console.

### Recovering from ElasticSearch (ES) cluster failures

#### Restore from ES snapshot

See [AWS documentation](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/managedomains-snapshots.html#managedomains-snapshot-restore).

#### Re-ingest items from backup queue

The system keeps `backup_insert_into_elasticsearch_queue` with the STAC items ingested in the last days, see `STACK_BACKUP_QUEUE_RETENTION_DAYS` configuration parameter. The items may be re-ingested after a restore to make sure that the archive includes the items processed between the restored backup date and the failure.

Use `cb2stac-redrive-sqs` (or `Start DLQ redrive` from AWS console) to transfer messages from DLQ to `insert_into_elasticsearch_queue`.

# Development

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

## Check CI integration testing before pushing

[act](https://github.com/nektos/act) may be used to test github actions locally. At the project's root directory:

```bash
$ act -j tests
$ act -r -j tests # To keep docker container's state
```

# References

The mechanism to include new items into the archive as soon as they are ingested is described in [this AWS blog post](https://aws.amazon.com/blogs/publicsector/keeping-a-spatiotemporal-asset-catalog-stac-up-to-date-with-sns-sqs/).

# Acknowledgments

[Radiant Earth Foundation](https://www.radiant.earth/) supported the migration to STAC 1.0.0 final.
