# cbers-2-stac

![CI](https://github.com/fredliporace/cbers-2-stac/actions/workflows/ci.yml/badge.svg?branch=dev)

Create and keep up-to-date a [STAC](https://github.com/radiantearth/stac-spec) static catalog for [CBERS-4/4A images on AWS](https://registry.opendata.aws/cbers/).

*NOTE* The dev branch is under major revamp to support STAC 1.0.0-rc-2. This documentation is not updated and should not be used for deployment.

The mechanism to include new items into the archive as soon as they are ingested is described in [this AWS blog post](https://aws.amazon.com/blogs/publicsector/keeping-a-spatiotemporal-asset-catalog-stac-up-to-date-with-sns-sqs/).

## STAC version and extensions

Supports STAC version 1.0.0-rc-2.

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

```bash
# Create a .env file in the project root directory and
# configure. You may use .env.example as a guide

# To list the available stacks
$ cdk list

$ cdk deploy cbers2stac-dev
```

<!---
First you need to define two buckets:

* STAC\_BUCKET, the bucket that will be populated with STAC files. This bucket currently needs to be named cbers-stac-VERSIONMAJOR-VERSIONMINOR, for instance, cbers-stac-1-0
* DEPLOY\_BUCKET, the bucket that will be temporarily used for deployment files.

Change the following parameters in ./sam/Makefile to reflect your environment:

* OPERATOR_EMAIL: this e-mail will be used to notify problems in the stack execution

Populate STAC\_BUCKET with the static STAC files by executing in the ./stac\_catalogs directory.
```
(export AWS_PROFILE=your_aws_profile && ./sync_to_aws.sh)
```

To deploy the stack execute in the ./sam directory:
```
(export AWS_PROFILE=your_aws_profile && export DEPLOY_BUCKET=deploy_bucket_created_above && make deploy)
```

After the first deployment it is required to create the Elasticseach index by executing the CreateElasticIndexFunction lambda with an empty payload input.
--->
