# cbers-2-stac

[![CircleCI](https://circleci.com/gh/fredliporace/cbers-2-stac.svg?style=svg)](https://circleci.com/gh/fredliporace/cbers-2-stac)

Create and keep up-to-date a [STAC](https://github.com/radiantearth/stac-spec/tree/v0.7.0) static catalog for [CBERS-4 images on AWS](https://registry.opendata.aws/cbers/) and serve its contents through the STAC API.

The mechanism to include new items into the archive as soon as they are ingested is described in [this AWS blog post](https://aws.amazon.com/blogs/publicsector/keeping-a-spatiotemporal-asset-catalog-stac-up-to-date-with-sns-sqs/).

## STAC version and extensions

System supports STAC version 0.7.

The following extensions/features are available:

  * [Query](https://github.com/radiantearth/stac-spec/tree/v0.7.0/api-spec/extensions/query) API extension to filter on properties of items.
  * [EO](https://github.com/radiantearth/stac-spec/tree/v0.7.0/extensions/eo) extension to define some specific fields for items.
  * [WFS3 core endpoints](https://github.com/radiantearth/stac-spec/blob/v0.7.0/api-spec/api-spec.md).

## Live version

A live version of the stack is deployed to AWS and serve its contents in:

  * ```arn:aws:s3:::cbers-stac-0-7``` S3 bucket with static STAC content, including its [root catalog](https://cbers-stac-0-7.s3.amazonaws.com/catalog.json).
  * WFS3 [endpoint](https://stac.amskepler.com/v07/).
  * STAC [endpoint](https://stac.amskepler.com/v07/stac/).

## Development

Developed and tested in CentOS8 with python 3.8.0

JAVA is required to execute elasticsearch under localstack.
