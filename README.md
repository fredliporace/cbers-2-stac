# cbers-2-stac

[![CircleCI](https://circleci.com/gh/fredliporace/cbers-2-stac.svg?style=svg)](https://circleci.com/gh/fredliporace/cbers-2-stac)

Generates a STAC static catalog of CBERS-4 AWFI and MUX cameras as they are ingested in AWS.

A live example of data updated daily is available from

arn:aws:s3:::cbers-stac

The bucket contents browse is:

https://cbers-stac.s3.amazonaws.com/index.html

Note that this bucket data will be reset as changes are made to the STAC generation procedure, and will not reflect - at least for now- the full [CBERS catalog in AWS](https://registry.opendata.aws/cbers/).
