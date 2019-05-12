# cbers-2-stac

[![CircleCI](https://circleci.com/gh/fredliporace/cbers-2-stac.svg?style=svg)](https://circleci.com/gh/fredliporace/cbers-2-stac)

Generates a STAC static and dynamic catalog of CBERS-4 AWFI, MUX, PAN10M and PAN5M cameras as they are ingested into AWS.

A live example of the full [CBERS catalog in AWS](https://registry.opendata.aws/cbers/), daily updated, is available from:

arn:aws:s3:::cbers-stac-0-6

# Documentation TODOs
 -  [] STAC static bucket must be manually created, it is not part of the stack. static collection and catalogs must also be manually changed when the bucket changes.

# STAC
 - [ ] Definitions for collections are duplicated, better to include them in catalog only?
 - [ ] Forbidden characters in catalog ids 