# Release notes

## Unreleased

* Support for Amazonia-1
* Development environment update to localstack V1.
* `constraints.txt` included
* Release step removed from CI, skip CI for .md files
* Vault directory removed
* Canary to check STAC API search endpoint (https://github.com/fredliporace/cbers-2-stac/issues/83)
* Better check for errors in bulk ES operations (https://github.com/fredliporace/cbers-2-stac/issues/74)
* Enabling API GW log at INFO level (https://github.com/fredliporace/cbers-2-stac/issues/72)
* Fixing codecov report (https://github.com/fredliporace/cbers-2-stac/issues/75)
* Deleting some resources that were kept after stack creation (https://github.com/fredliporace/cbers-2-stac/issues/67)
* Alternative to update static catalogs and collections in populated buckets (https://github.com/fredliporace/cbers-2-stac/issues/88)
* Queue to hold corrupted/inexistent XML files (https://github.com/fredliporace/cbers-2-stac/issues/89)

## 1.0.0 (2021-06-09)

* First release with STAC 1.0.0 support
