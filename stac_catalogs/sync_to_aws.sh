aws s3 sync . s3://cbers-meta-pds/stac --exclude "*" --include "*.json" --include "*.html"
