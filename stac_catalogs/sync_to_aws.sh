aws s3 cp . s3://cbers-stac-1-0 --recursive --exclude "*" --include "*catalog.json" --include "*.html"
