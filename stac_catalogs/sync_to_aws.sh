# Usage: ./sync_to_aws [STACBucketName]
aws s3 cp . s3://$1 --recursive --exclude "*" --include "*catalog.json" --include "*collection.json"
