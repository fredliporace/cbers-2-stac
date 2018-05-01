cd ..
./update_stac_catalog.sh
cd -
aws s3 sync . s3://cbers-stac --exclude "*" --include "*.json" --include "*.html"
