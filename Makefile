check:
	find . -name "*.json" -print0 | xargs -n 1 -0 jq '.' > /dev/null
