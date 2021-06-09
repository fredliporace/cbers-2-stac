ES_URL=http://localhost:4571
INDEX=stac

echo "Create index and mapping"
curl -X PUT -H "Content-Type: application/json" -d @./stac_mappings.json $ES_URL/$INDEX

echo
echo "Insert item"
curl -X PUT -H "Content-Type: application/json" -d @../test/CBERS_4_MUX_20170528_090_084_L2.json $ES_URL/$INDEX/_doc/CBERS_4_MUX_20170528_090_084_L2

echo
echo "Waiting some seconds for document to be indexed"
sleep 5

echo
echo "Search, geometry only, all coverage"
curl -X GET -H "Content-Type: application/json" -d @./search_geometry.json $ES_URL/$INDEX/_search?pretty

echo
echo "Search, geometry only, no intersection"
curl -X GET -H "Content-Type: application/json" -d @./search_geometry_no_intersection.json $ES_URL/$INDEX/_search?pretty

echo
echo "Search, date only"
curl -X GET -H "Content-Type: application/json" -d @./search_date.json $ES_URL/$INDEX/_search?pretty

echo
echo "Search, date & geometry"
curl -X GET -H "Content-Type: application/json" -d @./search_date_geometry.json $ES_URL/$INDEX/_search?pretty
