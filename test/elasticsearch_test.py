"""elasticsearch_test"""

# Remove warnings when using pytest fixtures
# pylint: disable=redefined-outer-name

import json

# import os
import re
import time
from test.conftest import ENDPOINT_URL

import boto3
import pytest

import elasticsearch
from cbers2stac.elasticsearch.es import (
    bulk_create_document_in_index,
    create_document_in_index,
    create_stac_index,
    es_connect,
    parse_bbox,
    parse_datetime,
    process_collections_filter,
    process_feature_filter,
    process_intersects_filter,
    process_query_extension,
    stac_search,
    strip_stac_item,
)


@pytest.fixture
def es_client(request):
    """Elasticsarch client"""

    domain_name = "es1"

    # boto3 ES client
    es_client = None
    # ES client
    client = None

    def fin():
        """fixture finalizer"""

        # Delete all indices
        if client:
            elasticsearch.client.IndicesClient(client).delete("*")
        # Delete ES domains
        if es_client:
            es_client.delete_elasticsearch_domain(DomainName=domain_name)

    # Hook teardown (finalizer) code
    request.addfinalizer(fin)

    # Create domain to start localstack's ES service
    es_client = boto3.client("es", endpoint_url=ENDPOINT_URL)
    result = es_client.create_elasticsearch_domain(
        DomainName=domain_name, ElasticsearchVersion="7.7.0"
    )
    endpoint = result["DomainStatus"]["Endpoint"]
    match = re.match(r"http://(?P<hostname>\w+):(?P<port>\d+)", endpoint)

    client = es_connect(
        match.groupdict()["hostname"],
        port=int(match.groupdict()["port"]),
        use_ssl=False,
        verify_certs=False,
    )

    for _ in range(0, 30):
        if client.ping():
            break
        time.sleep(20)
    assert client.ping()

    # Create the stac index
    create_stac_index(client, timeout=60)

    return client


def test_parse_datetime():
    """test_parse_datetime"""

    start, end = parse_datetime("2018-02-12T23:20:50Z")
    assert start == "2018-02-12T23:20:50Z"
    assert end is None

    start, end = parse_datetime("2018-02-12T00:00:00Z/2018-03-18T12:31:12Z")
    assert start == "2018-02-12T00:00:00Z"
    assert end == "2018-03-18T12:31:12Z"

    with pytest.raises(AssertionError):
        start, end = parse_datetime(
            "2018-02-12T00:00:00Z/2018-03-18T" "12:31:12Z/ERROR"
        )

    start, end = parse_datetime(None)
    assert start is None
    assert end is None


def test_parse_bbox():
    """test_parse_bbox"""

    bbox = parse_bbox("-170,70,160,-80")
    assert len(bbox) == 2
    assert bbox[0] == [-170, 70]
    assert bbox[1] == [160, -80]

    # Check inversion for ES convention
    bbox = parse_bbox("-180,-90,180,90")
    assert len(bbox) == 2
    assert bbox[0] == [-180, 90]
    assert bbox[1] == [180, -90]

    bbox = parse_bbox("-170,-80,160,70")
    assert len(bbox) == 2
    assert bbox[0] == [-170, 70]
    assert bbox[1] == [160, -80]


def test_strip_stac_item():
    """test_strip_stac_item"""
    with open("test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json", "r") as fin:
        item = json.loads(fin.read())
    assert "bbox" in item
    strip = strip_stac_item(item)
    assert "stac_version" not in strip
    assert "stac_extensions" not in strip
    assert "type" not in strip
    assert "links" not in strip
    assert "bbox" not in strip
    assert "assets" not in strip
    assert (
        strip["s3_key"] == "CBERS4/MUX/090/084/CBERS_4_MUX_20170528_" "090_084_L2.json"
    )


def test_connection(es_client):
    """test_connection"""

    # Should be up at this point
    assert es_client.ping()


def test_create_index(es_client):
    """test_create_index"""
    if es_client.indices.exists("stac"):
        es_client.indices.delete(index="stac")
    create_stac_index(es_client, timeout=60)
    assert es_client.indices.exists("stac")


def test_create_document_in_index(es_client):
    """test_create_document_in_index"""

    assert not es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    with open("test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json", "r") as fin:
        stac_item = fin.read()
    create_document_in_index(es_client=es_client, stac_item=stac_item)
    doc = es_client.get(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert doc["_source"]["id"] == "CBERS_4_MUX_20170528_090_084_L2"

    # Calling create again should raise an exception...
    with pytest.raises(elasticsearch.ConflictError):
        create_document_in_index(es_client=es_client, stac_item=stac_item)

    # .. unless we use upsert
    create_document_in_index(
        es_client=es_client, stac_item=stac_item, update_if_exists=True
    )
    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )

    # Reset index and call create with upsert option
    es_client.indices.delete(index="stac")
    create_stac_index(es_client, timeout=60)

    assert not es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    create_document_in_index(
        es_client=es_client, stac_item=stac_item, update_if_exists=True
    )
    doc = es_client.get(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert doc["_source"]["id"] == "CBERS_4_MUX_20170528_090_084_L2"


def test_create_stripped_document_in_index(es_client):
    """test_create_stripped_document_in_index"""

    assert not es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    with open("test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json", "r") as fin:
        stac_item = json.loads(fin.read())
    create_document_in_index(
        es_client=es_client, stac_item=json.dumps(strip_stac_item(stac_item))
    )
    doc = es_client.get(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert doc["_source"]["id"] == "CBERS_4_MUX_20170528_090_084_L2"


def test_bulk_create_document_in_index(es_client):
    """test_bulk_create_document_in_index"""

    assert not es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )

    # Two distinct items, create
    stac_items = list()
    with open("test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json", "r") as fin:
        stac_items.append(fin.read())
    with open("test/fixtures/ref_CBERS_4_AWFI_20170409_167_123_L4.json", "r") as fin:
        stac_items.append(fin.read())

    bulk_create_document_in_index(es_client=es_client, stac_items=stac_items)

    doc = es_client.get(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert doc["_source"]["id"] == "CBERS_4_MUX_20170528_090_084_L2"

    doc = es_client.get(
        index="stac", doc_type="_doc", id="CBERS_4_AWFI_20170409_167_123_L4"
    )
    assert doc["_source"]["id"] == "CBERS_4_AWFI_20170409_167_123_L4"

    # Calling create again should raise an exception...
    with pytest.raises(elasticsearch.helpers.BulkIndexError):
        bulk_create_document_in_index(es_client=es_client, stac_items=stac_items)

    # ...unless we use update_if_exists option
    bulk_create_document_in_index(
        es_client=es_client, stac_items=stac_items, update_if_exists=True
    )

    doc = es_client.get(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert doc["_source"]["id"] == "CBERS_4_MUX_20170528_090_084_L2"

    doc = es_client.get(
        index="stac", doc_type="_doc", id="CBERS_4_AWFI_20170409_167_123_L4"
    )
    assert doc["_source"]["id"] == "CBERS_4_AWFI_20170409_167_123_L4"

    # Resets index and calls bulk with upsert from the start
    es_client.indices.delete(index="stac")
    create_stac_index(es_client, timeout=60)

    es_client = es_connect("localhost", port=4571, use_ssl=False, verify_certs=False)
    assert not es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )

    bulk_create_document_in_index(
        es_client=es_client, stac_items=stac_items, update_if_exists=True
    )

    doc = es_client.get(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert doc["_source"]["id"] == "CBERS_4_MUX_20170528_090_084_L2"

    doc = es_client.get(
        index="stac", doc_type="_doc", id="CBERS_4_AWFI_20170409_167_123_L4"
    )
    assert doc["_source"]["id"] == "CBERS_4_AWFI_20170409_167_123_L4"


def test_basic_search(es_client):
    """test_basic_search"""

    stac_items = list()
    with open("test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json", "r") as fin:
        stac_items.append(fin.read())
    with open("test/fixtures/ref_CBERS_4_AWFI_20170409_167_123_L4.json", "r") as fin:
        stac_items.append(fin.read())

    for stac_item in stac_items:
        create_document_in_index(es_client=es_client, stac_item=stac_item)

    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_AWFI_20170409_167_123_L4"
    )

    # All items are returned for empty query, sleeps for 2 seconds
    # before searching to allow ES to index the documents.
    # See
    # https://stackoverflow.com/questions/45936211/check-if-elasticsearch-has-finished-indexing
    # for a possibly better solution
    time.sleep(2)
    res = stac_search(es_client=es_client).execute()
    assert res["hits"]["total"]["value"] == 2
    assert len(res) == 2

    # Test limit on the number of results
    res = stac_search(es_client=es_client, limit=1).execute()
    assert res["hits"]["total"]["value"] == 2
    assert len(res) == 1

    # Single item depending on date range
    res = stac_search(
        es_client=es_client, start_date="2017-05-28T00:00:00.000"
    ).execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0]["id"] == "CBERS_4_MUX_20170528_090_084_L2"

    res = stac_search(es_client=es_client, end_date="2017-04-10T00:00:00.000").execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0]["id"] == "CBERS_4_AWFI_20170409_167_123_L4"

    # Geo search
    res = stac_search(
        es_client=es_client,
        start_date="2010-04-10T00:00:00.000",
        end_date="2018-04-10T00:00:00.000",
        bbox=[[24.13, 14.34], [24.13, 14.34]],
    )
    res = res.execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0]["id"] == "CBERS_4_MUX_20170528_090_084_L2"
    # print(res[0].to_dict())

    # Geo search for whole envelope, was raising error when
    # update to ES 7.7
    res = stac_search(
        es_client=es_client,
        start_date=None,
        end_date=None,
        bbox=[[-180.0, 90.0], [180.0, -90.0]],
    )
    res = res.execute()
    assert res["hits"]["total"]["value"] == 2

    # Query extension (eq operator only)
    empty_query = stac_search(es_client=es_client)
    res = process_query_extension(dsl_query=empty_query, query_params={}).execute()
    assert res["hits"]["total"]["value"] == 2

    query = process_query_extension(
        dsl_query=empty_query, query_params={"cbers:data_type": {"eq": "L2"}}
    )
    # print(json.dumps(query.to_dict(), indent=2))
    res = query.execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0].to_dict()["properties"]["cbers:data_type"] == "L2"

    query = process_query_extension(
        dsl_query=empty_query, query_params={"cbers:data_type": {"eq": "L4"}}
    )
    # print(json.dumps(query.to_dict(), indent=2))
    res = query.execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0].to_dict()["properties"]["cbers:data_type"] == "L4"

    query = process_query_extension(
        dsl_query=empty_query, query_params={"cbers:path": {"eq": 90}}
    )
    # print(json.dumps(query.to_dict(), indent=2))
    res = query.execute()
    # print(res.to_dict())
    # for hit in res:
    #    print(hit.to_dict())
    assert res["hits"]["total"]["value"] == 1
    assert res[0].to_dict()["properties"]["cbers:path"] == 90


def test_query_extension_search(es_client):  # pylint: disable=too-many-statements
    """test_query_extension_search"""

    stac_items = list()
    with open("test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json", "r") as fin:
        stac_items.append(fin.read())
    with open("test/fixtures/ref_CBERS_4_AWFI_20170409_167_123_L4.json", "r") as fin:
        stac_items.append(fin.read())

    for stac_item in stac_items:
        create_document_in_index(es_client=es_client, stac_item=stac_item)

    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_AWFI_20170409_167_123_L4"
    )

    empty_query = stac_search(es_client=es_client)
    q_dsl = process_query_extension(dsl_query=empty_query, query_params={})

    # Start with an empty query
    assert q_dsl.to_dict()["query"] == {"match_all": {}}

    # eq operator
    q_payload = {"instruments": {"eq": '["MUX"]'}, "cbers:data_type": {"eq": "L2"}}
    q_dsl = process_query_extension(dsl_query=empty_query, query_params=q_payload)
    assert q_dsl.to_dict()["query"] == {
        "bool": {
            "must": [
                {"match": {"properties.instruments": '["MUX"]'}},
                {"match": {"properties.cbers:data_type": "L2"}},
            ]
        }
    }
    # All items are returned for empty query, sleeps for 2 seconds
    # before searching to allow ES to index the documents.
    # See
    # https://stackoverflow.com/questions/45936211/check-if-elasticsearch-has-finished-indexing
    # for a possibly better solution
    time.sleep(2)
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0].to_dict()["properties"]["cbers:data_type"] == "L2"

    # neq and eq operator
    q_payload = {"instruments": {"eq": '["MUX"]'}, "cbers:data_type": {"neq": "L4"}}
    q_dsl = process_query_extension(dsl_query=empty_query, query_params=q_payload)

    assert q_dsl.to_dict()["query"] == {
        "bool": {
            "must_not": [{"match": {"properties.cbers:data_type": "L4"}}],
            "must": [{"match": {"properties.instruments": '["MUX"]'}}],
        }
    }
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0].to_dict()["properties"]["cbers:data_type"] == "L2"

    # gt, gte, lt, lte operators
    q_payload = {"cbers:path": {"gte": 90, "lte": 90}}
    q_dsl = process_query_extension(dsl_query=empty_query, query_params=q_payload)
    # print(q_dsl.to_dict()['query'])
    assert q_dsl.to_dict()["query"] == {
        "bool": {
            "must": [
                {"range": {"properties.cbers:path": {"gte": 90}}},
                {"range": {"properties.cbers:path": {"lte": 90}}},
            ]
        }
    }
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0].to_dict()["properties"]["cbers:path"] == 90

    q_payload = {"cbers:path": {"gt": 90, "lt": 90}}
    q_dsl = process_query_extension(dsl_query=empty_query, query_params=q_payload)
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 0

    # startsWith operator
    q_payload = {
        "cbers:data_type": {"startsWith": "L"},
        "instruments": {"eq": '["MUX"]'},
    }
    q_dsl = process_query_extension(dsl_query=empty_query, query_params=q_payload)
    # assert q_dsl.to_dict()['query'] == \
    #                     {'bool':
    #                      {'must':
    #                       [{'query_string':
    #                         {'default_field': 'properties.cbers:data_type',
    #                          'query': 'L*'}},
    #                        {'query_string':
    #                         {'default_field': 'properties.instruments',
    #                          'query': '["MU*'}}]}}
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0].to_dict()["properties"]["cbers:data_type"] == "L2"
    assert res[0].to_dict()["properties"]["instruments"] == ["MUX"]

    # endsWith, contains operators
    q_payload = {"cbers:data_type": {"endsWith": "2"}, "instruments": {"contains": "U"}}
    q_dsl = process_query_extension(dsl_query=empty_query, query_params=q_payload)
    # print(q_dsl.to_dict()['query'])
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0].to_dict()["properties"]["cbers:data_type"] == "L2"
    assert res[0].to_dict()["properties"]["instruments"] == ["MUX"]


def test_collection_filter_search(es_client):
    """test_collection_filter_search"""

    stac_items = list()
    with open("test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json", "r") as fin:
        stac_items.append(fin.read())
    with open("test/fixtures/ref_CBERS_4_AWFI_20170409_167_123_L4.json", "r") as fin:
        stac_items.append(fin.read())

    for stac_item in stac_items:
        create_document_in_index(es_client=es_client, stac_item=stac_item)

    # Sleeps for 2 seconds
    # before searching to allow ES to index the documents.
    # See
    # https://stackoverflow.com/questions/45936211/check-if-elasticsearch-has-finished-indexing
    # for a possibly better solution
    time.sleep(2)

    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_AWFI_20170409_167_123_L4"
    )

    empty_query = stac_search(es_client=es_client)

    # Only items in MUX collection
    q_dsl = process_collections_filter(
        dsl_query=empty_query, collections=["CBERS4-MUX"]
    )
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0].to_dict()["properties"]["instruments"][0] == "MUX"

    # Only items in AWFI collection
    q_dsl = process_collections_filter(
        dsl_query=empty_query, collections=["CBERS4-AWFI"]
    )
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0].to_dict()["properties"]["instruments"][0] == "AWFI"

    # Unknown collection, should return no items
    q_dsl = process_collections_filter(
        dsl_query=empty_query, collections=["NOCOLLECTIONS"]
    )
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 0


def test_feature_filter_search(es_client):
    """test_feature_filter_search"""

    stac_items = list()
    with open("test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json", "r") as fin:
        stac_items.append(fin.read())
    with open("test/fixtures/ref_CBERS_4_AWFI_20170409_167_123_L4.json", "r") as fin:
        stac_items.append(fin.read())

    for stac_item in stac_items:
        create_document_in_index(es_client=es_client, stac_item=stac_item)

    # Sleeps for 2 seconds
    # before searching to allow ES to index the documents.
    # See
    # https://stackoverflow.com/questions/45936211/check-if-elasticsearch-has-finished-indexing
    # for a possibly better solution
    time.sleep(2)

    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_AWFI_20170409_167_123_L4"
    )

    empty_query = stac_search(es_client=es_client)

    # Single MUX item
    q_dsl = process_feature_filter(
        dsl_query=empty_query, feature_ids=["CBERS_4_MUX_20170528_090_084_L2"]
    )
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0].to_dict()["id"] == "CBERS_4_MUX_20170528_090_084_L2"

    # Unknown collection, should return no items
    q_dsl = process_feature_filter(dsl_query=empty_query, feature_ids=["NOID"])
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 0


def test_process_intersects_filter(es_client):
    """test_process_intersects_filter"""

    stac_items = list()
    with open("test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json", "r") as fin:
        stac_items.append(fin.read())
    with open("test/fixtures/ref_CBERS_4_AWFI_20170409_167_123_L4.json", "r") as fin:
        stac_items.append(fin.read())

    for stac_item in stac_items:
        create_document_in_index(es_client=es_client, stac_item=stac_item)

    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_AWFI_20170409_167_123_L4"
    )

    empty_query = stac_search(es_client=es_client)
    geometry = {
        "type": "Feature",
        "properties": {},
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-180.0, -90.0],
                    [-180.0, 90.0],
                    [180.0, 90.0],
                    [180.0, -90.0],
                    [-180.0, -90.0],
                ]
            ],
        },
    }
    q_dsl = process_intersects_filter(dsl_query=empty_query, geometry=geometry)
    # All items are returned for empty query, sleeps for 2 seconds
    # before searching to allow ES to index the documents.
    # See
    # https://stackoverflow.com/questions/45936211/check-if-elasticsearch-has-finished-indexing
    # for a possibly better solution
    time.sleep(2)
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 2

    # Change the polygon to intersect with a single scene
    geometry["geometry"]["coordinates"] = [
        [[23, 13], [25, 13], [25, 15], [23, 15], [23, 13]]
    ]
    q_dsl = process_intersects_filter(dsl_query=empty_query, geometry=geometry)
    res = q_dsl.execute()
    assert res["hits"]["total"]["value"] == 1
    assert res[0].to_dict()["properties"]["instruments"][0] == "MUX"


def test_paging(es_client):
    """test_paging"""

    stac_items = list()
    with open("test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json", "r") as fin:
        stac_items.append(fin.read())
    with open("test/fixtures/ref_CBERS_4_AWFI_20170409_167_123_L4.json", "r") as fin:
        stac_items.append(fin.read())

    for stac_item in stac_items:
        create_document_in_index(es_client=es_client, stac_item=stac_item)

    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_MUX_20170528_090_084_L2"
    )
    assert es_client.exists(
        index="stac", doc_type="_doc", id="CBERS_4_AWFI_20170409_167_123_L4"
    )

    # All items are returned for empty query, sleeps for 2 seconds
    # before searching to allow ES to index the documents.
    # See
    # https://stackoverflow.com/questions/45936211/check-if-elasticsearch-has-finished-indexing
    # for a possibly better solution
    time.sleep(2)
    res = stac_search(es_client=es_client, limit=1, page=1).execute()
    assert len(res) == 1
    assert res["hits"]["total"]["value"] == 2
    assert res[0]["id"] == "CBERS_4_AWFI_20170409_167_123_L4"

    res = stac_search(es_client=es_client, limit=1, page=2).execute()
    assert len(res) == 1
    assert res["hits"]["total"]["value"] == 2
    assert res[0]["id"] == "CBERS_4_MUX_20170528_090_084_L2"

    # past last page
    res = stac_search(es_client=es_client, limit=1, page=3).execute()
    assert len(res) == 0
    assert res["hits"]["total"]["value"] == 2
