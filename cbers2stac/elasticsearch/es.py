"""es"""

# pylint: disable=too-many-lines

import json
import logging
import os
import traceback
from typing import Any, Dict, List, Tuple, Union

from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from elasticsearch_dsl import Q, Search

from cbers2stac.layers.common.utils import (
    STAC_API_VERSION,
    get_client,
    get_collection_ids,
    get_collection_s3_key,
    get_resource,
    next_page_get_method_params,
    parse_api_gateway_event,
    static_to_api_collection,
)
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import bulk

# Get rid of "Found credentials in environment variables" messages
logging.getLogger("botocore.credentials").disabled = True
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

ES_CLIENT = None


def api_gw_lambda_integration(func):
    """
    Decorator to include standard exception handling for API GW
    lambda integration. Catches any exception and formats the message as JSON
    content, with error code 400.
    Include CORS headers in all responses
    Includes setup for localstack testing
    """

    def inner(event, context):
        try:
            retmsg = func(event, context)
            retmsg["headers"] = {
                "Content-Type": "application/json",
                "access-control-allow-origin": "*",
                "access-control-allow-headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key",
                "access-control-allow-methods": "GET,POST",
            }
            return retmsg
        except Exception as excp:  # pylint: disable=broad-except
            LOGGER.error(traceback.format_exc())
            retmsg = {
                "statusCode": "400",
                "body": json.dumps({"error": str(excp)}, indent=2),
                "headers": {"Content-Type": "application/json",},
            }
            return retmsg

    return inner


def sqs_messages(queue: str):
    """
    Generator for SQS messages.

    :param queue str: SQS URL
    :return: dict with the following keys:
             stac_item: STAC item, str
             ReceiptHandle: ditto
    :rtype: dict
    """

    while True:
        response = get_client("sqs").receive_message(QueueUrl=queue)
        if "Messages" not in response:
            break
        msg = json.loads(response["Messages"][0]["Body"])
        retd = {}
        retd["stac_item"] = msg["Message"]
        retd["ReceiptHandle"] = response["Messages"][0]["ReceiptHandle"]
        yield retd


def stac_item_from_s3_key(bucket: str, key: str):
    """
    Return a stac item from a s3 key

    :param key str: s3 key
    :param bucket str: bucket name
    :rtype: dict
    :return: stac item
    """

    obj = get_resource("s3").Object(bucket, key)
    return json.loads(obj.get()["Body"].read().decode("utf-8"))


def strip_stac_item(item: dict) -> dict:
    """
    Strips a stac item, removing not stored fields

    :param item dict: input stac item
    :rtype: dict
    :return: stripped stac item
    """

    strip = item
    s3_key = None
    for link in item["links"]:
        if link["rel"] == "self":
            s3_key = link["href"]
    assert s3_key is not None, "Can't find self key"
    # Remove fields that will not be stored
    strip.pop("stac_version")
    strip.pop("stac_extensions")
    strip.pop("type")
    strip.pop("links")
    strip.pop("bbox")
    strip.pop("assets")

    # https://cbers-stac-0-6.s3.amazonaws.com/CBERS4/PAN5M/156/107/CBERS_4_PAN5M_20150610_156_107_L4.json
    strip["s3_key"] = "/".join(s3_key.split("/")[3:])

    return strip


def process_insert_queue(
    es_client, queue: str, batch_size: int = 1, delete_messages: bool = True
):
    """
    Read queue with itemsto be inserted and send the items
    to ES.

    :param es_client: Elasticsearch client
    :param queue str: SQS URL
    :batch_size int: maximum number of messages to be processed, 0 for
                     all messages.
    """

    processed_messages = 0
    for msg in sqs_messages(queue):

        # print(msg['stac_item'])
        create_document_in_index(es_client=es_client, stac_item=msg["stac_item"])

        # Remove message from queue
        if delete_messages:
            get_client("sqs").delete_message(
                QueueUrl=queue, ReceiptHandle=msg["ReceiptHandle"]
            )

        processed_messages += 1
        if processed_messages == batch_size:
            break


def bulk_process_insert_queue(
    es_client,
    queue: str,
    batch_size: int = 1,
    delete_messages: bool = True,
    stripped: bool = False,
):
    """
    Read queue with items to be inserted and send the items
    to ES.

    :param es_client: Elasticsearch client
    :param queue str: SQS URL
    :batch_size int: maximum number of messages to be processed, 0 for
                     all messages.
    """

    processed_messages = 0
    receipts = []
    items = []
    for msg in sqs_messages(queue):

        receipts.append(msg["ReceiptHandle"])
        items.append(msg["stac_item"])

        processed_messages += 1
        if processed_messages == batch_size:
            break

    inserted_items = bulk_create_document_in_index(
        es_client=es_client, stac_items=items, update_if_exists=True, stripped=stripped
    )
    assert inserted_items == len(items), f"Mismatch, {inserted_items} - {items}"

    # Remove messages from queue
    if delete_messages:
        for receipt in receipts:
            get_client("sqs").delete_message(QueueUrl=queue, ReceiptHandle=receipt)


def parse_datetime(dtime: str):
    """
    Parse a datetime or period string from STAC request.

    :param dtime str: input datetime:
                        A date-time: "2018-02-12T23:20:50Z"
                        A period: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
                          or "2018-02-12T00:00:00Z/P1M6DT12H31M12S", the
                          former is not supported yet.
                      None is returned if None is inpue
    :return: start, end, equal to None if not defined
    """

    if not dtime:
        return None, None

    fields = dtime.split("/")
    assert len(fields) < 3 and fields, "Can't parse " + dtime
    start = fields[0]
    end = None
    if len(fields) == 2:
        end = fields[1]
    return start, end


def bbox_to_es_envelope(els: List[float]) -> List[List[float]]:
    """
    Receive a bbox using STAC representation and convert to
    ES 7 envelope convention (top left lon, top lef lat),
             (bottom right lon, bottom right lat)
    """
    assert len(els) == 4, els
    assert els[0] <= els[2], "First lon corner is not western"
    # Make sure that output bbox is top - bottom
    if els[1] < els[3]:
        els[1], els[3] = els[3], els[1]
    bbox_l = []
    bbox_l.append([els[0], els[1]])
    bbox_l.append([els[2], els[3]])
    return bbox_l


def parse_bbox(bbox: str) -> List[List[float]]:
    """
    Parse a bbox in string format from a STAC request

    :param bbox str: input bbox as CSV
    """

    els = [float(coord) for coord in bbox.split(",")]
    return bbox_to_es_envelope(els)


def es_connect(  # pylint: disable=too-many-arguments
    endpoint: str,
    port: int = None,
    use_ssl: bool = True,
    verify_certs: bool = True,
    http_auth=None,
    timeout: int = 60,
):
    """
    Connects to ES endpoint, returns Elasticsarch
    low level client.

    Args:
      endpoint: Elastic endpoint. If port is None then this is assumed
                to be a RFC-1738 format URL, otherwise it is assumed
                to be the host part of the endpoint.
      port: endpoint TCP port
      timeout: timeout in seconds

    Returns:
      class Elasticsearch
    """

    hosts: List[Union[str, Dict[str, Union[str, int]]]]
    if port is not None:
        hosts = [{"host": endpoint, "port": port}]
    else:
        hosts = [endpoint]
    es_client = Elasticsearch(
        hosts=hosts,
        use_ssl=use_ssl,
        verify_certs=verify_certs,
        connection_class=RequestsHttpConnection,
        http_auth=http_auth,
        timeout=timeout,
    )
    return es_client


def create_stac_index(es_client, timeout: int = 30):
    """
    Create STAC index.

    :param es_client: Elasticsearch client
    :param timeout int: timeout in seconds
    """

    # Check for geometry precision and impact on indexing performance
    # http://teknosrc.com/elasticsearch-geo-shape-slow-indexing-performance-solved/
    # https://www.elastic.co/guide/en/elasticsearch/reference/6.2/geo-shape.html
    # Old versions defined the geometry precision:
    #         "geometry": {
    #             "type": "geo_shape",
    #             "tree": "quadtree",
    #             "precision": "100m"

    # This mapping assumes as input a stripped stac item.
    mapping = json.dumps(
        {
            "mappings": {
                "properties": {
                    # Explicitly indexed fields
                    "id": {"type": "keyword"},
                    # Removing "tree" parameter since it is deprecated
                    # "geometry": {"type": "geo_shape", "tree": "quadtree"},
                    "geometry": {"type": "geo_shape"},
                    "collection": {"type": "keyword"},
                    "s3_key": {"type": "keyword"},
                    # Field "properties" is indexed but not explicitly
                    # Remaining fields are disabled
                    "stac_version": {"enabled": False},
                    "stac_extensions": {"enabled": False},
                    "type": {"enabled": False},
                    "assets": {"enabled": False},
                    "bbox": {"enabled": False},
                    "links": {"enabled": False},
                }
            }
        }
    )

    es_client.indices.create(index="stac", body=mapping, request_timeout=timeout)


def bulk_create_document_in_index(
    es_client, stac_items: list, update_if_exists: bool = False, stripped: bool = False,
) -> int:
    """
    Create operation, bulk mode.
    Return the number of inserted items, raise exception one update fails
    """

    # @todo include timeout option
    # @todo use generator instead of building list
    stac_updates = []

    for item in stac_items:
        if not stripped:
            dict_item = json.loads(item)
        else:
            dict_item = strip_stac_item(json.loads(item))
        if not update_if_exists:
            bulk_item = {}
            # doc type is deprecated
            # bulk_item["_type"] = "_doc"
            bulk_item["_id"] = dict_item["id"]
            bulk_item["_op_type"] = "create"
            bulk_item["_index"] = "stac"
            bulk_item["_source"] = dict_item
            stac_updates.append(bulk_item)
        else:
            bulk_item = {}
            # doc type is deprecated
            # bulk_item["_type"] = "_doc"
            bulk_item["_id"] = dict_item["id"]
            bulk_item["_op_type"] = "update"
            bulk_item["_index"] = "stac"
            bulk_item["doc"] = dict_item
            bulk_item["upsert"] = dict_item
            stac_updates.append(bulk_item)

    success, errors = bulk(es_client, stac_updates)

    assert len(errors) == 0, errors
    return success


def create_document_in_index(
    es_client,
    stac_item: str,
    update_if_exists: bool = False,
    stripped: bool = False,
    timeout: int = 30,
):
    """
    Create document in STAC index

    :param es_client: Elasticsearch client
    :param stac_item: String representing the STAC item
    :param timeout int: timeout in seconds
    """

    if not stripped:
        item = json.loads(stac_item)
    else:
        item = strip_stac_item(json.loads(stac_item))

    if not update_if_exists:
        es_client.create(
            index="stac",
            id=item["id"],
            body=item,
            # doc_type is deprecated
            # doc_type="_doc",
            request_timeout=timeout,
        )
    else:
        document = {}
        document["doc"] = item
        document["doc_as_upsert"] = True
        es_client.update(
            index="stac",
            id=document["doc"]["id"],
            body=document,
            # doc_type is deprecated
            # doc_type="_doc",
            request_timeout=timeout,
        )


def stac_search(  # pylint: disable=too-many-arguments
    es_client,
    start_date: str = None,
    end_date: str = None,
    bbox: list = None,
    limit: int = 10,
    page: int = 1,
) -> Search:
    """
    Search STAC items

    :param es_client: Elasticsearch client
    :param start_date str: ditto, format: 2014-10-21T20:03:12.963
    :param end_date str: ditto, same format as above
    :param bbox list: bounding box envelope, GeoJSON style
                      [[-180.0, -90.0], [180.0, 90.0]]
    :param limit int: max number of returned records
    :param page int: page number starting from 1
    :rtype: es.Search
    :return: built query

    query = {
        "query": {
            "bool": {
                "must": {
                    "range": {"properties.datetime": {"gte": "2014-10-21T20:03:12.963",
                                                      "lte": "2018-11-24T20:03:12.963"}}
                },
                "filter": {
                    "geo_shape": {
                        "geometry": {
                            "shape": {
                                "type": "envelope",
                                "coordinates" : [[-180.0, -90.0], [180.0, 90.0]]
                            },
                            "relation": "intersects"
                        }
                    }
                }
            }
        }
    }
    res = es_client.search(index='stac',
                           doc_type='_doc',
                           body=query)
    """

    # @todo include support for third coordinate in bbox

    search = Search(using=es_client, index="stac", doc_type="_doc")
    # https://stackoverflow.com/questions/39263663/elasticsearch-dsl-py-query-formation
    query = search.query()
    if start_date or end_date:
        date_range: Dict[str, Dict[str, str]] = {}
        date_range["properties.datetime"] = {}
        if start_date:
            date_range["properties.datetime"]["gte"] = start_date
        if end_date:
            date_range["properties.datetime"]["lte"] = end_date
        query = search.query("range", **date_range)
    if bbox:
        query = query.filter(
            "geo_shape",
            geometry={
                "shape": {"type": "envelope", "coordinates": bbox},
                "relation": "intersects",
            },
        )

    # Previously using "_id", deprecated
    query = query.sort("id")
    # query = query.query(Q("multi_match",
    #                      query="aa",
    #                      fields=['properties.provider']))
    # query = query.query(Q("match",
    #                      properties__datetime="2017-05-28T09:01:17Z"))
    # query = query.query(Q("match", **{"properties.cbers:data_type":"L2"}))

    # print(json.dumps(query.to_{}, indent=2))
    return query[(page - 1) * limit : page * limit]


def process_intersects_filter(dsl_query, geometry: dict):
    """
    Extends received query to include intersect filter with provided
    geometry

    :param dsl_query: ES DSL object
    :param geometry dict: geometry as GeoJSON
    :rtype: ES DSL object
    :return: DSL extended with query parameters
    """

    dsl_query = dsl_query.filter(
        "geo_shape",
        geometry={
            "shape": {
                "type": geometry["geometry"]["type"],
                "coordinates": geometry["geometry"]["coordinates"],
            },
            "relation": "intersects",
        },
    )
    return dsl_query


def process_collections_filter(dsl_query: Search, collections: List[str]) -> Search:
    """
    Extends received query to filter only items belonging to the
    desired collection list

    :param dsl_query: ES DSL object
    :param collections list: string list of collections
    :rtype: ES DSL object
    :return: DSL extended with query parameters
    """

    collection_or = Q(
        "bool",
        should=[Q("match", **{"collection": collection}) for collection in collections],
        minimum_should_match=1,
    )
    dsl_query = dsl_query.query(collection_or)
    return dsl_query


def process_ids_filter(dsl_query: Search, ids: List[str]) -> Search:
    """
    Extends received query to filter only items belonging to the
    desired collection list

    :param dsl_query: ES DSL object
    :param ids list: string list of ids
    :rtype: ES DSL object
    :return: DSL extended with query parameters
    """

    list_or = Q(
        "bool", should=[Q("match", **{"id": id}) for id in ids], minimum_should_match=1,
    )
    dsl_query = dsl_query.query(list_or)
    return dsl_query


def process_feature_filter(dsl_query: Search, feature_ids: list) -> Search:
    """
    Extends received query to filter only items with ids in
    the list

    :param dsl_query: ES DSL object
    :param feature_ids list: list of features ids
    :rtype: ES DSL object
    :return: DSL extended with query parameters
    """

    for feature_id in feature_ids:
        dsl_query = dsl_query.query(Q("match", **{"id": feature_id}))
    return dsl_query


def process_query_extension(dsl_query: Search, query_params: dict) -> Search:
    """
    Extends received query to include query extension parameters

    :param dsl_query: ES DSL object
    :param query_params dict: Query parameters, as defined in STAC
    :rtype: ES DSL object
    :return: DSL extended with query parameters
    """

    # See for reference on how to extend to complete STAC query extension
    # https://stackoverflow.com/questions/43138089/elasticsearch-dsl-python-unpack-q-queries
    # key is the property being queried
    for key in query_params:
        assert isinstance(query_params[key], dict), "Query prop must be a dictionary"
        for operator in query_params[key]:
            if operator == "eq":
                dsl_query = dsl_query.query(
                    Q("match", **{"properties." + key: query_params[key][operator]})
                )
            elif operator == "neq":
                dsl_query = dsl_query.query(
                    ~Q(  # pylint: disable=invalid-unary-operand-type
                        "match", **{"properties." + key: query_params[key][operator]}
                    )
                )
            elif operator in ["gt", "gte", "lt", "lte"]:
                dsl_query = dsl_query.query(
                    Q(
                        "range",
                        **{
                            "properties." + key: {operator: query_params[key][operator]}
                        },
                    )
                )
            elif operator == "startsWith":
                dsl_query = dsl_query.query(
                    Q(
                        "query_string",
                        **{
                            "default_field": "properties." + key,
                            "query": query_params[key][operator] + "*",
                        },
                    )
                )
            elif operator == "endsWith":
                dsl_query = dsl_query.query(
                    Q(
                        "query_string",
                        **{
                            "default_field": "properties." + key,
                            "query": "*" + query_params[key][operator],
                        },
                    )
                )
            elif operator == "contains":
                dsl_query = dsl_query.query(
                    Q(
                        "query_string",
                        **{
                            "default_field": "properties." + key,
                            "query": "*" + query_params[key][operator] + "*",
                        },
                    )
                )
            else:
                raise RuntimeError(f"{operator} is not a supported operator")
        # dsl_query = dsl_query.query(Q("match",
        #                              **{"properties."+key:query_params[key]}))

    return dsl_query


def query_from_event(  # pylint: disable=too-many-branches
    es_client, event
) -> Tuple[Search, dict]:
    """
    Build query from event

    :param event: event from lambda integration
    :return: Tuple with DSL query and processed parameters as a dict
    """

    document: Dict[str, Any] = {}
    if event["httpMethod"] == "GET":
        qsp = event["queryStringParameters"]
        if qsp:
            document["bbox"] = parse_bbox(qsp.get("bbox", "-180,90,180,-90"))
            document["datetime"] = qsp.get("datetime", None)
            document["limit"] = int(qsp.get("limit", "10"))
            document["page"] = int(qsp.get("page", "1"))
            if qsp.get("collections"):
                document["collections"] = qsp.get("collections").split(",")
            else:
                document["collections"] = []
            if qsp.get("ids"):
                document["ids"] = qsp.get("ids").split(",")
            else:
                document["ids"] = []
            if qsp.get("query"):
                document["query"] = json.loads(qsp.get("query"))
        else:
            document["bbox"] = parse_bbox("-180,90,180,-90")
            document["datetime"] = None
            document["limit"] = 10
            document["page"] = 1
            document["ids"] = []
    else:  # POST
        if event.get("body"):
            document = json.loads(event["body"])
        # bbox is not mandatory
        if document.get("bbox"):
            document["bbox"] = bbox_to_es_envelope(document["bbox"])
        else:
            document["bbox"] = None
        document["limit"] = int(document.get("limit", "10"))
        document["page"] = int(document.get("page", "1"))
        # print(document)

    start, end = None, None
    if document.get("datetime"):
        start, end = parse_datetime(document["datetime"])

    # Build basic query object
    query = stac_search(
        es_client=es_client,
        start_date=start,
        end_date=end,
        bbox=document["bbox"],
        limit=document["limit"],
        page=document["page"],
    )
    return query, document


def create_stac_index_handler(event, context):  # pylint: disable=unused-argument
    """
    Create STAC elasticsearch index
    """

    auth = BotoAWSRequestsAuth(
        aws_host=os.environ["ES_ENDPOINT"],
        aws_region=os.environ["AWS_REGION"],
        aws_service="es",
    )

    es_client = es_connect(
        endpoint=os.environ["ES_ENDPOINT"],
        port=int(os.environ["ES_PORT"]),
        http_auth=auth,
    )
    # print(es_client.info())
    create_stac_index(es_client)


def create_documents_handler(event, context):  # pylint: disable=unused-argument
    """
    Include document in index
    """

    global ES_CLIENT  # pylint: disable=global-statement
    if not ES_CLIENT:
        # print("Creating ES connection")
        auth = BotoAWSRequestsAuth(
            aws_host=os.environ["ES_ENDPOINT"],
            aws_region=os.environ["AWS_REGION"],
            aws_service="es",
        )
        ES_CLIENT = es_connect(
            endpoint=os.environ["ES_ENDPOINT"],
            port=int(os.environ["ES_PORT"]),
            http_auth=auth,
        )
    if "queue" in event:
        # Read STAC items from queue
        for _ in range(int(os.environ["BULK_CALLS"])):
            # process_insert_queue(es_client=es_client,
            #                     queue=event['queue'],
            #                     delete_messages=False)
            bulk_process_insert_queue(
                es_client=ES_CLIENT,
                queue=event["queue"],
                delete_messages=True,
                batch_size=int(os.environ["BULK_SIZE"]),
                stripped="ES_STRIPPED" in os.environ,
            )
    elif "Records" in event:
        # Lambda called from SQS trigger
        stac_items = []
        for record in event["Records"]:
            # print(json.dumps(record, indent=2))
            stac_items.append(json.loads(record["body"])["Message"])
        bulk_create_document_in_index(
            es_client=ES_CLIENT,
            stac_items=stac_items,
            update_if_exists=True,
            stripped="ES_STRIPPED" in os.environ,
        )

    # print(es_client.info())
    # create_document_in_index(es_client)


@api_gw_lambda_integration
def stac_search_endpoint_handler(
    event, context
):  # pylint: disable=unused-argument, too-many-branches
    """
    Lambda entry point
    """

    # todo: common code with WFS3 {collectionId/items} endpoint, unify  pylint:disable=fixme

    # LOGGER.info(event)

    # Check for local development or production environment
    if os.environ["ES_SSL"].lower() in ["y", "yes", "t", "true"]:
        auth = BotoAWSRequestsAuth(
            aws_host=os.environ["ES_ENDPOINT"],
            aws_region=os.environ["AWS_REGION"],
            aws_service="es",
        )
    else:
        auth = None

    # LOGGER.info(os.environ["ES_ENDPOINT"])
    es_client = es_connect(
        endpoint=os.environ["ES_ENDPOINT"],
        # This is just to make sure we convert to int only if ES_PORT
        # is defined.
        port=int(os.environ.get("ES_PORT"))
        if os.environ.get("ES_PORT")
        else os.environ.get("ES_PORT"),
        use_ssl=(auth is not None),
        verify_certs=(auth is not None),
        http_auth=auth,
    )

    query, document = query_from_event(es_client=es_client, event=event)

    # Process 'query' extension
    if document.get("query"):
        query = process_query_extension(dsl_query=query, query_params=document["query"])

    # Process 'intersects' filter
    if document.get("intersects"):
        query = process_intersects_filter(
            dsl_query=query, geometry=document["intersects"]
        )

    # Process 'collections' filter
    if document.get("collections"):
        query = process_collections_filter(
            dsl_query=query, collections=document["collections"]
        )

    # Process 'ids' filter
    if document.get("ids"):
        query = process_ids_filter(dsl_query=query, ids=document["ids"])

    # Execute query
    LOGGER.info(query.to_dict())
    res = query.execute()
    # LOGGER.info(type(res))
    # LOGGER.info(res.to_dict())
    results = {}
    results["stac_version"] = STAC_API_VERSION
    results["type"] = "FeatureCollection"
    results["features"] = []

    for item in res:
        LOGGER.info(item)
        item_dict = item.to_dict()
        # If s3_key is present then we recover the original item from
        # the STAC bucket
        if "s3_key" in item_dict:
            item_dict = stac_item_from_s3_key(
                bucket=os.environ["CBERS_" "STAC_BUCKET"], key=item_dict["s3_key"]
            )
        results["features"].append(item_dict)

    # Check need for rel=next object (paging)
    if document["page"] * document["limit"] < res["hits"]["total"]["value"]:
        parsed = parse_api_gateway_event(event)
        results["links"] = []
        if event["httpMethod"] == "GET":
            params = next_page_get_method_params(event["queryStringParameters"])
            results["links"].append(
                {"rel": "next", "href": f"{parsed['ppath']}?{params}"}
            )
        else:
            results["links"].append(
                {
                    "rel": "next",
                    "href": f"{parsed['ppath']}",
                    "method": "POST",
                    "body": {"page": document["page"] + 1,},
                    "merge": True,
                }
            )
    # Headers are now set by decorator
    retmsg = {
        "statusCode": "200",
        "body": json.dumps(results, indent=2),
    }

    return retmsg


@api_gw_lambda_integration
def wfs3_collections_endpoint_handler(
    event, context
):  # pylint: disable=unused-argument
    """
    Lambda entry point serving WFS3 collections requests
    """

    collections = {}
    collections["collections"] = []
    collections["links"] = []
    cids = get_collection_ids()
    for cid in cids:
        collections["collections"].append(
            static_to_api_collection(
                collection=stac_item_from_s3_key(
                    bucket=os.environ["CBERS_STAC_BUCKET"],
                    key=get_collection_s3_key(cid),
                ),
                event=event,
            )
        )
    retmsg = {
        "statusCode": "200",
        "body": json.dumps(collections, indent=2),
        "headers": {"Content-Type": "application/json",},
    }

    return retmsg


@api_gw_lambda_integration
def wfs3_collectionid_endpoint_handler(
    event, context
):  # pylint: disable=unused-argument
    """
    Lambda entry point serving WFS3 collection/{collectionId} requests
    """

    cid = event["pathParameters"]["collectionId"]
    collection = stac_item_from_s3_key(
        bucket=os.environ["CBERS_STAC_BUCKET"], key=get_collection_s3_key(cid)
    )
    retmsg = {
        "statusCode": "200",
        "body": json.dumps(
            static_to_api_collection(collection=collection, event=event), indent=2
        ),
        "headers": {"Content-Type": "application/json",},
    }

    return retmsg


@api_gw_lambda_integration
def wfs3_collectionid_items_endpoint_handler(
    event, context
):  # pylint: disable=unused-argument
    """
    Lambda entry point serving WFS3 collection/{collectionId}/items requests
    """

    # Check for local development or production environment
    if os.environ["ES_SSL"].lower() in ["y", "yes", "t", "true"]:
        auth = BotoAWSRequestsAuth(
            aws_host=os.environ["ES_ENDPOINT"],
            aws_region=os.environ["AWS_REGION"],
            aws_service="es",
        )
    else:
        auth = None

    es_client = es_connect(
        endpoint=os.environ["ES_ENDPOINT"],
        port=int(os.environ["ES_PORT"]),
        use_ssl=(auth is not None),
        verify_certs=(auth is not None),
        http_auth=auth,
    )

    cid = event["pathParameters"]["collectionId"]

    # Build basic query object
    query, document = query_from_event(  # pylint: disable=unused-variable
        es_client=es_client, event=event
    )
    # Process 'collections' filter
    query = process_collections_filter(dsl_query=query, collections=[cid])

    # Execute query
    res = query.execute()
    results = {}
    results["type"] = "FeatureCollection"
    results["features"] = []

    for item in res:
        item_dict = item.to_dict()
        # If s3_key is present then we recover the original item from
        # the STAC bucket
        if "s3_key" in item_dict:
            item_dict = stac_item_from_s3_key(
                bucket=os.environ["CBERS_" "STAC_BUCKET"], key=item_dict["s3_key"]
            )
        results["features"].append(item_dict)

    retmsg = {
        "statusCode": "200",
        "body": json.dumps(results, indent=2),
        "headers": {"Content-Type": "application/json",},
    }
    return retmsg


@api_gw_lambda_integration
def wfs3_collectionid_featureid_endpoint_handler(
    event, context
):  # pylint: disable=unused-argument
    """
    Lambda entry point serving WFS3 collection/{collectionId}/items/{featureId} requests
    """

    # Check for local development or production environment
    if os.environ["ES_SSL"].lower() in ["y", "yes", "t", "true"]:
        auth = BotoAWSRequestsAuth(
            aws_host=os.environ["ES_ENDPOINT"],
            aws_region=os.environ["AWS_REGION"],
            aws_service="es",
        )
    else:
        auth = None

    es_client = es_connect(
        endpoint=os.environ["ES_ENDPOINT"],
        port=int(os.environ["ES_PORT"]),
        use_ssl=(auth is not None),
        verify_certs=(auth is not None),
        http_auth=auth,
    )

    cid = event["pathParameters"]["collectionId"]
    fid = event["pathParameters"]["featureId"]

    # Build basic query object
    query, document = query_from_event(  # pylint: disable=unused-variable
        es_client=es_client, event=event
    )
    # Process filters
    query = process_collections_filter(dsl_query=query, collections=[cid])
    query = process_feature_filter(dsl_query=query, feature_ids=[fid])

    # Execute query
    res = query.execute()
    results = {}
    results["type"] = "FeatureCollection"
    results["features"] = []

    for item in res:
        item_dict = item.to_dict()
        # If s3_key is present then we recover the original item from
        # the STAC bucket
        if "s3_key" in item_dict:
            item_dict = stac_item_from_s3_key(
                bucket=os.environ["CBERS_" "STAC_BUCKET"], key=item_dict["s3_key"]
            )
        results["features"].append(item_dict)

    retmsg = {
        "statusCode": "200",
        "body": json.dumps(results, indent=2),
        "headers": {"Content-Type": "application/json",},
    }
    return retmsg
