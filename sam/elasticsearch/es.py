"""es"""

import os
import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Search, Q
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

SQS_CLIENT = boto3.client('sqs')
S3 = boto3.resource('s3')
ES_CLIENT = None

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
        response = SQS_CLIENT.receive_message(
            QueueUrl=queue)
        if 'Messages' not in response:
            break
        msg = json.loads(response['Messages'][0]['Body'])
        retd = dict()
        retd['stac_item'] = msg['Message']
        retd['ReceiptHandle'] = response['Messages'][0]['ReceiptHandle']
        yield retd

def stac_item_from_s3_key(bucket: str, key: str):
    """
    Return a stac item from a s3 key

    :param key str: s3 key
    :param bucket str: bucket name
    :rtype: dict
    :return: stac item
    """

    obj = S3.Object(bucket, key)
    return json.loads(obj.get()['Body'].read().decode('utf-8'))

def strip_stac_item(item: dict):
    """
    Strips a stac item, removing not stored fields

    :param item dict: input stac item
    :rtype: dict
    :return: stripped stac item
    """

    strip = item
    strip.pop('bbox')
    s3_key = None
    for link in item['links']:
        if link['rel'] == 'self':
            s3_key = link['href']
    assert s3_key is not None, "Can't find self key"
    strip.pop('links')
    strip.pop('assets')

    # https://cbers-stac-0-6.s3.amazonaws.com/CBERS4/PAN5M/156/107/CBERS_4_PAN5M_20150610_156_107_L4.json
    strip['s3_key'] = '/'.join(s3_key.split('/')[3:])

    return strip

def process_insert_queue(es_client, queue: str,
                         batch_size: int = 1,
                         delete_messages: bool = True):
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

        #print(msg['stac_item'])
        create_document_in_index(es_client=es_client,
                                 stac_item=msg['stac_item'])

        # Remove message from queue
        if delete_messages:
            SQS_CLIENT.delete_message(
                QueueUrl=queue,
                ReceiptHandle=msg['ReceiptHandle'])

        processed_messages += 1
        if processed_messages == batch_size:
            break

def bulk_process_insert_queue(es_client, queue: str,
                              batch_size: int = 1,
                              delete_messages: bool = True,
                              stripped: bool = False):
    """
    Read queue with itemsto be inserted and send the items
    to ES.

    :param es_client: Elasticsearch client
    :param queue str: SQS URL
    :batch_size int: maximum number of messages to be processed, 0 for
                     all messages.
    """

    processed_messages = 0
    receipts = list()
    items = list()
    for msg in sqs_messages(queue):

        receipts.append(msg['ReceiptHandle'])
        items.append(msg['stac_item'])

        processed_messages += 1
        if processed_messages == batch_size:
            break

    bulk_create_document_in_index(es_client=es_client,
                                  stac_items=items,
                                  update_if_exists=True,
                                  stripped=stripped)

    # Remove messages from queue
    if delete_messages:
        for receipt in receipts:
            SQS_CLIENT.delete_message(
                QueueUrl=queue,
                ReceiptHandle=receipt)

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

    fields = dtime.split('/')
    assert len(fields) < 3 and fields, "Can't parse " + dtime
    start = fields[0]
    end = None
    if len(fields) == 2:
        end = fields[1]
    return start, end

def parse_bbox(bbox: str):
    """
    Parse a bbox in string format from a STAC request

    :param bbox str: input bbox
    :return: List of floats
    :rtype: list
    """

    els = bbox.split(',')
    bbox_l = list()
    bbox_l.append([float(els[0]), float(els[1])])
    bbox_l.append([float(els[2]), float(els[3])])
    return bbox_l

def es_connect(endpoint: str, port: int,
               use_ssl: bool = True, verify_certs: bool = True,
               http_auth=None, timeout: int = 60):
    """
    Connects to ES endpoint, returns Elasticsarch
    low level client.

    :param endpoint str: Elastic endpoint
    :param port int: endpoint TCP port
    :param timeout int: timeout in seconds
    :return: Elastic low level client
    :rtype: class Elasticsearch
    """

    es_client = Elasticsearch(hosts=[{'host':endpoint,
                                      'port':port}],
                              use_ssl=use_ssl,
                              verify_certs=verify_certs,
                              connection_class=RequestsHttpConnection,
                              http_auth=http_auth,
                              timeout=timeout)
    return es_client


def create_stac_index(es_client, mapping_name: str = "whole",
                      timeout: int = 30):
    """
    Create STAC index.

    :param es_client: Elasticsearch client
    :param mapping_name: Mapping to be created, the following options
      are supported:
        "whole": whole stac item is stored, geo and properties indexed
        "restricted": only geometry, properties and id are stored and indexed.
                      an additional field is included with the static item
                      key in S3
    :param timeout int: timeout in seconds
    """

    assert mapping_name in ("whole", "restricted"), \
        "Unrecognized mapping name, " + mapping_name

    # Check for geometry precision and impact on indexing performance
    # http://teknosrc.com/elasticsearch-geo-shape-slow-indexing-performance-solved/
    # https://www.elastic.co/guide/en/elasticsearch/reference/6.2/geo-shape.html

    # @todo this index selection turns out to be not necessary, remove

    mapping = dict()
    mapping['whole'] = '''
{
    "mappings": {
        "_doc": {
            "properties": {
                "geometry": {
                    "type": "geo_shape",
                    "tree": "quadtree"
                },
                "assets": {
                	"enabled": false
                },
                "bbox": {
                	"enabled": false
                },
                "links": {
        		"enabled": false
          	}
            }
        }
    }
}
'''

    mapping['restricted'] = '''
{
    "mappings": {
        "_doc": {
            "properties": {
                "geometry": {
                    "type": "geo_shape",
                    "tree": "quadtree"
                },
                "assets": {
                    "enabled": false
                },
                "bbox": {
                    "enabled": false
                },
                "links": {
                    "enabled": false
          	}
            }
        }
    }
}
'''

    es_client.indices.create(index='stac', body=mapping[mapping_name],
                             request_timeout=timeout)

def bulk_create_document_in_index(es_client,
                                  stac_items: list,
                                  update_if_exists: bool = False,
                                  stripped: bool = False,
                                  timeout: int = 30):
    """
    Create operation, bulk mode
    """

    # @todo include timeout option
    # @todo use generator instead of building list
    stac_updates = list()

    for item in stac_items:
        if not stripped:
            dict_item = json.loads(item)
        else:
            dict_item = strip_stac_item(json.loads(item))
        if not update_if_exists:
            bulk_item = dict()
            bulk_item['_type'] = '_doc'
            bulk_item['_id'] = dict_item['id']
            bulk_item['_op_type'] = 'create'
            bulk_item['_index'] = 'stac'
            bulk_item['_source'] = dict_item
            stac_updates.append(bulk_item)
        else:
            bulk_item = dict()
            bulk_item['_type'] = '_doc'
            bulk_item['_id'] = dict_item['id']
            bulk_item['_op_type'] = 'update'
            bulk_item['_index'] = 'stac'
            bulk_item['doc'] = dict_item
            bulk_item['upsert'] = dict_item
            stac_updates.append(bulk_item)

    success, _ = bulk(es_client, stac_updates)

    return success

def create_document_in_index(es_client,
                             stac_item: str,
                             update_if_exists: bool = False,
                             stripped: bool = False,
                             timeout: int = 30):
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
        es_client.create(index='stac', id=item['id'],
                         body=item, doc_type='_doc',
                         request_timeout=timeout)
    else:
        document = dict()
        document['doc'] = item
        document['doc_as_upsert'] = True
        es_client.update(index='stac', id=document['doc']['id'],
                         body=document, doc_type='_doc',
                         request_timeout=timeout)

def stac_search(es_client, start_date: str = None, end_date: str = None,
                bbox: list = None, limit: int = 10):
    """
    Search STAC items

    :param es_client: Elasticsearch client
    :param start_date str: ditto, format: 2014-10-21T20:03:12.963
    :param end_date str: ditto, same format as above
    :param bbox list: bounding box envelope, GeoJSON style
                      [[-180.0, -90.0], [180.0, 90.0]]
    :param limit int: number of returned records
    :rtype: es.Search
    :return: built query
    """

    # @todo include support for third coordinate in bbox

    """
    query = {
        "query": {
            "bool": {
                "must": {
                    "range": {"properties.datetime": {"gte": "2014-10-21T20:03:12.963", "lte": "2018-11-24T20:03:12.963"}}
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

    search = Search(using=es_client, index='stac', doc_type='_doc')
    # https://stackoverflow.com/questions/39263663/elasticsearch-dsl-py-query-formation
    query = search.query()
    if start_date or end_date:
        date_range = dict()
        date_range['properties.datetime'] = dict()
        if start_date:
            date_range['properties.datetime']['gte'] = start_date
        if end_date:
            date_range['properties.datetime']['lte'] = end_date
        query = search.query("range", **date_range)
    if bbox:
        query = query.filter("geo_shape",
                             geometry={"shape": {"type": "envelope",
                                                 "coordinates" : bbox},
                                       "relation": "intersects"})

    #query = query.query(Q("multi_match",
    #                      query="aa",
    #                      fields=['properties.provider']))
    #query = query.query(Q("match",
    #                      properties__datetime="2017-05-28T09:01:17Z"))
    #query = query.query(Q("match", **{"properties.cbers:data_type":"L2"}))

    #print(json.dumps(query.to_dict(), indent=2))
    return query[0:limit]

def process_query_extension(dsl_query, query_params: dict):
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
        assert isinstance(query_params[key], dict), \
            "Query prop must be a dictionary"
        for operator in query_params[key]:
            if operator == 'eq':
                dsl_query = dsl_query.\
                            query(Q("match",
                                    **{"properties."+key:query_params[key]\
                                       [operator]}))
            elif operator == 'neq':
                dsl_query = dsl_query.\
                            query(~Q("match", # pylint: disable=invalid-unary-operand-type
                                     **{"properties."+key:query_params[key]\
                                        [operator]}))
            elif operator in ['gt', 'gte', 'lt', 'lte']:
                dsl_query = dsl_query.\
                            query(Q("range",
                                    **{"properties."+key:{operator:\
                                                          query_params[key]\
                                                          [operator]}}))
            else:
                raise RuntimeError("{op} is not a supported operator".\
                                   format(op=operator))
        #dsl_query = dsl_query.query(Q("match",
        #                              **{"properties."+key:query_params[key]}))

    return dsl_query

def create_stac_index_handler(event, context): # pylint: disable=unused-argument
    """
    Create STAC elasticsearch index
    """

    auth = BotoAWSRequestsAuth(aws_host=os.environ['ES_ENDPOINT'],
                               aws_region=os.environ['AWS_REGION'],
                               aws_service='es')

    # es_client = Elasticsearch(hosts=[{'host':os.environ['ES_ENDPOINT'],
    #                                'port':int(os.environ['ES_PORT'])}],
    #                          connection_class=RequestsHttpConnection,
    #                          use_ssl=True,
    #                          http_auth=auth)
    # print(es_client.info())

    es_client = es_connect(endpoint=os.environ['ES_ENDPOINT'],
                           port=int(os.environ['ES_PORT']),
                           http_auth=auth)
    #print(es_client.info())
    create_stac_index(es_client)

def create_documents_handler(event,
                             context):  # pylint: disable=unused-argument
    """
    Include document in index
    """

    global ES_CLIENT # pylint: disable=global-statement
    if not ES_CLIENT:
        print("Creating ES connection")
        auth = BotoAWSRequestsAuth(aws_host=os.environ['ES_ENDPOINT'],
                                   aws_region=os.environ['AWS_REGION'],
                                   aws_service='es')
        ES_CLIENT = es_connect(endpoint=os.environ['ES_ENDPOINT'],
                               port=int(os.environ['ES_PORT']),
                               http_auth=auth)
    if 'queue' in event:
        # Read STAC items from queue
        for _ in range(int(os.environ['BULK_CALLS'])):
            #process_insert_queue(es_client=es_client,
            #                     queue=event['queue'],
            #                     delete_messages=False)
            bulk_process_insert_queue(es_client=ES_CLIENT,
                                      queue=event['queue'],
                                      delete_messages=True,
                                      batch_size=int(os.environ['BULK_SIZE']),
                                      stripped='ES_STRIPPED' in os.environ)
    elif 'Records' in event:
        # Lambda called from SQS trigger
        stac_items = list()
        for record in event['Records']:
            #print(json.dumps(record, indent=2))
            stac_items.append(json.loads(record['body'])['Message'])
        bulk_create_document_in_index(es_client=ES_CLIENT,
                                      stac_items=stac_items,
                                      update_if_exists=True,
                                      stripped='ES_STRIPPED' in os.environ)

    #print(es_client.info())
    #create_document_in_index(es_client)

def stac_search_endpoint_handler(event,
                                 context):  # pylint: disable=unused-argument
    """
    Lambda entry point
    """

    # Check for local development or production environment
    if os.environ['ES_SSL'].lower() in ['y', 'yes', 't', 'true']:
        auth = BotoAWSRequestsAuth(aws_host=os.environ['ES_ENDPOINT'],
                                   aws_region=os.environ['AWS_REGION'],
                                   aws_service='es')
    else:
        auth = None

    #print(os.environ['ES_ENDPOINT'])
    #print(os.environ['ES_PORT'])
    #print(os.environ['ES_SSL'])
    #print(auth)
    es_client = es_connect(endpoint=os.environ['ES_ENDPOINT'],
                           port=int(os.environ['ES_PORT']),
                           use_ssl=(auth is not None),
                           verify_certs=(auth is not None),
                           http_auth=auth)
    #print(es_client)
    #print("Checking ES connecion")
    #print(es_client.ping())
    #print("Checking ES connecion end")

    #print(json.dumps(event, indent=2))
    if event['httpMethod'] == 'GET':
        document = dict()
        qsp = event['queryStringParameters']
        # @todo process query extension for GET
        if qsp:
            document['bbox'] = parse_bbox(qsp.get('bbox', '-180,-90,180,90'))
            document['time'] = qsp.get('time', None)
            document['limit'] = int(qsp.get('limit', '10'))
        else:
            document['bbox'] = parse_bbox('-180,-90,180,90')
            document['time'] = None
            document['limit'] = 10
    else:
        document = json.loads(event['body'])
        document['bbox'] = [[document['bbox'][0], document['bbox'][1]],
                            [document['bbox'][2], document['bbox'][3]]]
        document['limit'] = int(document.get('limit', '10'))
        #print(document)

    start, end = None, None
    if document.get('time'):
        start, end = parse_datetime(document['time'])

    # Build basic query object
    query = stac_search(es_client=es_client,
                        start_date=start, end_date=end,
                        bbox=document['bbox'],
                        limit=document['limit'])

    # Process 'query' extension
    if document.get('query'):
        query = process_query_extension(dsl_query=query,
                                        query_params=document['query'])

    # Execute query
    res = query.execute()
    results = dict()
    results["type"] = "FeatureCollection"
    results["features"] = list()

    for item in res:
        item_dict = item.to_dict()
        # If s3_key is present then we recover the original item from
        # the STAC bucket
        if 's3_key' in item_dict:
            item_dict = stac_item_from_s3_key(bucket=os.environ['CBERS_'\
                                                                'STAC_BUCKET'],
                                              key=item_dict['s3_key'])
        results["features"].append(item_dict)

    retmsg = {
        'statusCode': '200',
        'body': json.dumps(results, indent=2),
        'headers': {
            'Content-Type': 'application/json',
        }
    }

    return retmsg
