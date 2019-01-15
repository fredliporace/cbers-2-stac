"""es"""

import os
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

def es_connect(endpoint: str, port: int,
               use_ssl: bool = True, verify_certs: bool = True,
               http_auth=None):
    """
    Connects to ES endpoint, returns Elasticsarch
    low level client.

    :param endpoint str: Elastic endpoint
    :param port int: endpoint TCP port
    :return: Elastic low level client
    :rtype: class Elasticsearch
    """

    es_client = Elasticsearch(hosts=[{'host':endpoint,
                                      'port':port}],
                              use_ssl=use_ssl,
                              verify_certs=verify_certs,
                              connection_class=RequestsHttpConnection,
                              http_auth=http_auth)
    return es_client

def create_stac_index(es_client, timeout: int=30):
    """
    Create STAC index.

    :param es_client: Elasticsearch client
    :param timeout int: timeout in seconds
    """
    mapping = '''
{
    "mappings": {
        "_doc": {
            "properties": {
                "geometry": {
                    "type": "geo_shape",
                    "tree": "quadtree",
                    "precision": "100m"
                }
            }
        }
    }
}'''
    es_client.indices.create(index='stac', body=mapping,
                             request_timeout=timeout)

def create_stac_index_handler(event, context):
    """
    Create STAC elasticsearch index
    """

    auth = BotoAWSRequestsAuth(aws_host=os.environ['ES_ENDPOINT'],
                               aws_region=os.environ['AWS_REGION'],
                               aws_service='es')

    #es_client = Elasticsearch(hosts=[{'host':os.environ['ES_ENDPOINT'],
    #                                'port':int(os.environ['ES_PORT'])}],
    #                          connection_class=RequestsHttpConnection,
    #                          use_ssl=True,
    #                          http_auth=auth)
    #print(es_client.info())

    es_client = es_connect(endpoint=os.environ['ES_ENDPOINT'],
                           port=int(os.environ['ES_PORT']),
                           http_auth=auth)
    print(es_client.info())
    create_stac_index(es_client)
