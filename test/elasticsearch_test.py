"""elasticsearch_test"""

import os
import time
import unittest
import json
from localstack.services import infra
from elasticsearch.helpers import BulkIndexError
from elasticsearch import ConflictError
#from localstack.utils.aws import aws_stack
from sam.elasticsearch.es import es_connect, create_stac_index, \
    create_document_in_index, bulk_create_document_in_index, \
    stac_search

class ElasticsearchTest(unittest.TestCase):
    """ElasticsearchTest"""

    def setUp(self):
        """localstack ES setup"""
        # We only try to start localstack ES if there is
        # no connection to the server.
        es_client = es_connect('localhost', port=4571,
                               use_ssl=False, verify_certs=False)
        if not es_client.ping():
            # Skipping localstack setup when running in CircleCI
            if 'CI' not in os.environ.keys():
                infra.start_infra(asynchronous=True,
                                  apis=['es', 'elasticsearch'])

    def tearDown(self):
        """localstack ES teardown"""
        if 'CI' not in os.environ.keys():
            infra.stop_infra()

    def test_connection(self):
        """test_connection"""

        # Parameters may be obtained from localstack, using
        # default parameters for now
        #es_url = aws_stack.get_local_service_url('elasticsearch')
        #self.assertEqual(es_url, 'http://osboxes:4578')
        es_client = es_connect('localhost', port=4571,
                               use_ssl=False, verify_certs=False)
        # Wait up to 30 seconds approximately for ES service
        # to be up
        for _ in range(0, 30):
            if es_client.ping():
                break
            else:
                time.sleep(1)
        self.assertTrue(es_client.ping())

    def test_create_index(self):
        """test_create_index"""
        es_client = es_connect('localhost', port=4571,
                               use_ssl=False, verify_certs=False)
        if es_client.indices.exists('stac'):
            es_client.indices.delete(index='stac')
        create_stac_index(es_client, timeout=60)
        self.assertTrue(es_client.indices.exists('stac'))

    def test_create_document_in_index(self):
        """test_create_document_in_index"""

        # Create an empty index
        self.test_create_index()

        es_client = es_connect('localhost', port=4571,
                               use_ssl=False, verify_certs=False)
        self.assertFalse(es_client.
                         exists(index='stac', doc_type='_doc',
                                id='CBERS_4_MUX_20170528_090_084_L2'))
        with open('test/CBERS_4_MUX_20170528_090_084_L2.json',
                  'r') as fin:
            stac_item = fin.read()
        create_document_in_index(es_client=es_client,
                                 stac_item=stac_item)
        doc = es_client.get(index='stac', doc_type='_doc',
                            id='CBERS_4_MUX_20170528_090_084_L2')
        self.assertEqual(doc['_source']['id'],
                         'CBERS_4_MUX_20170528_090_084_L2')

        # Calling create again should raise an exception...
        with self.assertRaises(ConflictError):
            create_document_in_index(es_client=es_client,
                                     stac_item=stac_item)

        # .. unless we use upsert
        create_document_in_index(es_client=es_client,
                                 stac_item=stac_item,
                                 update_if_exists=True)
        self.assertTrue(es_client.exists(index='stac', doc_type='_doc',
                                         id='CBERS_4_MUX_20170528_090_084_L2'))

        # Reset index and call create with upsert option
        self.test_create_index()

        es_client = es_connect('localhost', port=4571,
                               use_ssl=False, verify_certs=False)
        self.assertFalse(es_client.
                         exists(index='stac', doc_type='_doc',
                                id='CBERS_4_MUX_20170528_090_084_L2'))
        create_document_in_index(es_client=es_client,
                                 stac_item=stac_item,
                                 update_if_exists=True)
        doc = es_client.get(index='stac', doc_type='_doc',
                            id='CBERS_4_MUX_20170528_090_084_L2')
        self.assertEqual(doc['_source']['id'],
                         'CBERS_4_MUX_20170528_090_084_L2')


    def test_bulk_create_document_in_index(self):
        """test_bulk_create_document_in_index"""

        # Create an empty index
        self.test_create_index()

        es_client = es_connect('localhost', port=4571,
                               use_ssl=False, verify_certs=False)
        self.assertFalse(es_client.
                         exists(index='stac', doc_type='_doc',
                                id='CBERS_4_MUX_20170528_090_084_L2'))

        # Two distinct items, create
        stac_items = list()
        with open('test/CBERS_4_MUX_20170528_090_084_L2.json',
                  'r') as fin:
            stac_items.append(fin.read())
        with open('test/CBERS_4_AWFI_20170409_167_123_L4.json',
                  'r') as fin:
            stac_items.append(fin.read())

        bulk_create_document_in_index(es_client=es_client,
                                      stac_items=stac_items)

        doc = es_client.get(index='stac', doc_type='_doc',
                            id='CBERS_4_MUX_20170528_090_084_L2')
        self.assertEqual(doc['_source']['id'],
                         'CBERS_4_MUX_20170528_090_084_L2')

        doc = es_client.get(index='stac', doc_type='_doc',
                            id='CBERS_4_AWFI_20170409_167_123_L4')
        self.assertEqual(doc['_source']['id'],
                         'CBERS_4_AWFI_20170409_167_123_L4')

        # Calling create again should raise an exception...
        with self.assertRaises(BulkIndexError):
            bulk_create_document_in_index(es_client=es_client,
                                          stac_items=stac_items)

        # ...unless we use update_if_exists option
        bulk_create_document_in_index(es_client=es_client,
                                      stac_items=stac_items,
                                      update_if_exists=True)

        doc = es_client.get(index='stac', doc_type='_doc',
                            id='CBERS_4_MUX_20170528_090_084_L2')
        self.assertEqual(doc['_source']['id'],
                         'CBERS_4_MUX_20170528_090_084_L2')

        doc = es_client.get(index='stac', doc_type='_doc',
                            id='CBERS_4_AWFI_20170409_167_123_L4')
        self.assertEqual(doc['_source']['id'],
                         'CBERS_4_AWFI_20170409_167_123_L4')

        # Resets index and calls bulk with upsert from the start
        self.test_create_index()

        es_client = es_connect('localhost', port=4571,
                               use_ssl=False, verify_certs=False)
        self.assertFalse(es_client.
                         exists(index='stac', doc_type='_doc',
                                id='CBERS_4_MUX_20170528_090_084_L2'))

        bulk_create_document_in_index(es_client=es_client,
                                      stac_items=stac_items,
                                      update_if_exists=True)

        doc = es_client.get(index='stac', doc_type='_doc',
                            id='CBERS_4_MUX_20170528_090_084_L2')
        self.assertEqual(doc['_source']['id'],
                         'CBERS_4_MUX_20170528_090_084_L2')

        doc = es_client.get(index='stac', doc_type='_doc',
                            id='CBERS_4_AWFI_20170409_167_123_L4')
        self.assertEqual(doc['_source']['id'],
                         'CBERS_4_AWFI_20170409_167_123_L4')


    def test_stac_search(self):
        """test_stac_search"""

        # Create an empty index
        self.test_create_index()

        es_client = es_connect('localhost', port=4571,
                               use_ssl=False, verify_certs=False)
        stac_items = list()
        with open('test/CBERS_4_MUX_20170528_090_084_L2.json',
                  'r') as fin:
            stac_items.append(fin.read())
        with open('test/CBERS_4_AWFI_20170409_167_123_L4.json',
                  'r') as fin:
            stac_items.append(fin.read())

        for stac_item in stac_items:
            create_document_in_index(es_client=es_client,
                                     stac_item=stac_item)

        self.assertTrue(es_client.exists(index='stac', doc_type='_doc',
                                         id='CBERS_4_MUX_20170528_090_084_L2'))
        self.assertTrue(es_client.exists(index='stac', doc_type='_doc',
                                         id='CBERS_4_AWFI_20170409_167_123_L4'))

        # All items are returned for empty query
        res = stac_search(es_client=es_client)
        self.assertEqual(res['hits']['total'], 2)

        # Single item depending on date range
        res = stac_search(es_client=es_client,
                          start_date='2017-05-28T00:00:00.000')
        self.assertEqual(res['hits']['total'], 1)
        self.assertEqual(res[0]['id'], 'CBERS_4_MUX_20170528_090_084_L2')

        res = stac_search(es_client=es_client,
                          end_date='2017-04-10T00:00:00.000')
        self.assertEqual(res['hits']['total'], 1)
        self.assertEqual(res[0]['id'], 'CBERS_4_AWFI_20170409_167_123_L4')

        # Geo search
        res = stac_search(es_client=es_client,
                          start_date='2010-04-10T00:00:00.000',
                          end_date='2018-04-10T00:00:00.000',
                          bbox=[[24.13, 14.34], [24.13, 14.34]])
        self.assertEqual(res['hits']['total'], 1)
        self.assertEqual(res[0]['id'], 'CBERS_4_MUX_20170528_090_084_L2')

if __name__ == '__main__':
    unittest.main()
