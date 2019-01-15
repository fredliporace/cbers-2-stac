"""elasticsearch_test"""

import os
import time
import unittest
from localstack.services import infra
#from localstack.utils.aws import aws_stack
from sam.elasticsearch.es import es_connect, create_stac_index

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

if __name__ == '__main__':
    unittest.main()
