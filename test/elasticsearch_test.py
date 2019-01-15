"""elasticsearch_test"""

import os
import time
import unittest
from localstack.services import infra
from localstack.utils.aws import aws_stack
from sam.elasticsearch.es import es_connect

class ElasticsearchTest(unittest.TestCase):
    """ElasticsearchTest"""

    def setUp(self):
        """localstack ES setup"""

        # Skipping ES tests in CircleCI for now, see
        # https://discuss.circleci.com/t/
        # circle-2-localstack-support/18420/3
        # for possible workaround
        if 'CI' not in os.environ.keys():
            infra.start_infra(asynchronous=True, apis=['es', 'elasticsearch'])

    def tearDown(self):
        """localstack ES teardown"""
        if 'CI' not in os.environ.keys():
            infra.stop_infra()

    def test_connection(self):
        """test_connection"""

        #if 'CI' in os.environ.keys():
        #    return

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

if __name__ == '__main__':
    unittest.main()
