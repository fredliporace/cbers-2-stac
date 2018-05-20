"""process_new_scene_test"""

import unittest

from sam.process_new_scene_queue.code import parse_quicklook_key, \
    get_s3_keys, process_queue

class ProcessNewSceneTest(unittest.TestCase):
    """ProcessNewSceneTest"""

    def parse_quicklook_key_test(self):
        """parse_quicklook_key_test"""

        keys = parse_quicklook_key('CBERS4/AWFI/155/135/'
                                   'CBERS_4_AWFI_20170515_155_135_L2/'
                                   'CBERS_4_AWFI_20170515_155_135.jpg')
        self.assertEqual(keys['satellite'], 'CBERS4')
        self.assertEqual(keys['camera'], 'AWFI')
        self.assertEqual(keys['path'], '155')
        self.assertEqual(keys['row'], '135')
        self.assertEqual(keys['scene_id'], 'CBERS_4_AWFI_20170515_155_135_L2')

    def get_s3_keys_test(self):
        """get_s3_keys_test"""

        s3_keys = get_s3_keys('CBERS4/AWFI/155/135/'
                              'CBERS_4_AWFI_20170515_155_135_L2/'
                              'CBERS_4_AWFI_20170515_155_135.jpg')
        self.assertEqual(s3_keys['stac'],
                         'CBERS4/AWFI/155/135/CBERS_4_AWFI_20170515_155_135_L2.json')
        self.assertEqual(s3_keys['inpe_metadata'],
                         'CBERS4/AWFI/155/135/CBERS_4_AWFI_20170515_155_135_L2/'
                         'CBERS_4_AWFI_20170515_155_135_L2_BAND14.xml')

    #@unittest.skip("Require AWS credentials and environment")
    def process_queue_test(self):
        """process_queue_test"""

        process_queue(cbers_pds_bucket='cbers-pds',
                      cbers_stac_bucket='cbers-stac',
                      cbers_meta_pds_bucket='cbers-meta-pds',
                      queue='https://sqs.us-east-1.amazonaws.com/769537946825/'
                      'NewAWFIQuicklookMonitor')

if __name__ == '__main__':
    unittest.main()
