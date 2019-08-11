"""utils_test"""

import json
import unittest

from sam.process_new_scene_queue.utils import get_collection_ids, \
    get_collection_s3_key, get_api_stac_root

class UtilsTest(unittest.TestCase):
    """UtilsTest"""

    def test_collection_utils(self):
        """test_collection_utils"""

        collections = get_collection_ids()
        self.assertEqual(len(collections), 4)

        mux_prefix = get_collection_s3_key('CBERS4MUX')
        self.assertEqual(mux_prefix, 'CBERS4/MUX/collection.json')

    def test_get_api_stac_root(self):
        """test_get_api_stac_root"""

        with open('test/api_event.json', 'r') as jfile:
            event = json.load(jfile)

        sroot = get_api_stac_root(event=event)
        self.assertEqual(sroot['links'][0]['self'],
                         'https://stac.amskepler.com/v07/stac')
        self.assertEqual(sroot['links'][1]['child'],
                         'https://stac.amskepler.com/v07/collections/CBERS4MUX')
        self.assertEqual(sroot['links'][4]['child'],
                         'https://stac.amskepler.com/v07/collections/CBERS4PAN5M')

        # Check correct number on second call
        sroot = get_api_stac_root(event=event)
        self.assertEqual(len(sroot['links']), 5)

if __name__ == '__main__':
    unittest.main()
