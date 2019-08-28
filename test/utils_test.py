"""utils_test"""

import json
import unittest

from sam.process_new_scene_queue.utils import get_collection_ids, \
    get_collection_s3_key, get_api_stac_root, \
    static_to_api_collection, parse_api_gateway_event

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

    def test_parse_api_gateway_event(self):
        """test_parse_api_gateway_event"""

        with open('test/api_event.json', 'r') as cfile:
            event = json.load(cfile)
        parsed = parse_api_gateway_event(event)
        self.assertEqual(parsed['phost'], 'https://stac.amskepler.com')
        self.assertEqual(parsed['ppath'], 'https://stac.amskepler.com/v07/stac')
        self.assertEqual(parsed['spath'], 'https://stac.amskepler.com/v07/stac')
        self.assertEqual(parsed['vpath'], 'https://stac.amskepler.com/v07')
        self.assertEqual(parsed['prefix'], 'v07')

    def test_static_to_api_collection(self):
        """test_static_to_api_collection"""

        with open('test/cbers4muxcollection.json', 'r') as cfile:
            collection = json.load(cfile)
        with open('test/api_event.json', 'r') as cfile:
            event = json.load(cfile)
        #from nose.tools import set_trace; set_trace()
        api_collection = static_to_api_collection(collection=collection,
                                                  event=event)

        self.assertEqual(len(api_collection['links']), 4)
        for link in api_collection['links']:
            if link['rel'] == 'self':
                self.assertEqual(link['href'],
                                 'https://stac.amskepler.com/v07/collections/CBERS4MUX')
            elif link['rel'] == 'parent':
                self.assertEqual(link['href'],
                                 'https://stac.amskepler.com/v07/stac')
            elif link['rel'] == 'root':
                self.assertEqual(link['href'],
                                 'https://stac.amskepler.com/v07/stac')
            elif link['rel'] == 'items':
                self.assertEqual(link['href'],
                                 'https://stac.amskepler.com/v07/collections/CBERS4MUX/items')


if __name__ == '__main__':
    unittest.main()
