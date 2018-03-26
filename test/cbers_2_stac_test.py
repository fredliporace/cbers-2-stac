"""cbers_to_stac_test"""

import unittest

from cbers_2_stac import get_keys_from_cbers, \
    build_stac_item_keys, create_json_item

class CERS2StacTest(unittest.TestCase):
    """CBERS2StacTest"""

    def test_get_keys_from_cbers(self):
        """test_get_keys_from_cbers"""
        meta = get_keys_from_cbers('test/CBERS_4_MUX_20170528_090_084_L2_BAND6.xml')
        self.assertEqual(meta['sensor'], 'MUX')

    def test_build_stac_item_keys(self):
        """test_build_stac_item_keys"""

        meta = get_keys_from_cbers('test/CBERS_4_MUX_20170528_090_084_L2_BAND6.xml')
        smeta = build_stac_item_keys(meta)

        # id
        self.assertEqual(smeta['id'], 'CBERS_4_MUX_20170528_090_084_L2')

        # bbox
        self.assertEqual(len(smeta['bbox']), 4)
        self.assertEqual(smeta['bbox'][0], 13.700498)
        self.assertEqual(smeta['bbox'][1], 23.465111)
        self.assertEqual(smeta['bbox'][2], 14.988180)
        self.assertEqual(smeta['bbox'][3], 24.812825)

        # geometry
        self.assertEqual(len(smeta['geometry']['coordinates'][0][0]), 5)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][0][0], 13.891487)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][0][1], 23.463987)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][4][0], 13.891487)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][4][1], 23.463987)

        # properties
        self.assertEqual(smeta['properties']['datetime'], '2017-05-28T09:01:17Z')
        
    def test_create_json_item(self):
        """test_create_json_item"""

        meta = get_keys_from_cbers('test/CBERS_4_MUX_20170528_090_084_L2_BAND6.xml')
        smeta = build_stac_item_keys(meta)
        create_json_item(smeta, 'test/CBERS_4_MUX_20170528_090_084_L2.json')
        
if __name__ == '__main__':
    unittest.main()
