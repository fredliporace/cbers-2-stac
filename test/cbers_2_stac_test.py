"""cbers_to_stac_test"""

import unittest

from cbers_2_stac import get_keys_from_cbers, \
    build_stac_item_keys, create_json_item, \
    epsg_from_utm_zone

class CERS2StacTest(unittest.TestCase):
    """CBERS2StacTest"""

    def epsg_from_utm_zone(self):
        """test_epsg_from_utm_zone"""
        self.assertEqual( epsg_from_utm_zone(-23), 32723)
        self.assertEqual( epsg_from_utm_zone(23), 32623)
    
    def test_get_keys_from_cbers(self):
        """test_get_keys_from_cbers"""

        # MUX
        meta = get_keys_from_cbers('test/CBERS_4_MUX_20170528_090_084_L2_BAND6.xml')
        self.assertEqual(meta['sensor'], 'MUX')
        self.assertEqual(meta['projection_name'], 'UTM')
        self.assertEqual(meta['origin_longitude'], '27')
        self.assertEqual(meta['origin_latitude'], '0')

        # AWFI
        meta = get_keys_from_cbers('test/CBERS_4_AWFI_20170409_167_123_L4_BAND14.xml')
        self.assertEqual(meta['sensor'], 'AWFI')
        self.assertEqual(meta['projection_name'], 'UTM')
        self.assertEqual(meta['origin_longitude'], '-57')
        self.assertEqual(meta['origin_latitude'], '0')
        
    def test_build_awfi_stac_item_keys(self):
        """test_awfi_build_stac_item_keys"""

        meta = get_keys_from_cbers('test/CBERS_4_AWFI_20170409_167_123_L4_BAND14.xml')
        smeta = build_stac_item_keys(meta)

        # id
        self.assertEqual(smeta['id'], 'CBERS_4_AWFI_20170409_167_123_L4')

        # bbox
        self.assertEqual(len(smeta['bbox']), 4)
        self.assertEqual(smeta['bbox'][0], -24.425554)
        self.assertEqual(smeta['bbox'][1], -63.157102)
        self.assertEqual(smeta['bbox'][2], -16.364230)
        self.assertEqual(smeta['bbox'][3], -53.027684)

        # geometry
        self.assertEqual(len(smeta['geometry']['coordinates'][0][0]), 5)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][0][0], -23.152887)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][0][1], -63.086835)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][4][0], -23.152887)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][4][1], -63.086835)

        # properties
        self.assertEqual(smeta['properties']['datetime'], '2017-04-09T14:09:23Z')

        # properties:eo
        self.assertEqual(smeta['properties']['eo:collection'], 'default')
        self.assertEqual(smeta['properties']['eo:sun_azimuth'], 43.9164)
        self.assertEqual(smeta['properties']['eo:sun_elevation'], 53.4479)
        #self.assertEqual(smeta['properties']['eo:resolution'], 20.)
        self.assertEqual(smeta['properties']['eo:off_nadir'], -0.00828942)
        self.assertEqual(smeta['properties']['eo:epsg'], 32757)

        # properties:cbers
        self.assertEqual(smeta['properties']['cbers:data_type'], 'L4')
        self.assertEqual(smeta['properties']['cbers:path'], 167)
        self.assertEqual(smeta['properties']['cbers:row'], 123)

        def test_build_mux_stac_item_keys(self):
            """test_mux_build_stac_item_keys"""

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

        # properties:eo
        self.assertEqual(smeta['properties']['eo:collection'], 'default')
        self.assertEqual(smeta['properties']['eo:sun_azimuth'], 66.2923)
        self.assertEqual(smeta['properties']['eo:sun_elevation'], 70.3079)
        #self.assertEqual(smeta['properties']['eo:resolution'], 20.)
        self.assertEqual(smeta['properties']['eo:off_nadir'], -0.00744884)
        self.assertEqual(smeta['properties']['eo:epsg'], 32627)

        # properties:cbers
        self.assertEqual(smeta['properties']['cbers:data_type'], 'L2')
        self.assertEqual(smeta['properties']['cbers:path'], 90)
        self.assertEqual(smeta['properties']['cbers:row'], 84)

    def test_create_mux_json_item(self):
        """test_create_mux_json_item"""

        meta = get_keys_from_cbers('test/CBERS_4_MUX_20170528_090_084_L2_BAND6.xml')
        smeta = build_stac_item_keys(meta)
        create_json_item(smeta, 'test/CBERS_4_MUX_20170528_090_084_L2.json')

    def test_create_awfi_json_item(self):
        """test_create_awfi_json_item"""

        meta = get_keys_from_cbers('test/CBERS_4_AWFI_20170409_167_123_L4_BAND14.xml')
        smeta = build_stac_item_keys(meta)
        create_json_item(smeta, 'test/CBERS_4_AWFI_20170409_167_123_L4.json')
        
if __name__ == '__main__':
    unittest.main()
