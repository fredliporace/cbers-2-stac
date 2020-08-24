"""cbers_to_stac_test"""

import os
import unittest
import json
import difflib

# This allows utils module to be imported when nosetests
# is invoked within emacs
import site
site.addsitedir('sam/process_new_scene_queue')

from jsonschema import validate, RefResolver
from jsonschema.exceptions import ValidationError

from pystac.validation import validate_dict

from sam.process_new_scene_queue.cbers_2_stac import get_keys_from_cbers, \
    build_stac_item_keys, \
    epsg_from_utm_zone, convert_inpe_to_stac

def diff_files(filename1, filename2):
    """
    Return string with context diff, empty if files are equal
    """
    with open(filename1) as file1:
        with open(filename2) as file2:
            diff = difflib.context_diff(file1.readlines(), file2.readlines())
    res = ''
    for line in diff:
        res += line # pylint: disable=consider-using-join
    return res

# @todo change debug output to give more information when
# the validation fails
def validate_json(filename):
    """
    Validate STAC item using PySTAC
    """
    with open(filename) as fname:
        jsd = json.load(fname)
    validate_dict(jsd)

json_schema_path = os.path.join(os.path.dirname(os.path.\
                                                abspath(__file__)),
                                'json_schema/item-spec/json-schema')

class CERS2StacTest(unittest.TestCase):
    """CBERS2StacTest"""

    def test_epsg_from_utm_zone(self):
        """test_epsg_from_utm_zone"""
        self.assertEqual(epsg_from_utm_zone(-23), 32723)
        self.assertEqual(epsg_from_utm_zone(23), 32623)

    def test_get_keys_from_cbers4(self):
        """test_get_keys_from_cbers"""

        # MUX
        meta = get_keys_from_cbers('test/CBERS_4_MUX_20170528_090_084_'\
                                   'L2_BAND6.xml')
        self.assertEqual(meta['mission'], 'CBERS')
        self.assertEqual(meta['number'], '4')
        self.assertEqual(meta['sensor'], 'MUX')
        self.assertEqual(meta['projection_name'], 'UTM')
        self.assertEqual(meta['origin_longitude'], '27')
        self.assertEqual(meta['origin_latitude'], '0')
        self.assertEqual(meta['collection'], 'CBERS4MUX')

        # AWFI
        meta = get_keys_from_cbers('test/CBERS_4_AWFI_20170409_167_123'\
                                   '_L4_BAND14.xml')
        self.assertEqual(meta['sensor'], 'AWFI')
        self.assertEqual(meta['mission'], 'CBERS')
        self.assertEqual(meta['number'], '4')
        self.assertEqual(meta['projection_name'], 'UTM')
        self.assertEqual(meta['origin_longitude'], '-57')
        self.assertEqual(meta['origin_latitude'], '0')
        self.assertEqual(meta['collection'], 'CBERS4AWFI')

        # PAN10
        meta = get_keys_from_cbers('test/CBERS_4_PAN10M_20190201_180_'\
                                   '125_L2_BAND2.xml')
        self.assertEqual(meta['sensor'], 'PAN10M')
        self.assertEqual(meta['mission'], 'CBERS')
        self.assertEqual(meta['number'], '4')
        self.assertEqual(meta['projection_name'], 'UTM')
        self.assertEqual(meta['origin_longitude'], '-69')
        self.assertEqual(meta['origin_latitude'], '0')
        self.assertEqual(meta['collection'], 'CBERS4PAN10M')

        # PAN5
        meta = get_keys_from_cbers('test/CBERS_4_PAN5M_20161009_219_050_'\
                                   'L2_BAND1.xml')
        self.assertEqual(meta['sensor'], 'PAN5M')
        self.assertEqual(meta['mission'], 'CBERS')
        self.assertEqual(meta['number'], '4')
        self.assertEqual(meta['projection_name'], 'UTM')
        self.assertEqual(meta['origin_longitude'], '-93')
        self.assertEqual(meta['origin_latitude'], '0')
        self.assertEqual(meta['collection'], 'CBERS4PAN5M')

        # PAN10, no gain attribute for each band
        meta = get_keys_from_cbers('test/CBERS_4_PAN10M_NOGAIN.xml')
        self.assertEqual(meta['sensor'], 'PAN10M')
        self.assertEqual(meta['mission'], 'CBERS')
        self.assertEqual(meta['number'], '4')
        self.assertEqual(meta['projection_name'], 'UTM')

    def test_get_keys_from_cbers4a(self):
        """test_get_keys_from_cbers4a"""

        # MUX
        meta = get_keys_from_cbers('test/CBERS_4A_MUX_20200808_201_137_'
                                   'L4_BAND6.xml')
        self.assertEqual(meta['mission'], 'CBERS')
        self.assertEqual(meta['number'], '4A')
        self.assertEqual(meta['sensor'], 'MUX')
        self.assertEqual(meta['projection_name'], 'UTM')
        self.assertEqual(meta['origin_longitude'], '-45')
        self.assertEqual(meta['origin_latitude'], '0')
        self.assertEqual(meta['collection'], 'CBERS4AMUX')

        # WPM
        meta = get_keys_from_cbers('test/CBERS_4A_WPM_20200730_209_139_'
                                   'L4_BAND2.xml')
        self.assertEqual(meta['sensor'], 'WPM')
        self.assertEqual(meta['mission'], 'CBERS')
        self.assertEqual(meta['number'], '4A')
        self.assertEqual(meta['projection_name'], 'UTM')
        self.assertEqual(meta['origin_longitude'], '-51')
        self.assertEqual(meta['origin_latitude'], '0')
        self.assertEqual(meta['collection'], 'CBERS4AWPM')

    def test_build_awfi_stac_item_keys(self):
        """test_awfi_build_stac_item_keys"""

        meta = get_keys_from_cbers('test/CBERS_4_AWFI_20170409_167_123_'\
                                   'L4_BAND14.xml')
        buckets = {
            'metadata':'cbers-meta-pds',
            'cog':'cbers-pds',
            'stac':'cbers-stac'}
        smeta = build_stac_item_keys(meta, buckets)

        # id
        self.assertEqual(smeta['id'], 'CBERS_4_AWFI_20170409_167_123_L4')

        # bbox
        self.assertEqual(len(smeta['bbox']), 4)
        self.assertEqual(smeta['bbox'][1], -24.425554)
        self.assertEqual(smeta['bbox'][0], -63.157102)
        self.assertEqual(smeta['bbox'][3], -16.364230)
        self.assertEqual(smeta['bbox'][2], -53.027684)

        # geometry
        self.assertEqual(len(smeta['geometry']['coordinates'][0][0]), 5)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][0][1],
                         -23.152887)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][0][0],
                         -63.086835)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][4][1],
                         -23.152887)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][4][0],
                         -63.086835)

        # properties
        self.assertEqual(smeta['properties']['datetime'],
                         '2017-04-09T14:09:23Z')

        # properties:view
        self.assertEqual(smeta['properties']['view:sun_azimuth'], 43.9164)
        self.assertEqual(smeta['properties']['view:sun_elevation'], 53.4479)
        self.assertEqual(smeta['properties']['view:off_nadir'], 0.00828942)

        # properties:proj
        self.assertEqual(smeta['properties']['proj:epsg'], 32757)

        # properties:cbers
        self.assertEqual(smeta['properties']['cbers:data_type'], 'L4')
        self.assertEqual(smeta['properties']['cbers:path'], 167)
        self.assertEqual(smeta['properties']['cbers:row'], 123)

    def test_build_mux_stac_item_keys(self):
        """test_mux_build_stac_item_keys"""

        meta = get_keys_from_cbers('test/CBERS_4_MUX_20170528_090_084_L2_'\
                                   'BAND6.xml')
        buckets = {
            'metadata':'cbers-meta-pds',
            'cog':'cbers-pds',
            'stac':'cbers-stac'}
        smeta = build_stac_item_keys(meta, buckets)

        # id
        self.assertEqual(smeta['id'], 'CBERS_4_MUX_20170528_090_084_L2')

        # bbox
        self.assertEqual(len(smeta['bbox']), 4)
        self.assertEqual(smeta['bbox'][1], 13.700498)
        self.assertEqual(smeta['bbox'][0], 23.465111)
        self.assertEqual(smeta['bbox'][3], 14.988180)
        self.assertEqual(smeta['bbox'][2], 24.812825)

        # geometry
        self.assertEqual(len(smeta['geometry']['coordinates'][0][0]), 5)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][0][1],
                         13.891487)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][0][0],
                         23.463987)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][4][1],
                         13.891487)
        self.assertEqual(smeta['geometry']['coordinates'][0][0][4][0],
                         23.463987)

        # properties
        self.assertEqual(smeta['properties']['datetime'],
                         '2017-05-28T09:01:17Z')

        # properties:view
        self.assertEqual(smeta['properties']['view:sun_azimuth'], 66.2923)
        self.assertEqual(smeta['properties']['view:sun_elevation'], 70.3079)
        self.assertEqual(smeta['properties']['view:off_nadir'], 0.00744884)

        # properties:proj
        self.assertEqual(smeta['properties']['proj:epsg'], 32627)

        # properties:cbers
        self.assertEqual(smeta['properties']['cbers:data_type'], 'L2')
        self.assertEqual(smeta['properties']['cbers:path'], 90)
        self.assertEqual(smeta['properties']['cbers:row'], 84)

        # links
        for link in smeta['links']:
            if link['rel'] == 'self':
                self.assertEqual(link['href'],
                                 'https://cbers-stac.s3.amazonaws.com/CBERS4/'\
                                 'MUX/'\
                                 '090/084/CBERS_4_MUX_20170528_090_084_L2.json')
            elif link['rel'] == 'parent':
                self.assertEqual(link['href'],
                                 'https://cbers-stac.s3.amazonaws.com/'\
                                 'CBERS4/MUX/090/084/catalog.json')
            elif link['rel'] == 'collection':
                self.assertEqual(link['href'],
                                 'https://cbers-stac.s3.amazonaws.com/'\
                                 'CBERS4/MUX/collection.json')
            else:
                self.fail('Unrecognized rel %s' % link['rel'])

        # assets
        # 4 bands, 1 metadata, 1 thumbnail
        self.assertEqual(len(smeta['assets']), 6)

    def test_build_pan10_stac_item_keys(self):
        """test_pan10_build_stac_item_keys"""

        meta = get_keys_from_cbers('test/CBERS_4_PAN10M_20190201_180_125_'\
                                   'L2_BAND2.xml')
        buckets = {
            'metadata':'cbers-meta-pds',
            'cog':'cbers-pds',
            'stac':'cbers-stac'}
        smeta = build_stac_item_keys(meta, buckets)

        # id
        self.assertEqual(smeta['id'], 'CBERS_4_PAN10M_20190201_180_125_L2')

        # bbox
        self.assertEqual(len(smeta['bbox']), 4)
        self.assertEqual(smeta['bbox'][1], -22.882858)
        self.assertEqual(smeta['bbox'][0], -71.601800)
        self.assertEqual(smeta['bbox'][3], -21.746077)
        self.assertEqual(smeta['bbox'][2], -70.762020)

        # skipping geometry values, same as other cameras
        self.assertEqual(len(smeta['geometry']['coordinates'][0][0]), 5)

        # properties
        self.assertEqual(smeta['properties']['datetime'],
                         '2019-02-01T14:36:38Z')

        # properties:view
        self.assertEqual(smeta['properties']['view:sun_azimuth'], 87.5261)
        self.assertEqual(smeta['properties']['view:sun_elevation'], 57.0749)
        self.assertEqual(smeta['properties']['view:off_nadir'], 0.0073997)

        # properties:proj
        self.assertEqual(smeta['properties']['proj:epsg'], 32769)

        # properties:cbers
        self.assertEqual(smeta['properties']['cbers:data_type'], 'L2')
        self.assertEqual(smeta['properties']['cbers:path'], 180)
        self.assertEqual(smeta['properties']['cbers:row'], 125)

        # links
        for link in smeta['links']:
            if link['rel'] == 'self':
                self.assertEqual(link['href'],
                                 'https://cbers-stac.s3.amazonaws.com/'\
                                 'CBERS4/PAN10M/180/125/CBERS_4_PAN10M_'\
                                 '20190201_180_125_L2.json')
            elif link['rel'] == 'parent':
                self.assertEqual(link['href'],
                                 'https://cbers-stac.s3.amazonaws.com/'\
                                 'CBERS4/PAN10M/180/125/catalog.json')
            elif link['rel'] == 'collection':
                self.assertEqual(link['href'],
                                 'https://cbers-stac.s3.amazonaws.com/'\
                                 'CBERS4/PAN10M/collection.json')
            else:
                self.fail('Unrecognized rel %s' % link['rel'])

        # assets
        # 3 bands, 1 metadata, 1 thumbnail
        self.assertEqual(len(smeta['assets']), 5)

    def test_build_pan5_stac_item_keys(self):
        """test_pan5_build_stac_item_keys"""

        meta = get_keys_from_cbers('test/CBERS_4_PAN5M_20161009_219_050_'\
                                   'L2_BAND1.xml')
        buckets = {
            'metadata':'cbers-meta-pds',
            'cog':'cbers-pds',
            'stac':'cbers-stac'}
        smeta = build_stac_item_keys(meta, buckets)

        # id
        self.assertEqual(smeta['id'], 'CBERS_4_PAN5M_20161009_219_050_L2')

        # bbox
        # skipping check for valus, same as other cameras
        self.assertEqual(len(smeta['bbox']), 4)

        # skipping geometry values, same as other cameras
        self.assertEqual(len(smeta['geometry']['coordinates'][0][0]), 5)

        # properties
        self.assertEqual(smeta['properties']['datetime'],
                         '2016-10-09T17:14:38Z')

        # properties:view
        self.assertEqual(smeta['properties']['view:sun_azimuth'], 167.751)
        self.assertEqual(smeta['properties']['view:sun_elevation'], 38.3015)
        self.assertEqual(smeta['properties']['view:off_nadir'], 0.0050659)

        # properties:proj
        self.assertEqual(smeta['properties']['proj:epsg'], 32793)

        # properties:cbers
        self.assertEqual(smeta['properties']['cbers:data_type'], 'L2')
        self.assertEqual(smeta['properties']['cbers:path'], 219)
        self.assertEqual(smeta['properties']['cbers:row'], 50)

        # links
        for link in smeta['links']:
            if link['rel'] == 'self':
                self.assertEqual(link['href'],
                                 'https://cbers-stac.s3.amazonaws.com/'\
                                 'CBERS4/PAN5M/219/050/CBERS_4_PAN5M_'\
                                 '20161009_219_050_L2.json')
            elif link['rel'] == 'parent':
                self.assertEqual(link['href'],
                                 'https://cbers-stac.s3.amazonaws.com/'\
                                 'CBERS4/PAN5M/219/050/catalog.json')
            elif link['rel'] == 'collection':
                self.assertEqual(link['href'],
                                 'https://cbers-stac.s3.amazonaws.com/'\
                                 'CBERS4/PAN5M/collection.json')
            else:
                self.fail('Unrecognized rel %s' % link['rel'])

        # assets
        # 1 band, 1 metadata, 1 thumbnail
        self.assertEqual(len(smeta['assets']), 3)

    def test_convert_inpe_to_stac(self):
        """test_convert_inpe_to_stac"""

        schema_path = os.path.join(json_schema_path,
                                   'item.json')
        resolver = RefResolver('file://' + json_schema_path + '/',
                               None)
        with open(schema_path) as fp_schema:
            schema = json.load(fp_schema)

        buckets = {
            'metadata':'cbers-meta-pds',
            'cog':'cbers-pds',
            'stac':'cbers-stac'}

        # MUX, CB4
        output_filename = 'test/CBERS_4_MUX_20170528_090_084_L2.json'
        ref_output_filename = 'test/ref_CBERS_4_MUX_20170528_090_084_L2.json'
        convert_inpe_to_stac(inpe_metadata_filename='test/CBERS_4_MUX_20170528'
                             '_090_084_L2_BAND6.xml',
                             stac_metadata_filename=output_filename,
                             buckets=buckets)
        validate_json(output_filename)
        with open(output_filename) as fp_in:
            self.assertEqual(validate(json.load(fp_in), schema,
                                      resolver=resolver),
                             None)
        res = diff_files(ref_output_filename, output_filename)
        self.assertEqual(len(res), 0, res)

        # AWFI, CB4
        output_filename = 'test/CBERS_4_AWFI_20170409_167_123_L4.json'
        ref_output_filename = 'test/ref_CBERS_4_AWFI_20170409_167_123_L4.json'
        convert_inpe_to_stac(inpe_metadata_filename='test/CBERS_4_AWFI_20170409'
                             '_167_123_L4_BAND14.xml',
                             stac_metadata_filename=output_filename,
                             buckets=buckets)
        validate_json(output_filename)
        with open(output_filename) as fp_in:
            self.assertEqual(validate(json.load(fp_in), schema,
                                      resolver=resolver),
                             None)
        res = diff_files(ref_output_filename, output_filename)
        self.assertEqual(len(res), 0, res)

        # PAN10M, CB4
        output_filename = 'test/CBERS_4_PAN10M_20190201_180_125_L2.json'
        ref_output_filename = output_filename.replace('test/CBERS',
                                                      'test/ref_CBERS')
        convert_inpe_to_stac(inpe_metadata_filename='test/CBERS_4_PAN10M_'
                             '20190201_180_125_L2_BAND2.xml',
                             stac_metadata_filename=output_filename,
                             buckets=buckets)
        validate_json(output_filename)
        with open(output_filename) as fp_in:
            self.assertEqual(validate(json.load(fp_in), schema,
                                      resolver=resolver),
                             None)
        res = diff_files(ref_output_filename, output_filename)
        self.assertEqual(len(res), 0, res)

        # PAN5M, CB4
        output_filename = 'test/CBERS_4_PAN5M_20161009_219_050_L2.json'
        ref_output_filename = output_filename.replace('test/CBERS',
                                                      'test/ref_CBERS')
        convert_inpe_to_stac(inpe_metadata_filename='test/CBERS_4_PAN5M_'\
                             '20161009_219_050_L2_BAND1.xml',
                             stac_metadata_filename=output_filename,
                             buckets=buckets)
        validate_json(output_filename)
        with open(output_filename) as fp_in:
            self.assertEqual(validate(json.load(fp_in), schema,
                                      resolver=resolver),
                             None)
        res = diff_files(ref_output_filename, output_filename)
        self.assertEqual(len(res), 0, res)

        # PAN10M CB4, no gain
        output_filename = 'test/CBERS_4_PAN10M_NOGAIN.json'
        ref_output_filename = output_filename.replace('test/CBERS',
                                                      'test/ref_CBERS')
        convert_inpe_to_stac(inpe_metadata_filename='test/CBERS_4_PAN10M_'
                             'NOGAIN.xml',
                             stac_metadata_filename=output_filename,
                             buckets=buckets)
        validate_json(output_filename)
        with open(output_filename) as fp_in:
            self.assertEqual(validate(json.load(fp_in), schema,
                                      resolver=resolver),
                             None)
        res = diff_files(ref_output_filename, output_filename)
        self.assertEqual(len(res), 0, res)

        # MUX, CB4A
        output_filename = 'test/CBERS_4A_MUX_20200808_201_137_L4.json'
        ref_output_filename = 'test/ref_CBERS_4A_MUX_20200808_201_137_L4.json'
        convert_inpe_to_stac(inpe_metadata_filename='test/CBERS_4A_MUX_'
                             '20200808_201_137_L4_BAND6.xml',
                             stac_metadata_filename=output_filename,
                             buckets=buckets)
        validate_json(output_filename)
        with open(output_filename) as fp_in:
            self.assertEqual(validate(json.load(fp_in), schema,
                                      resolver=resolver),
                             None)
        res = diff_files(ref_output_filename, output_filename)
        self.assertEqual(len(res), 0, res)

        # WPM, CB4A
        output_filename = 'test/CBERS_4A_WPM_20200730_209_139_L4.json'
        ref_output_filename = 'test/ref_CBERS_4A_WPM_20200730_209_139_L4.json'
        convert_inpe_to_stac(inpe_metadata_filename='test/CBERS_4A_WPM_'
                             '20200730_209_139_L4_BAND2.xml',
                             stac_metadata_filename=output_filename,
                             buckets=buckets)
        validate_json(output_filename)
        with open(output_filename) as fp_in:
            self.assertEqual(validate(json.load(fp_in), schema,
                                      resolver=resolver),
                             None)
        res = diff_files(ref_output_filename, output_filename)
        self.assertEqual(len(res), 0, res)

    def test_json_schema(self):
        """test_json_schema"""

        schema_path = os.path.join(json_schema_path,
                                   'item.json')
        resolver = RefResolver('file://' + json_schema_path + '/',
                               None)
        #self.assertEqual(schema_path, '')
        with open(schema_path) as fp_schema:
            schema = json.load(fp_schema)
        invalid_filename = 'test/CBERS_4_MUX_20170528_090_084_L2_error.json'
        with open(invalid_filename) as fp_in:
            with self.assertRaises(ValidationError) as context:
                validate(json.load(fp_in), schema, resolver=resolver)
                self.assertTrue("'links' is a required property" \
                                in str(context.exception))

if __name__ == '__main__':
    unittest.main()
