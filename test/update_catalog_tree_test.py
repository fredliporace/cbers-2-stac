"""update_catalog_tree_test"""

import datetime
import unittest
import shutil
import os
import json
from dateutil.tz import tzutc

from jsonschema import validate, RefResolver

from sam.update_catalog_tree.code import get_items_from_s3, get_catalogs_from_s3, \
    get_catalog_info, base_stac_catalog, build_catalog_from_s3, write_catalog_to_s3

MUX_083_RESPONSE = {'ResponseMetadata': {'RequestId': '28742BED38FC3852', 'HostId': 'ikYxQ4guhPSng8OfEXOQ7CfTudw9xZwWvSJBR59KUYni4SXaWxNp7Ov+m4pWsBUThU64vE/vhzU=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'ikYxQ4guhPSng8OfEXOQ7CfTudw9xZwWvSJBR59KUYni4SXaWxNp7Ov+m4pWsBUThU64vE/vhzU=', 'x-amz-request-id': '28742BED38FC3852', 'date': 'Wed, 12 Sep 2018 00:12:15 GMT', 'x-amz-bucket-region': 'us-east-1', 'content-type': 'application/xml', 'transfer-encoding': 'chunked', 'server': 'AmazonS3'}, 'RetryAttempts': 0}, 'IsTruncated': False, 'Contents': [{'Key': 'CBERS4/MUX/083/catalog.json', 'LastModified': datetime.datetime(2018, 9, 7, 23, 17, 19, tzinfo=tzutc()), 'ETag': '"62df51d8fadf3c707d6acb4e11cd0ddf"', 'Size': 2205, 'StorageClass': 'STANDARD'}], 'Name': 'cbers-stac', 'Prefix': 'CBERS4/MUX/083/', 'Delimiter': '/', 'MaxKeys': 1000, 'CommonPrefixes': [{'Prefix': 'CBERS4/MUX/083/083/'}, {'Prefix': 'CBERS4/MUX/083/084/'}, {'Prefix': 'CBERS4/MUX/083/085/'}, {'Prefix': 'CBERS4/MUX/083/086/'}, {'Prefix': 'CBERS4/MUX/083/087/'}, {'Prefix': 'CBERS4/MUX/083/088/'}, {'Prefix': 'CBERS4/MUX/083/089/'}, {'Prefix': 'CBERS4/MUX/083/090/'}, {'Prefix': 'CBERS4/MUX/083/091/'}, {'Prefix': 'CBERS4/MUX/083/092/'}, {'Prefix': 'CBERS4/MUX/083/093/'}, {'Prefix': 'CBERS4/MUX/083/094/'}, {'Prefix': 'CBERS4/MUX/083/095/'}, {'Prefix': 'CBERS4/MUX/083/096/'}, {'Prefix': 'CBERS4/MUX/083/097/'}, {'Prefix': 'CBERS4/MUX/083/098/'}, {'Prefix': 'CBERS4/MUX/083/099/'}, {'Prefix': 'CBERS4/MUX/083/100/'}, {'Prefix': 'CBERS4/MUX/083/101/'}, {'Prefix': 'CBERS4/MUX/083/102/'}, {'Prefix': 'CBERS4/MUX/083/103/'}, {'Prefix': 'CBERS4/MUX/083/104/'}, {'Prefix': 'CBERS4/MUX/083/105/'}, {'Prefix': 'CBERS4/MUX/083/106/'}, {'Prefix': 'CBERS4/MUX/083/107/'}, {'Prefix': 'CBERS4/MUX/083/108/'}, {'Prefix': 'CBERS4/MUX/083/109/'}, {'Prefix': 'CBERS4/MUX/083/110/'}, {'Prefix': 'CBERS4/MUX/083/111/'}], 'KeyCount': 30}

MUX_083_095_RESPONSE = {'ResponseMetadata': {'RequestId': 'D9B03883E4648843', 'HostId': 'BlxldtuteBWlCeNg46mQIlLeEHTOAr1gemxxgXjKVqa0TUKpk3bggDFxPnyIqAyCcCmPSxK1LwQ=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'BlxldtuteBWlCeNg46mQIlLeEHTOAr1gemxxgXjKVqa0TUKpk3bggDFxPnyIqAyCcCmPSxK1LwQ=', 'x-amz-request-id': 'D9B03883E4648843', 'date': 'Wed, 12 Sep 2018 00:12:15 GMT', 'x-amz-bucket-region': 'us-east-1', 'content-type': 'application/xml', 'transfer-encoding': 'chunked', 'server': 'AmazonS3'}, 'RetryAttempts': 0}, 'IsTruncated': False, 'Contents': [{'Key': 'CBERS4/MUX/083/095/CBERS_4_MUX_20170714_083_095_L4.json', 'LastModified': datetime.datetime(2018, 7, 15, 13, 52, 29, tzinfo=tzutc()), 'ETag': '"023dc703c8ef1db42574f5391eccaa63"', 'Size': 2681, 'StorageClass': 'STANDARD'}, {'Key': 'CBERS4/MUX/083/095/CBERS_4_MUX_20180903_083_095_L2.json', 'LastModified': datetime.datetime(2018, 9, 5, 5, 50, 40, tzinfo=tzutc()), 'ETag': '"f7c77dc176c30062386f2766728a0106"', 'Size': 2679, 'StorageClass': 'STANDARD'}, {'Key': 'CBERS4/MUX/083/095/catalog.json', 'LastModified': datetime.datetime(2018, 9, 7, 23, 16, 19, tzinfo=tzutc()), 'ETag': '"4d4960a9db81e5656bd5e67326d75e67"', 'Size': 419, 'StorageClass': 'STANDARD'}], 'Name': 'cbers-stac', 'Prefix': 'CBERS4/MUX/083/095/', 'Delimiter': '/', 'MaxKeys': 1000, 'KeyCount': 3}

class UpdateCatalogTreeTest(unittest.TestCase):
    """UpdateCatalogTreeTest"""

    @classmethod
    def setUpClass(cls):
        shutil.copy2('sam/process_new_scene_queue/utils.py',
                     'sam/update_catalog_tree/utils.py')
        json_schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        'json_schema/')
        schema_path = os.path.join(json_schema_path,
                                   'catalog.json')
        cls.resolver_ = RefResolver('file://' + json_schema_path + '/',
                                    None)
        with open(schema_path) as fp_schema:
            cls.schema_ = json.load(fp_schema)

    def get_items_from_s3_test(self):
        """get_items_from_s3_test"""

        items = get_items_from_s3(bucket=None, prefix=None, response=MUX_083_095_RESPONSE)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0], {'rel':'item',
                                    'href':'CBERS_4_MUX_20170714_083_095_L4.json'})
        self.assertEqual(items[1], {'rel':'item',
                                    'href':'CBERS_4_MUX_20180903_083_095_L2.json'})

    def get_catalogs_from_s3_test(self):
        """get_catalogs_from_s3_test"""

        items = get_catalogs_from_s3(bucket=None, prefix=None, response=MUX_083_RESPONSE)
        self.assertEqual(len(items), 29)
        self.assertEqual(items[0], {'rel':'child',
                                    'href':'083/catalog.json'})
        self.assertEqual(items[1], {'rel':'child',
                                    'href':'084/catalog.json'})
        self.assertEqual(items[28], {'rel':'child',
                                     'href':'111/catalog.json'})

    def get_catalog_info_test(self):
        """get_catalog_info_test"""

        level = get_catalog_info('CBERS4/AWFI/150/129')
        self.assertEqual(level, {'level':0,
                                 'satellite+mission':'CBERS4',
                                 'camera':'AWFI',
                                 'path':'150',
                                 'row':'129'})

        level = get_catalog_info('CBERS4/AWFI/150')
        self.assertEqual(level, {'level':1,
                                 'satellite+mission':'CBERS4',
                                 'camera':'AWFI',
                                 'path':'150',
                                 'row':None})

        level = get_catalog_info('CBERS4/AWFI')
        self.assertEqual(level, {'level':2,
                                 'satellite+mission':'CBERS4',
                                 'camera':'AWFI',
                                 'path':None,
                                 'row':None})

    def build_catalog_from_s3_test(self):
        """build_catalog_from_s3_test"""

        catalog = build_catalog_from_s3(bucket='cbers-stac',
                                        prefix='CBERS4/MUX/083/095',
                                        response=MUX_083_095_RESPONSE)
        self.assertEqual(catalog['id'], 'CBERS4 MUX 083/095')
        self.assertEqual(catalog['description'],
                         'CBERS4 MUX camera path 083 row 095 catalog')
        self.assertEqual(len(catalog['links']), 5)
        self.assertEqual(catalog['links'][0]['rel'], 'self')
        self.assertEqual(catalog['links'][0]['href'],
                         'https://cbers-stac.s3.amazonaws.com/CBERS4/MUX/083/095/catalog.json')
        self.assertEqual(catalog['links'][1]['rel'], 'root')
        self.assertEqual(catalog['links'][1]['href'],
                         'https://cbers-stac.s3.amazonaws.com/catalog.json')
        self.assertEqual(catalog['links'][2]['rel'], 'parent')
        self.assertEqual(catalog['links'][2]['href'], '../catalog.json')
        self.assertEqual(catalog['links'][3]['rel'], 'item')
        self.assertEqual(catalog['links'][3]['href'], 'CBERS_4_MUX_20170714_083_095_L4.json')
        self.assertEqual(catalog['links'][4]['rel'], 'item')
        self.assertEqual(catalog['links'][4]['href'], 'CBERS_4_MUX_20180903_083_095_L2.json')
        self.assertEqual(validate(catalog, self.schema_, resolver=self.resolver_),
                         None)

        catalog = build_catalog_from_s3(bucket='cbers-stac',
                                        prefix='CBERS4/MUX/083',
                                        response=MUX_083_RESPONSE)
        self.assertEqual(catalog['id'], 'CBERS4 MUX 083')
        self.assertEqual(catalog['description'],
                         'CBERS4 MUX camera path 083 catalog')
        self.assertEqual(len(catalog['links']), 32)
        self.assertEqual(catalog['links'][0]['rel'], 'self')
        self.assertEqual(catalog['links'][0]['href'],
                         'https://cbers-stac.s3.amazonaws.com/CBERS4/MUX/083/catalog.json')
        self.assertEqual(catalog['links'][1]['rel'], 'root')
        self.assertEqual(catalog['links'][1]['href'],
                         'https://cbers-stac.s3.amazonaws.com/catalog.json')
        self.assertEqual(catalog['links'][2]['rel'], 'parent')
        self.assertEqual(catalog['links'][2]['href'], '../catalog.json')
        self.assertEqual(catalog['links'][3]['rel'], 'child')
        self.assertEqual(catalog['links'][3]['href'], '083/catalog.json')
        self.assertEqual(catalog['links'][-1]['rel'], 'child')
        self.assertEqual(catalog['links'][-1]['href'], '111/catalog.json')
        self.assertEqual(validate(catalog, self.schema_, resolver=self.resolver_),
                         None)

    def base_stac_catalog_test(self):
        """base_stac_catalog_test"""

        catalog = base_stac_catalog('cbers-stac', 'CBERS', '4', 'AWFI', '130', '100')
        self.assertEqual(catalog['id'], 'CBERS4 AWFI 130/100')
        self.assertEqual(catalog['description'],
                         'CBERS4 AWFI camera path 130 row 100 catalog')
        self.assertEqual(catalog['links'][0]['rel'], 'self')
        self.assertEqual(catalog['links'][0]['href'],
                         'https://cbers-stac.s3.amazonaws.com/CBERS4/AWFI/130/100/catalog.json')
        self.assertEqual(catalog['links'][1]['rel'], 'root')
        self.assertEqual(catalog['links'][1]['href'],
                         'https://cbers-stac.s3.amazonaws.com/catalog.json')
        self.assertEqual(catalog['links'][2]['rel'], 'parent')
        self.assertEqual(catalog['links'][2]['href'], '../catalog.json')
        self.assertEqual(validate(catalog, self.schema_, resolver=self.resolver_),
                         None)

        catalog = base_stac_catalog('cbers-stac', 'CBERS', '4', 'AWFI', '130')
        self.assertEqual(catalog['id'], 'CBERS4 AWFI 130')
        self.assertEqual(catalog['description'],
                         'CBERS4 AWFI camera path 130 catalog')
        self.assertEqual(catalog['links'][0]['rel'], 'self')
        self.assertEqual(catalog['links'][0]['href'],
                         'https://cbers-stac.s3.amazonaws.com/CBERS4/AWFI/130/catalog.json')
        self.assertEqual(catalog['links'][1]['rel'], 'root')
        self.assertEqual(catalog['links'][1]['href'],
                         'https://cbers-stac.s3.amazonaws.com/catalog.json')
        self.assertEqual(catalog['links'][2]['rel'], 'parent')
        self.assertEqual(catalog['links'][2]['href'], '../catalog.json')
        self.assertEqual(validate(catalog, self.schema_, resolver=self.resolver_),
                         None)

        catalog = base_stac_catalog('cbers-stac', 'CBERS', '4', 'AWFI')
        self.assertEqual(catalog['id'], 'CBERS4 AWFI')
        self.assertEqual(catalog['description'],
                         'CBERS4 AWFI camera catalog')
        self.assertEqual(catalog['links'][0]['rel'], 'self')
        self.assertEqual(catalog['links'][0]['href'],
                         'https://cbers-stac.s3.amazonaws.com/CBERS4/AWFI/catalog.json')
        self.assertEqual(catalog['links'][1]['rel'], 'root')
        self.assertEqual(catalog['links'][1]['href'],
                         'https://cbers-stac.s3.amazonaws.com/catalog.json')
        self.assertEqual(catalog['links'][2]['rel'], 'parent')
        self.assertEqual(catalog['links'][2]['href'], '../catalog.json')
        self.assertEqual(validate(catalog, self.schema_, resolver=self.resolver_),
                         None)

        catalog = base_stac_catalog('cbers-stac', 'CBERS', '4')
        self.assertEqual(catalog['id'], 'CBERS4')
        self.assertEqual(catalog['description'],
                         'CBERS4 catalog')
        self.assertEqual(catalog['links'][0]['rel'], 'self')
        self.assertEqual(catalog['links'][0]['href'],
                         'https://cbers-stac.s3.amazonaws.com/CBERS4/catalog.json')
        self.assertEqual(catalog['links'][1]['rel'], 'root')
        self.assertEqual(catalog['links'][1]['href'],
                         'https://cbers-stac.s3.amazonaws.com/catalog.json')
        self.assertEqual(catalog['links'][2]['rel'], 'parent')
        self.assertEqual(catalog['links'][2]['href'], '../catalog.json')
        self.assertEqual(validate(catalog, self.schema_, resolver=self.resolver_),
                         None)

        catalog = base_stac_catalog('cbers-stac', 'CBERS')
        self.assertEqual(catalog['id'], 'CBERS')
        self.assertEqual(catalog['description'],
                         'CBERS catalog')
        self.assertEqual(len(catalog['links']), 2)
        self.assertEqual(catalog['links'][0]['rel'], 'self')
        self.assertEqual(catalog['links'][0]['href'],
                         'https://cbers-stac.s3.amazonaws.com/catalog.json')
        self.assertEqual(catalog['links'][1]['rel'], 'root')
        self.assertEqual(catalog['links'][1]['href'],
                         'https://cbers-stac.s3.amazonaws.com/catalog.json')
        self.assertEqual(validate(catalog, self.schema_, resolver=self.resolver_),
                         None)

    def root_catalog_schema_test(self):
        """root_catalog_schema_test"""

        with open('stac_catalogs/catalog.json', 'r') as catalog_file:
            catalog = json.loads(catalog_file.read())
        self.assertEqual(validate(catalog, self.schema_, resolver=self.resolver_),
                         None)

        with open('stac_catalogs/CBERS4/catalog.json', 'r') as catalog_file:
            catalog = json.loads(catalog_file.read())
        self.assertEqual(validate(catalog, self.schema_, resolver=self.resolver_),
                         None)

    @unittest.skip("Require AWS credentials and environment")
    def integration_test(self):
        """integration_test"""

        prefix = 'CBERS4/MUX/083/095'
        catalog = build_catalog_from_s3(bucket='cbers-stac',
                                        prefix=prefix)
        self.assertEqual(catalog['name'], 'CBERS4 MUX 083/095')
        write_catalog_to_s3(bucket='cbers-stac', prefix='test/' + prefix,
                            catalog=catalog)

        prefix = 'CBERS4/MUX/083'
        catalog = build_catalog_from_s3(bucket='cbers-stac',
                                        prefix=prefix)
        write_catalog_to_s3(bucket='cbers-stac', prefix='test/' + prefix,
                            catalog=catalog)

        prefix = 'CBERS4/MUX'
        catalog = build_catalog_from_s3(bucket='cbers-stac',
                                        prefix=prefix)
        write_catalog_to_s3(bucket='cbers-stac', prefix='test/' + prefix,
                            catalog=catalog)

if __name__ == '__main__':
    unittest.main()
