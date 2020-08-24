"""collection_test"""

import os
import unittest
import json

from jsonschema import validate, RefResolver
from jsonschema.exceptions import ValidationError

from pystac.validation import validate_dict

# Region is required for testing
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

import site
site.addsitedir('sam/process_new_scene_queue')
site.addsitedir('sam/update_catalog_tree')

from sam.update_catalog_tree.code import base_stac_catalog

# @todo change debug output to give more information when
# the validation fails
def validate_json(filename):
    """
    Validate STAC item using PySTAC
    """
    with open(filename) as fname:
        jsd = json.load(fname)
    validate_dict(jsd)

class CollectionTest(unittest.TestCase):
    """CollectionTest"""

    def test_collection_json_schema(self):
        """test_collection_json_schema"""

        json_schema_path = os.path.join(os.path.dirname(os.path.\
                                                        abspath(__file__)),
                                        'json_schema/collection-spec/json-schema')
        schema_path = os.path.join(json_schema_path,
                                   'collection.json')
        resolver = RefResolver('file://' + json_schema_path + '/',
                               None)

        # loads schema
        with open(schema_path) as fp_schema:
            schema = json.load(fp_schema)

        # Makes sure that a invalid file is flagged
        collection_filename = 'test/CBERS_4_MUX_bogus_collection.json'
        with open(collection_filename) as fp_in:
            with self.assertRaises(ValidationError) as context:
                validate(json.load(fp_in), schema, resolver=resolver)
            self.assertTrue("'stac_version' is a required property" \
                            in str(context.exception))

        # Checks all CBERS-4 collections
        collections = ['MUX', 'AWFI', 'PAN5M', 'PAN10M']
        for collection in collections:
            col_dict = base_stac_catalog('cbers-stac', 'CBERS', '4', collection)
            collection_filename = 'stac_catalogs/CBERS4/{col}/' \
                                  'collection.json'.format(col=collection)
            with open(collection_filename, 'w') as out_filename:
                json.dump(col_dict, out_filename, indent=2)
            validate_json(collection_filename)
            with open(collection_filename) as fp_in:
                validate(json.load(fp_in), schema, resolver=resolver)

        # Checks all CBERS-4A collections
        collections = ['MUX', 'WFI', 'WPM']
        for collection in collections:
            col_dict = base_stac_catalog('cbers-stac', 'CBERS', '4A', collection)
            collection_filename = 'stac_catalogs/CBERS4A/{col}/' \
                                  'collection.json'.format(col=collection)
            with open(collection_filename, 'w') as out_filename:
                json.dump(col_dict, out_filename, indent=2)
            validate_json(collection_filename)
            with open(collection_filename) as fp_in:
                validate(json.load(fp_in), schema, resolver=resolver)

if __name__ == '__main__':
    unittest.main()
