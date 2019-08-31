"""collection_test"""

import os
import unittest
import json

from jsonschema import validate, RefResolver
from jsonschema.exceptions import ValidationError

class CollectionTest(unittest.TestCase):
    """CollectionTest"""

    def test_collection_json_schema(self):
        """test_collection_json_schema"""

        json_schema_path = os.path.join(os.path.dirname(os.path.\
                                                        abspath(__file__)),
                                        'json_schema/')
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

        # Checks all collections
        collections = ['MUX', 'AWFI', 'PAN5M', 'PAN10M']
        for collection in collections:
            collection_filename = 'stac_catalogs/CBERS4/{col}/' \
                                  'collection.json'.format(col=collection)
            with open(collection_filename) as fp_in:
                validate(json.load(fp_in), schema, resolver=resolver)

if __name__ == '__main__':
    unittest.main()
