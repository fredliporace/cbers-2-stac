"""collection_test"""

import json
import os

import pytest
from jsonschema import RefResolver, validate
from jsonschema.exceptions import ValidationError
from pystac.validation import validate_dict

from sam.update_catalog_tree.code import base_stac_catalog

# Region is required for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

# @todo change debug output to give more information when
# the validation fails
def validate_json(filename):
    """
    Validate STAC item using PySTAC
    """
    with open(filename) as fname:
        jsd = json.load(fname)
    validate_dict(jsd)


def test_collection_json_schema():
    """test_collection_json_schema"""

    json_schema_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "json_schema/collection-spec/json-schema",
    )
    schema_path = os.path.join(json_schema_path, "collection.json")
    resolver = RefResolver("file://" + json_schema_path + "/", None)

    # loads schema
    with open(schema_path) as fp_schema:
        schema = json.load(fp_schema)

    # Makes sure that a invalid file is flagged
    collection_filename = "test/CBERS_4_MUX_bogus_collection.json"
    with open(collection_filename) as fp_in:
        with pytest.raises(ValidationError) as context:
            validate(json.load(fp_in), schema, resolver=resolver)
        assert "'stac_version' is a required property" in context.value.message

    # Checks all CBERS-4 collections
    collections = ["MUX", "AWFI", "PAN5M", "PAN10M"]
    for collection in collections:
        col_dict = base_stac_catalog("cbers-stac", "CBERS", "4", collection)
        assert col_dict["id"] == "CBERS4-" + collection
        collection_filename = "stac_catalogs/CBERS4/{col}/" "collection.json".format(
            col=collection
        )
        with open(collection_filename, "w") as out_filename:
            json.dump(col_dict, out_filename, indent=2)
        validate_json(collection_filename)
        with open(collection_filename) as fp_in:
            validate(json.load(fp_in), schema, resolver=resolver)

    # Checks all CBERS-4A collections
    collections = ["MUX", "WFI", "WPM"]
    for collection in collections:
        col_dict = base_stac_catalog("cbers-stac", "CBERS", "4A", collection)
        assert col_dict["id"] == "CBERS4A-" + collection
        collection_filename = "stac_catalogs/CBERS4A/{col}/" "collection.json".format(
            col=collection
        )
        with open(collection_filename, "w") as out_filename:
            json.dump(col_dict, out_filename, indent=2)
        validate_json(collection_filename)
        with open(collection_filename) as fp_in:
            validate(json.load(fp_in), schema, resolver=resolver)
