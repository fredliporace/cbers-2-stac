"""collection_test"""

import json
import pathlib
from test.stac_validator import STACValidator

import pytest
from jsonschema.exceptions import ValidationError

from cbers2stac.update_catalog_tree.code import base_stac_catalog


def test_collection_json_schema():
    """test_collection_json_schema"""

    jsv = STACValidator(schema_filename="collection.json")

    # Makes sure that a invalid file is flagged
    collection_filename = "test/CBERS_4_MUX_bogus_collection.json"
    with pytest.raises(ValidationError) as context:
        jsv.validate(collection_filename)
    assert "'stac_version' is a required property" in context.value.message

    # Checks all CBERS-4 collections
    pathlib.Path("test/output").mkdir(parents=True, exist_ok=True)
    collections = ["MUX", "AWFI", "PAN5M", "PAN10M"]
    for collection in collections:
        col_dict = base_stac_catalog("cbers-stac", "CBERS", "4", collection)
        assert col_dict["id"] == "CBERS4-" + collection
        collection_filename = f"test/output/{collection}_collection.json"
        with open(collection_filename, "w", encoding="utf-8") as out_filename:
            json.dump(col_dict, out_filename, indent=2)
        jsv.validate(collection_filename)

    # Checks all CBERS-4A collections
    collections = ["MUX", "WFI", "WPM"]
    for collection in collections:
        col_dict = base_stac_catalog("cbers-stac", "CBERS", "4A", collection)
        assert col_dict["id"] == "CBERS4A-" + collection
        collection_filename = f"test/output/{collection}_collection.json"
        with open(collection_filename, "w", encoding="utf-8") as out_filename:
            json.dump(col_dict, out_filename, indent=2)
        jsv.validate(collection_filename)
