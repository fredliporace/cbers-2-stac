"""collection_test"""

import json
from test.stac_validator import STACValidator

import pytest
from jsonschema.exceptions import ValidationError

from cbers2stac.update_catalog_tree.code import base_stac_catalog


def test_collection_json_schema(tmp_path):
    """test_collection_json_schema"""

    jsv = STACValidator(schema_filename="collection.json")

    # Makes sure that a invalid file is flagged
    collection_filename = "test/fixtures/CBERS_4_MUX_bogus_collection.json"
    with pytest.raises(ValidationError) as context:
        jsv.validate(collection_filename)
    assert "'stac_version' is a required property" in context.value.message

    # Checks all Amazonia-1 collections
    collections = ["WFI"]
    for collection in collections:
        col_dict = base_stac_catalog(
            bucket="amazonia-stac", satellite="AMAZONIA", mission="1", camera=collection
        )
        assert col_dict["id"] == "AMAZONIA1-" + collection
        collection_filename = f"{str(tmp_path)}/{collection}_collection.json"
        with open(collection_filename, "w", encoding="utf-8") as out_filename:
            json.dump(col_dict, out_filename, indent=2)
        jsv.validate(collection_filename)

    # Checks all CBERS-4 collections
    collections = ["MUX", "AWFI", "PAN5M", "PAN10M"]
    for collection in collections:
        col_dict = base_stac_catalog("cbers-stac", "CBERS", "4", collection)
        assert col_dict["id"] == "CBERS4-" + collection
        collection_filename = f"{str(tmp_path)}/{collection}_collection.json"
        with open(collection_filename, "w", encoding="utf-8") as out_filename:
            json.dump(col_dict, out_filename, indent=2)
        jsv.validate(collection_filename)

    # Checks all CBERS-4A collections
    collections = ["MUX", "WFI", "WPM"]
    for collection in collections:
        col_dict = base_stac_catalog("cbers-stac", "CBERS", "4A", collection)
        assert col_dict["id"] == "CBERS4A-" + collection
        collection_filename = f"{str(tmp_path)}/{collection}_collection.json"
        with open(collection_filename, "w", encoding="utf-8") as out_filename:
            json.dump(col_dict, out_filename, indent=2)
        jsv.validate(collection_filename)
