"""
Create local catalog and collection initial structure
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple

from jsonschema import RefResolver, validate

from cbers2stac.layers.common.utils import (
    get_collections_for_satmission,
    get_satmissions,
)
from cbers2stac.update_catalog_tree.code import base_root_catalog, base_stac_catalog


def create_resolver_schema(entity: str) -> Tuple[RefResolver, Dict]:
    """
    Create resolver and schema from json spec, catalog or collection
    """
    json_schema_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        f"../../test/json_schema/{entity}-spec/json-schema",
    )
    schema_path = os.path.join(json_schema_path, f"{entity}.json")
    resolver = RefResolver("file://" + json_schema_path + "/", None)

    # Loads schema
    with open(schema_path, encoding="utf-8") as fp_schema:
        schema = json.load(fp_schema)
    return resolver, schema


def update_catalog_or_collection(c_dict: Dict[Any, Any], filename: str) -> bool:
    """
    Update a catalog or collection if the structure differs
    from the existing file.

    Return True if the file was updated or created.
    """

    if os.path.exists(filename):
        # Check if same
        with open(filename, "r", encoding="utf-8") as cfile:
            current_dict = json.load(cfile)
            if current_dict == c_dict:
                return False
    else:
        # File does not exist, create dir
        Path(os.path.dirname(filename)).mkdir(parents=True, exist_ok=True)
    with open(filename, "w", encoding="utf-8") as out_filename:
        json.dump(c_dict, out_filename, indent=2)
    return True


def create_local_catalog_structure(root_directory: str, bucket_name: str) -> None:
    """
    Create a local, empty catalog structure

    root_directory: the root of the local structure
    bucket_name: used for self links
    """

    col_resolver, col_schema = create_resolver_schema("collection")
    cat_resolver, cat_schema = create_resolver_schema("catalog")

    # The root catalog for all satellites and missions
    cat_dict = base_root_catalog(bucket_name)
    assert cat_dict["id"] == "CBERS-AMAZONIA STATIC ROOT", cat_dict["id"]
    catalog_filename = f"{root_directory}/catalog.json"
    update_catalog_or_collection(c_dict=cat_dict, filename=catalog_filename)
    with open(catalog_filename, encoding="utf-8") as fp_in:
        validate(json.load(fp_in), cat_schema, resolver=cat_resolver)

    # Create catalogs and collections
    for satmission in get_satmissions(use_hyphen=True):
        satellite = satmission.split("-")[0]
        mission = satmission.split("-")[1]
        # Create catalog
        cat_dict = base_stac_catalog(bucket_name, satellite, mission)
        assert cat_dict["id"] == f"{satellite}{mission}"
        catalog_filename = f"{root_directory}/{satellite}{mission}/catalog.json"
        update_catalog_or_collection(c_dict=cat_dict, filename=catalog_filename)
        with open(catalog_filename, encoding="utf-8") as fp_in:
            validate(json.load(fp_in), cat_schema, resolver=cat_resolver)
        for collection in get_collections_for_satmission(satellite, mission):
            # Create collections
            col_dict = base_stac_catalog(bucket_name, satellite, mission, collection)
            assert col_dict["id"] == f"{satellite}{mission}-" + collection
            collection_filename = (
                f"{root_directory}/{satellite}{mission}/{collection}" "/collection.json"
            )
            update_catalog_or_collection(c_dict=col_dict, filename=collection_filename)
            with open(collection_filename, encoding="utf-8") as fp_in:
                validate(json.load(fp_in), col_schema, resolver=col_resolver)
