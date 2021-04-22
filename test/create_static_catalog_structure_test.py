"""create_static_catalog_structure_test"""

# This is generated by pytest fixtures
# pylint: disable=redefined-outer-name

import json
import os
import shutil
from dataclasses import dataclass
from typing import Dict

import pytest
from jsonschema import RefResolver, validate

from cbers2stac.local.create_static_catalog_structure import (
    create_local_catalog_structure,
    update_catalog_or_collection,
)
from cbers2stac.update_catalog_tree.code import base_root_catalog


@dataclass
class Config:
    """
    Module test helpers
    """

    cat_resolver: RefResolver
    cat_schema: Dict
    col_resolver: RefResolver
    col_schema: Dict


@pytest.fixture(scope="module")
def setup():
    """
    Setup for test module
    """

    params = dict()
    json_schema_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "json_schema/catalog-spec/json-schema",
    )
    # Catalog schema validator
    schema_path = os.path.join(json_schema_path, "catalog.json")
    params["cat_resolver"] = RefResolver("file://" + json_schema_path + "/", None)
    with open(schema_path) as fp_schema:
        params["cat_schema"] = json.load(fp_schema)

    # Collection schema validator
    json_schema_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "json_schema/collection-spec/json-schema",
    )
    schema_path = os.path.join(json_schema_path, "collection.json")
    params["col_resolver"] = RefResolver("file://" + json_schema_path + "/", None)
    with open(schema_path) as fp_schema:
        params["col_schema"] = json.load(fp_schema)

    return Config(**params)


def test_update_catalog_or_collection():
    """
    test_update_catalog_or_collection
    """
    bucket_name = "bucket_name"
    prefix = "test/fixtures/test_update_catalog_or_collection"
    cat_dict = base_root_catalog(bucket_name)
    catalog_filename = f"{prefix}/catalog.json"

    # Remove test directory
    shutil.rmtree(prefix, ignore_errors=True)

    # First call, new catalog must return True
    ret = update_catalog_or_collection(c_dict=cat_dict, filename=catalog_filename)
    assert ret

    # File already generated, must return False
    ret = update_catalog_or_collection(c_dict=cat_dict, filename=catalog_filename)
    assert not ret

    # Update dict, shold be updated
    cat_dict = base_root_catalog(bucket_name + "new")
    ret = update_catalog_or_collection(c_dict=cat_dict, filename=catalog_filename)
    assert ret


def test_create(setup):
    """test_create"""

    prefix = "test/fixtures/catalog_structure"
    shutil.rmtree(prefix, ignore_errors=True)
    create_local_catalog_structure(root_directory=f"{prefix}", bucket_name="cbers-stac")

    catalog_files = [
        f"{prefix}/catalog.json",
        f"{prefix}/CBERS4/catalog.json",
        f"{prefix}/CBERS4A/catalog.json",
    ]
    catalog_files = list()
    for c_file in catalog_files:
        with open(c_file, "r") as catalog_file:
            catalog = json.loads(catalog_file.read())
            assert (
                validate(catalog, setup.cat_schema, resolver=setup.cat_resolver) is None
            )

    collection_files = [
        f"{prefix}/CBERS4/AWFI/collection.json",
        f"{prefix}/CBERS4/MUX/collection.json",
        f"{prefix}/CBERS4/PAN10M/collection.json",
        f"{prefix}/CBERS4/PAN5M/collection.json",
        f"{prefix}/CBERS4A/MUX/collection.json",
        f"{prefix}/CBERS4A/WFI/collection.json",
        f"{prefix}/CBERS4A/WPM/collection.json",
    ]
    for c_file in collection_files:
        with open(c_file, "r") as collection_file:
            collection = json.loads(collection_file.read())
            assert (
                validate(collection, setup.col_schema, resolver=setup.col_resolver)
                is None
            )