"""create_static_catalog_structure_test"""

import shutil
from test.stac_validator import STACValidator

from cbers2stac.local.create_static_catalog_structure import (
    create_local_catalog_structure,
    update_catalog_or_collection,
)
from cbers2stac.update_catalog_tree.code import base_root_catalog


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


def test_create():
    """test_create"""

    catv = STACValidator(schema_filename="catalog.json")
    colv = STACValidator(schema_filename="collection.json")

    prefix = "test/fixtures/catalog_structure"
    shutil.rmtree(prefix, ignore_errors=True)
    create_local_catalog_structure(root_directory=f"{prefix}", bucket_name="cbers-stac")

    catalog_files = [
        f"{prefix}/catalog.json",
        f"{prefix}/CBERS4/catalog.json",
        f"{prefix}/CBERS4A/catalog.json",
    ]
    catalog_files = []
    for c_file in catalog_files:
        catv.validate(c_file)

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
        colv.validate(c_file)
