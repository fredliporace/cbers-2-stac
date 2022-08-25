"""create_static_catalog_structure_test"""

from test.stac_validator import STACValidator

from cbers2stac.local.create_static_catalog_structure import (
    create_local_catalog_structure,
    update_catalog_or_collection,
)
from cbers2stac.update_catalog_tree.code import base_root_catalog


def test_update_catalog_or_collection(tmp_path):
    """
    test_update_catalog_or_collection
    """
    bucket_name = "bucket_name"
    cat_dict = base_root_catalog(bucket_name)
    catalog_filename = f"{str(tmp_path)}/catalog.json"

    # First call, new catalog must return True
    ret = update_catalog_or_collection(c_dict=cat_dict, filename=catalog_filename)
    assert ret

    # File already generated, must return False
    ret = update_catalog_or_collection(c_dict=cat_dict, filename=catalog_filename)
    assert not ret

    # Update dict, should be updated
    cat_dict = base_root_catalog(bucket_name + "new")
    ret = update_catalog_or_collection(c_dict=cat_dict, filename=catalog_filename)
    assert ret


def test_create(tmp_path):
    """test_create"""

    catv = STACValidator(schema_filename="catalog.json")
    colv = STACValidator(schema_filename="collection.json")

    prefix = str(tmp_path)
    create_local_catalog_structure(root_directory=f"{prefix}", bucket_name="cbers-stac")

    catalog_files = [
        f"{prefix}/catalog.json",
        f"{prefix}/CBERS4/catalog.json",
        f"{prefix}/CBERS4A/catalog.json",
        f"{prefix}/AMAZONIA1/catalog.json",
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
        f"{prefix}/AMAZONIA1/WFI/collection.json",
    ]
    for c_file in collection_files:
        colv.validate(c_file)
