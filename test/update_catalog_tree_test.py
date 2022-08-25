"""update_catalog_tree_test"""

# This is generated by pytest fixtures
# pylint: disable=redefined-outer-name

import datetime
from test.stac_validator import STACValidator

import pytest
from dateutil.tz import tzutc

from cbers2stac.layers.common.utils import STAC_VERSION
from cbers2stac.update_catalog_tree.code import (
    base_stac_catalog,
    build_catalog_from_s3,
    get_base_collection,
    get_catalog_info,
    get_catalogs_from_s3,
    get_items_from_s3,
    write_catalog_to_s3,
)

MUX_083_RESPONSE = {
    "ResponseMetadata": {
        "RequestId": "28742BED38FC3852",
        "HostId": "ikYxQ4guhPSng8OfEXOQ7CfTudw9xZwWvSJBR59KUYni4SXaWxNp7Ov+m4pWsBUThU64vE/vhzU=",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amz-id-2": "ikYxQ4guhPSng8OfEXOQ7CfTudw9xZwWvSJBR"
            "59KUYni4SXaWxNp7Ov+m4pWsBUThU64vE/vhzU=",
            "x-amz-request-id": "28742BED38FC3852",
            "date": "Wed, 12 Sep 2018 00:12:15 GMT",
            "x-amz-bucket-region": "us-east-1",
            "content-type": "application/xml",
            "transfer-encoding": "chunked",
            "server": "AmazonS3",
        },
        "RetryAttempts": 0,
    },
    "IsTruncated": False,
    "Contents": [
        {
            "Key": "CBERS4/MUX/083/catalog.json",
            "LastModified": datetime.datetime(2018, 9, 7, 23, 17, 19, tzinfo=tzutc()),
            "ETag": '"62df51d8fadf3c707d6acb4e11cd0ddf"',
            "Size": 2205,
            "StorageClass": "STANDARD",
        }
    ],
    "Name": "cbers-stac",
    "Prefix": "CBERS4/MUX/083/",
    "Delimiter": "/",
    "MaxKeys": 1000,
    "CommonPrefixes": [
        {"Prefix": "CBERS4/MUX/083/083/"},
        {"Prefix": "CBERS4/MUX/083/084/"},
        {"Prefix": "CBERS4/MUX/083/085/"},
        {"Prefix": "CBERS4/MUX/083/086/"},
        {"Prefix": "CBERS4/MUX/083/087/"},
        {"Prefix": "CBERS4/MUX/083/088/"},
        {"Prefix": "CBERS4/MUX/083/089/"},
        {"Prefix": "CBERS4/MUX/083/090/"},
        {"Prefix": "CBERS4/MUX/083/091/"},
        {"Prefix": "CBERS4/MUX/083/092/"},
        {"Prefix": "CBERS4/MUX/083/093/"},
        {"Prefix": "CBERS4/MUX/083/094/"},
        {"Prefix": "CBERS4/MUX/083/095/"},
        {"Prefix": "CBERS4/MUX/083/096/"},
        {"Prefix": "CBERS4/MUX/083/097/"},
        {"Prefix": "CBERS4/MUX/083/098/"},
        {"Prefix": "CBERS4/MUX/083/099/"},
        {"Prefix": "CBERS4/MUX/083/100/"},
        {"Prefix": "CBERS4/MUX/083/101/"},
        {"Prefix": "CBERS4/MUX/083/102/"},
        {"Prefix": "CBERS4/MUX/083/103/"},
        {"Prefix": "CBERS4/MUX/083/104/"},
        {"Prefix": "CBERS4/MUX/083/105/"},
        {"Prefix": "CBERS4/MUX/083/106/"},
        {"Prefix": "CBERS4/MUX/083/107/"},
        {"Prefix": "CBERS4/MUX/083/108/"},
        {"Prefix": "CBERS4/MUX/083/109/"},
        {"Prefix": "CBERS4/MUX/083/110/"},
        {"Prefix": "CBERS4/MUX/083/111/"},
    ],
    "KeyCount": 30,
}

MUX_083_095_RESPONSE = {
    "ResponseMetadata": {
        "RequestId": "D9B03883E4648843",
        "HostId": "BlxldtuteBWlCeNg46mQIlLeEHTOAr1gemxxgXjKVqa0TUKpk3bggDFxPnyIqAyCcCmPSxK1LwQ=",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amz-id-2": "BlxldtuteBWlCeNg46mQIlLeEHTOAr1g"
            "emxxgXjKVqa0TUKpk3bggDFxPnyIqAyCcCmPSxK1LwQ=",
            "x-amz-request-id": "D9B03883E4648843",
            "date": "Wed, 12 Sep 2018 00:12:15 GMT",
            "x-amz-bucket-region": "us-east-1",
            "content-type": "application/xml",
            "transfer-encoding": "chunked",
            "server": "AmazonS3",
        },
        "RetryAttempts": 0,
    },
    "IsTruncated": False,
    "Contents": [
        {
            "Key": "CBERS4/MUX/083/095/CBERS_4_MUX_20170714_083_095_L4.json",
            "LastModified": datetime.datetime(2018, 7, 15, 13, 52, 29, tzinfo=tzutc()),
            "ETag": '"023dc703c8ef1db42574f5391eccaa63"',
            "Size": 2681,
            "StorageClass": "STANDARD",
        },
        {
            "Key": "CBERS4/MUX/083/095/CBERS_4_MUX_20180903_083_095_L2.json",
            "LastModified": datetime.datetime(2018, 9, 5, 5, 50, 40, tzinfo=tzutc()),
            "ETag": '"f7c77dc176c30062386f2766728a0106"',
            "Size": 2679,
            "StorageClass": "STANDARD",
        },
        {
            "Key": "CBERS4/MUX/083/095/catalog.json",
            "LastModified": datetime.datetime(2018, 9, 7, 23, 16, 19, tzinfo=tzutc()),
            "ETag": '"4d4960a9db81e5656bd5e67326d75e67"',
            "Size": 419,
            "StorageClass": "STANDARD",
        },
    ],
    "Name": "cbers-stac",
    "Prefix": "CBERS4/MUX/083/095/",
    "Delimiter": "/",
    "MaxKeys": 1000,
    "KeyCount": 3,
}


def test_get_items_from_s3():
    """get_items_from_s3_test"""

    items = get_items_from_s3(bucket=None, prefix=None, response=MUX_083_095_RESPONSE)
    assert len(items) == 2
    assert items[0] == {"rel": "item", "href": "CBERS_4_MUX_20170714_083_095_L4.json"}
    assert items[1] == {"rel": "item", "href": "CBERS_4_MUX_20180903_083_095_L2.json"}


def test_get_catalogs_from_s3():
    """get_catalogs_from_s3_test"""

    items = get_catalogs_from_s3(bucket=None, prefix=None, response=MUX_083_RESPONSE)
    assert len(items) == 29
    assert items[0] == {"rel": "child", "href": "083/catalog.json"}
    assert items[1] == {"rel": "child", "href": "084/catalog.json"}
    assert items[28] == {"rel": "child", "href": "111/catalog.json"}


def test_get_catalog_info():
    """get_catalog_info_test"""

    info = get_catalog_info("CBERS4/AWFI/150/129")
    assert info == {
        "level": 0,
        "satellite+mission": "CBERS4",
        "camera": "AWFI",
        "path": "150",
        "row": "129",
        "is_collection": False,
    }

    info = get_catalog_info("CBERS4/AWFI/150")
    assert info == {
        "level": 1,
        "satellite+mission": "CBERS4",
        "camera": "AWFI",
        "path": "150",
        "row": None,
        "is_collection": False,
    }

    info = get_catalog_info("CBERS4/AWFI")
    assert info == {
        "level": 2,
        "satellite+mission": "CBERS4",
        "camera": "AWFI",
        "path": None,
        "row": None,
        "is_collection": True,
    }

    # Amazonia1 check
    info = get_catalog_info("AMAZONIA1/WFI/150/129")
    assert info == {
        "level": 0,
        "satellite+mission": "AMAZONIA1",
        "camera": "WFI",
        "path": "150",
        "row": "129",
        "is_collection": False,
    }

    info = get_catalog_info("AMAZONIA1/WFI/150")
    assert info == {
        "level": 1,
        "satellite+mission": "AMAZONIA1",
        "camera": "WFI",
        "path": "150",
        "row": None,
        "is_collection": False,
    }

    info = get_catalog_info("AMAZONIA1/WFI")
    assert info == {
        "level": 2,
        "satellite+mission": "AMAZONIA1",
        "camera": "WFI",
        "path": None,
        "row": None,
        "is_collection": True,
    }


def test_build_catalog_from_s3():
    """build_catalog_from_s3_test"""

    catv = STACValidator(schema_filename="catalog.json")

    catalog = build_catalog_from_s3(
        bucket="cbers-stac", prefix="CBERS4/MUX/083/095", response=MUX_083_095_RESPONSE
    )
    assert catalog["stac_version"] == STAC_VERSION
    assert catalog["id"] == "CBERS4 MUX 083/095"
    assert catalog["description"] == "CBERS4 MUX camera path 083 row 095 catalog"
    assert len(catalog["links"]) == 5
    assert catalog["links"][0]["rel"] == "self"
    assert (
        catalog["links"][0]["href"] == "https://cbers-stac.s3.amazonaws.com/CBERS4/"
        "MUX/083/095/catalog.json"
    )
    assert catalog["links"][1]["rel"] == "root"
    assert (
        catalog["links"][1]["href"]
        == "https://cbers-stac.s3.amazonaws.com/catalog.json"
    )
    assert catalog["links"][2]["rel"] == "parent"
    assert catalog["links"][2]["href"] == "../catalog.json"
    assert catalog["links"][3]["rel"] == "item"
    assert catalog["links"][3]["href"] == "CBERS_4_MUX_20170714_083_095_L4.json"
    assert catalog["links"][4]["rel"] == "item"
    assert catalog["links"][4]["href"] == "CBERS_4_MUX_20180903_083_095_L2.json"
    catv.validate_dict(catalog)

    catalog = build_catalog_from_s3(
        bucket="cbers-stac", prefix="CBERS4/MUX/083", response=MUX_083_RESPONSE
    )
    assert catalog["stac_version"] == STAC_VERSION
    # validate_dict(catalog)
    assert catalog["id"] == "CBERS4 MUX 083"
    assert catalog["description"] == "CBERS4 MUX camera path 083 catalog"
    assert len(catalog["links"]) == 32
    assert catalog["links"][0]["rel"] == "self"
    assert (
        catalog["links"][0]["href"] == "https://cbers-stac.s3.amazonaws.com/CBERS4/"
        "MUX/083/catalog.json"
    )
    assert catalog["links"][1]["rel"] == "root"
    assert (
        catalog["links"][1]["href"]
        == "https://cbers-stac.s3.amazonaws.com/catalog.json"
    )
    assert catalog["links"][2]["rel"] == "parent"
    assert catalog["links"][2]["href"] == "../collection.json"
    assert catalog["links"][3]["rel"] == "child"
    assert catalog["links"][3]["href"] == "083/catalog.json"
    assert catalog["links"][-1]["rel"] == "child"
    assert catalog["links"][-1]["href"] == "111/catalog.json"
    catv.validate_dict(catalog)


def test_get_base_collection():
    """test_get_base_collection."""
    col = get_base_collection(sat_mission="CBERS4", camera="AWFI")
    assert col["providers"][0]["url"] == "http://www.cbers.inpe.br"

    col = get_base_collection(sat_mission="AMAZONIA1", camera="WFI")
    assert col["providers"][0]["url"] == "http://www.inpe.br/amazonia1"


@pytest.mark.parametrize(
    "satellite, mission, camera", [("CBERS", "4", "AWFI"), ("AMAZONIA", "1", "WFI")],
)
def test_base_stac_catalog(
    satellite: str, mission: str, camera: str
):  # pylint: disable=too-many-statements
    """base_stac_catalog_test"""

    catv = STACValidator(schema_filename="catalog.json")
    colv = STACValidator(schema_filename="collection.json")

    # path/row level
    catalog = base_stac_catalog("cbers-stac", satellite, mission, camera, "130", "100")
    assert catalog["stac_version"] == STAC_VERSION
    # validate_dict(catalog)
    assert catalog["id"] == f"{satellite}{mission} {camera} 130/100"
    assert (
        catalog["description"]
        == f"{satellite}{mission} {camera} camera path 130 row 100 catalog"
    )
    assert catalog["links"][0]["rel"] == "self"
    assert (
        catalog["links"][0]["href"]
        == f"https://cbers-stac.s3.amazonaws.com/{satellite}{mission}/{camera}/130/100/catalog.json"
    )
    assert catalog["links"][1]["rel"] == "root"
    assert (
        catalog["links"][1]["href"]
        == "https://cbers-stac.s3.amazonaws.com/catalog.json"
    )
    assert catalog["links"][2]["rel"] == "parent"
    assert catalog["links"][2]["href"] == "../catalog.json"
    catv.validate_dict(catalog)
    assert "license" not in catalog

    # Same as before using concatenated satellite+mission
    catalog = base_stac_catalog(
        "cbers-stac", f"{satellite}{mission}", None, f"{camera}", "130", "100"
    )
    assert catalog["stac_version"] == STAC_VERSION
    # validate_dict(catalog)
    assert catalog["id"] == f"{satellite}{mission} {camera} 130/100"
    assert (
        catalog["description"]
        == f"{satellite}{mission} {camera} camera path 130 row 100 catalog"
    )
    assert catalog["links"][0]["rel"] == "self"
    assert (
        catalog["links"][0]["href"]
        == f"https://cbers-stac.s3.amazonaws.com/{satellite}{mission}/{camera}/"
        "130/100/catalog.json"
    )
    assert catalog["links"][1]["rel"] == "root"
    assert (
        catalog["links"][1]["href"]
        == "https://cbers-stac.s3.amazonaws.com/catalog.json"
    )
    assert catalog["links"][2]["rel"] == "parent"
    assert catalog["links"][2]["href"] == "../catalog.json"
    catv.validate_dict(catalog)
    assert "license" not in catalog

    # path level
    catalog = base_stac_catalog(
        "cbers-stac", f"{satellite}", f"{mission}", f"{camera}", "130"
    )
    assert catalog["stac_version"] == STAC_VERSION
    assert catalog["id"] == f"{satellite}{mission} {camera} 130"
    assert (
        catalog["description"]
        == f"{satellite}{mission} {camera} camera path 130 catalog"
    )
    assert catalog["links"][0]["rel"] == "self"
    assert (
        catalog["links"][0]["href"]
        == f"https://cbers-stac.s3.amazonaws.com/{satellite}{mission}/{camera}/"
        "130/catalog.json"
    )
    assert catalog["links"][1]["rel"] == "root"
    assert (
        catalog["links"][1]["href"]
        == "https://cbers-stac.s3.amazonaws.com/catalog.json"
    )
    assert catalog["links"][2]["rel"] == "parent"
    assert catalog["links"][2]["href"] == "../collection.json"
    catv.validate_dict(catalog)

    # This is the collection level
    collection = base_stac_catalog(
        "cbers-stac", f"{satellite}", f"{mission}", f"{camera}"
    )
    assert collection["stac_version"] == STAC_VERSION
    assert collection["id"] == f"{satellite}{mission}-{camera}"
    assert collection["description"] == f"{satellite}{mission} {camera} camera catalog"
    assert collection["links"][0]["rel"] == "self"
    assert (
        collection["links"][0]["href"]
        == f"https://cbers-stac.s3.amazonaws.com/{satellite}{mission}/{camera}/"
        "collection.json"
    )
    assert collection["links"][1]["rel"] == "root"
    assert (
        collection["links"][1]["href"]
        == "https://cbers-stac.s3.amazonaws.com/catalog.json"
    )
    assert collection["links"][2]["rel"] == "parent"
    assert collection["summaries"]["gsd"] == [64.0]
    assert collection["links"][2]["href"] == "../catalog.json"
    colv.validate_dict(collection)

    # This is the collection level using satellite and mission
    # concatenated
    catalog = base_stac_catalog(
        "cbers-stac", f"{satellite}{mission}", None, f"{camera}"
    )
    assert catalog["stac_version"] == STAC_VERSION
    assert catalog["id"] == f"{satellite}{mission}-{camera}"
    assert catalog["description"] == f"{satellite}{mission} {camera} camera catalog"
    assert catalog["links"][0]["rel"] == "self"
    assert (
        catalog["links"][0]["href"]
        == f"https://cbers-stac.s3.amazonaws.com/{satellite}{mission}/{camera}/"
        "collection.json"
    )
    assert catalog["links"][1]["rel"] == "root"
    assert (
        catalog["links"][1]["href"]
        == "https://cbers-stac.s3.amazonaws.com/catalog.json"
    )
    assert catalog["links"][2]["rel"] == "parent"
    assert catalog["links"][2]["href"] == "../catalog.json"
    assert catalog["summaries"]["gsd"] == [64.0]
    colv.validate_dict(catalog)

    catalog = base_stac_catalog("cbers-stac", f"{satellite}", f"{mission}")
    assert catalog["stac_version"] == STAC_VERSION
    # validate_dict(catalog)
    assert catalog["id"] == f"{satellite}{mission}"
    assert catalog["description"] == f"{satellite}{mission} catalog"
    assert catalog["links"][0]["rel"] == "self"
    assert (
        catalog["links"][0]["href"]
        == f"https://cbers-stac.s3.amazonaws.com/{satellite}{mission}/"
        "catalog.json"
    )
    assert catalog["links"][1]["rel"] == "root"
    assert (
        catalog["links"][1]["href"]
        == "https://cbers-stac.s3.amazonaws.com/catalog.json"
    )
    assert catalog["links"][2]["rel"] == "parent"
    assert catalog["links"][2]["href"] == "../catalog.json"
    catv.validate_dict(catalog)

    catalog = base_stac_catalog("cbers-stac", f"{satellite}")
    assert catalog["stac_version"], STAC_VERSION
    # validate_dict(catalog)
    assert catalog["id"] == f"{satellite}"
    assert catalog["description"] == f"{satellite} catalog"
    assert len(catalog["links"]) == 2
    assert catalog["links"][0]["rel"] == "self"
    assert (
        catalog["links"][0]["href"]
        == "https://cbers-stac.s3.amazonaws.com/catalog.json"
    )
    assert catalog["links"][1]["rel"] == "root"
    assert (
        catalog["links"][1]["href"]
        == "https://cbers-stac.s3.amazonaws.com/catalog.json"
    )
    catv.validate_dict(catalog)


@pytest.mark.s3_bucket_args("cbers-stac")
def test_integration(s3_bucket):
    """integration_test"""

    s3_client, _ = s3_bucket

    prefix = "CBERS4/MUX/083/095"

    # Create STAC metadata structure, single file
    # The file contents doesn't matter for the test, we only check
    # here the filename
    s3_client.upload_file(
        Filename="test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json",
        Bucket="cbers-stac",
        Key=f"{prefix}/CBERS_4_MUX_20170714_083_095_L4.json",
    )
    catalog = build_catalog_from_s3(bucket="cbers-stac", prefix=prefix)
    assert catalog["id"] == "CBERS4 MUX 083/095"
    write_catalog_to_s3(bucket="cbers-stac", prefix="test/" + prefix, catalog=catalog)

    prefix = "CBERS4/MUX/083"
    catalog = build_catalog_from_s3(bucket="cbers-stac", prefix=prefix)
    write_catalog_to_s3(bucket="cbers-stac", prefix="test/" + prefix, catalog=catalog)

    prefix = "CBERS4/MUX"
    catalog = build_catalog_from_s3(bucket="cbers-stac", prefix=prefix)
    write_catalog_to_s3(bucket="cbers-stac", prefix="test/" + prefix, catalog=catalog)

    # Same as above for Amazonia1
    prefix = "AMAZONIA1/WFI/033/018"
    s3_client.upload_file(
        Filename="test/fixtures/ref_AMAZONIA_1_WFI_20220810_033_018_L4.json",
        Bucket="cbers-stac",
        Key=f"{prefix}/CBERS_4_MUX_20170528_090_084_L2.json",
    )
    catalog = build_catalog_from_s3(bucket="cbers-stac", prefix=prefix)
    assert catalog["id"] == "AMAZONIA1 WFI 033/018"
    write_catalog_to_s3(bucket="cbers-stac", prefix="test/" + prefix, catalog=catalog)

    prefix = "AMAZONIA1/WFI/033"
    catalog = build_catalog_from_s3(bucket="cbers-stac", prefix=prefix)
    write_catalog_to_s3(bucket="cbers-stac", prefix="test/" + prefix, catalog=catalog)

    prefix = "AMAZONIA1/WFI"
    catalog = build_catalog_from_s3(bucket="cbers-stac", prefix=prefix)
    write_catalog_to_s3(bucket="cbers-stac", prefix="test/" + prefix, catalog=catalog)
