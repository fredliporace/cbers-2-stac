"""utils_test"""

import json
from test.stac_validator import STACValidator

from cbers2stac.layers.common.utils import (
    build_collection_name,
    get_api_stac_root,
    get_collection_ids,
    get_collection_s3_key,
    get_collections_for_satmission,
    get_satmissions,
    next_page_get_method_params,
    parse_api_gateway_event,
    static_to_api_collection,
)


def test_get_collection_from_satmission():
    """test_get_collection_from_satmission"""
    cols = get_collections_for_satmission(satellite="CBERS", mission="4")
    assert set(cols) == set(["MUX", "AWFI", "PAN5M", "PAN10M"])

    cols = get_collections_for_satmission(satellite="CBERS", mission="4A")
    assert set(cols) == set(["MUX", "WFI", "WPM"])


def test_get_satmissions():
    """
    test_get_satmissions
    """
    smi = get_satmissions(use_hyphen=False)
    assert set(smi) == set(["AMAZONIA1", "CBERS4", "CBERS4A"])

    smi = get_satmissions(use_hyphen=True)
    assert set(smi) == set(["AMAZONIA-1", "CBERS-4", "CBERS-4A"])


def test_build_collection_name():
    """test_build_collection_name"""

    collection = build_collection_name(satellite="CBERS", mission="4A", camera="WFI")
    assert collection == "CBERS4A-WFI"

    collection = build_collection_name(satellite="CBERS4", camera="AWFI")
    assert collection == "CBERS4-AWFI"


def test_collection_utils():
    """test_collection_utils"""

    collections = get_collection_ids()
    assert len(collections) == 8

    mux_prefix = get_collection_s3_key("CBERS4-MUX")
    assert mux_prefix == "CBERS4/MUX/collection.json"

    am1_wfi_prefix = get_collection_s3_key("AMAZONIA1-WFI")
    assert am1_wfi_prefix == "AMAZONIA1/WFI/collection.json"


def test_get_api_stac_root():
    """test_get_api_stac_root"""

    val = STACValidator(schema_filename="catalog.json")

    with open("test/fixtures/api_event.json", "r", encoding="utf-8") as jfile:
        event = json.load(jfile)

    # core only
    sroot = get_api_stac_root(event=event)
    assert sroot["links"][0]["href"] == "https://stac.amskepler.com/v07/stac"
    val.validate_dict(sroot)
    assert len(sroot["links"]) == 1

    # item-search
    sroot = get_api_stac_root(event=event, item_search=True)
    assert len(sroot["links"]) == 3
    assert sroot["links"][0]["href"] == "https://stac.amskepler.com/v07/stac"
    assert sroot["links"][1]["href"] == "https://stac.amskepler.com/v07/stac/search"
    assert sroot["links"][2]["href"] == "https://stac.amskepler.com/v07/stac/search"
    val.validate_dict(sroot)

    # include static collection links
    sroot = get_api_stac_root(
        event=event,
        item_search=True,
        static_catalog=True,
        static_bucket="cbers-stac-1-0-0",
    )
    val.validate_dict(sroot)
    assert len(sroot["links"]) == 4
    assert (
        sroot["links"][1]["href"]
        == "https://cbers-stac-1-0-0.s3.amazonaws.com/catalog.json"
    )

    # Commented out while /collections endpoint is not implemented
    # assert (
    #     sroot["links"][1]["child"]
    #     == "https://stac.amskepler.com/v07/collections/CBERS4-MUX"
    # )
    # assert (
    #     sroot["links"][4]["child"]
    #     == "https://stac.amskepler.com/v07/collections/CBERS4-PAN5M"
    # )


def test_parse_api_gateway_event():
    """test_parse_api_gateway_event"""

    with open("test/fixtures/api_event.json", "r", encoding="utf-8") as cfile:
        event = json.load(cfile)
    parsed = parse_api_gateway_event(event)
    assert parsed["phost"] == "https://stac.amskepler.com"
    assert parsed["ppath"] == "https://stac.amskepler.com/v07/stac"
    assert parsed["spath"] == "https://stac.amskepler.com/v07/stac"
    assert parsed["vpath"] == "https://stac.amskepler.com/v07"
    assert parsed["prefix"] == "v07"


def test_static_to_api_collection():
    """test_static_to_api_collection"""

    with open("test/cbers4muxcollection.json", "r", encoding="utf-8") as cfile:
        collection = json.load(cfile)
    with open("test/fixtures/api_event.json", "r", encoding="utf-8") as cfile:
        event = json.load(cfile)
    # from nose.tools import set_trace; set_trace()
    api_collection = static_to_api_collection(collection=collection, event=event)

    assert len(api_collection["links"]) == 4
    for link in api_collection["links"]:
        if link["rel"] == "self":
            assert (
                link["href"] == "https://stac.amskepler.com/v07/collections/CBERS4-MUX"
            )
        elif link["rel"] == "parent":
            assert link["href"] == "https://stac.amskepler.com/v07/stac"
        elif link["rel"] == "root":
            assert link["href"] == "https://stac.amskepler.com/v07/stac"
        elif link["rel"] == "items":
            assert (
                link["href"]
                == "https://stac.amskepler.com/v07/collections/CBERS4-MUX/items"
            )


def test_next_page_get_method_params():
    """
    test_next_page_get_method_params
    """

    params = next_page_get_method_params({})
    assert params == "page=2"

    params = next_page_get_method_params({"page": "1"})
    assert params == "page=2"

    params = next_page_get_method_params({"page": "2", "limit": "2"})
    assert params == "page=3&limit=2"

    params = next_page_get_method_params(None)
    assert params == "page=2"
