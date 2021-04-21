"""utils_test"""

import json

from cbers2stac.layers.common.utils import (
    build_collection_name,
    get_api_stac_root,
    get_collection_ids,
    get_collection_s3_key,
    get_collections_for_satmission,
    get_satmissions,
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
    assert set(smi) == set(["CBERS4", "CBERS4A"])

    smi = get_satmissions(use_hyphen=True)
    assert set(smi) == set(["CBERS-4", "CBERS-4A"])


def test_build_collection_name():
    """test_build_collection_name"""

    collection = build_collection_name(satellite="CBERS", mission="4A", camera="WFI")
    assert collection == "CBERS4A-WFI"

    collection = build_collection_name(satellite="CBERS4", camera="AWFI")
    assert collection == "CBERS4-AWFI"


def test_collection_utils():
    """test_collection_utils"""

    collections = get_collection_ids()
    assert len(collections) == 7

    mux_prefix = get_collection_s3_key("CBERS4-MUX")
    assert mux_prefix == "CBERS4/MUX/collection.json"


def test_get_api_stac_root():
    """test_get_api_stac_root"""

    with open("test/api_event.json", "r") as jfile:
        event = json.load(jfile)

    sroot = get_api_stac_root(event=event)
    assert sroot["links"][0]["self"] == "https://stac.amskepler.com/v07/stac"
    assert (
        sroot["links"][1]["child"]
        == "https://stac.amskepler.com/v07/collections/CBERS4-MUX"
    )
    assert (
        sroot["links"][4]["child"]
        == "https://stac.amskepler.com/v07/collections/CBERS4-PAN5M"
    )

    # Check correct number on second call
    sroot = get_api_stac_root(event=event)
    assert len(sroot["links"]) == 8


def test_parse_api_gateway_event():
    """test_parse_api_gateway_event"""

    with open("test/api_event.json", "r") as cfile:
        event = json.load(cfile)
    parsed = parse_api_gateway_event(event)
    assert parsed["phost"] == "https://stac.amskepler.com"
    assert parsed["ppath"] == "https://stac.amskepler.com/v07/stac"
    assert parsed["spath"] == "https://stac.amskepler.com/v07/stac"
    assert parsed["vpath"] == "https://stac.amskepler.com/v07"
    assert parsed["prefix"] == "v07"


def test_static_to_api_collection():
    """test_static_to_api_collection"""

    with open("test/cbers4muxcollection.json", "r") as cfile:
        collection = json.load(cfile)
    with open("test/api_event.json", "r") as cfile:
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
