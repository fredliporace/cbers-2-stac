"""cbers_am_2_stac_test."""

import difflib
import re
from test.stac_validator import STACValidator

import pytest
from jsonschema.exceptions import ValidationError

from cbers2stac.layers.common.cbers_2_stac import (
    build_stac_item_keys,
    candidate_xml_files,
    convert_inpe_to_stac,
    epsg_from_utm_zone,
    get_keys_from_cbers_am,
)


def diff_files(filename1, filename2):
    """
    Return string with context diff, empty if files are equal
    """
    with open(filename1, encoding="utf-8") as file1:
        with open(filename2, encoding="utf-8") as file2:
            diff = difflib.context_diff(file1.readlines(), file2.readlines())
    res = ""
    for line in diff:
        res += line  # pylint: disable=consider-using-join
    return res


def test_epsg_from_utm_zone():
    """test_epsg_from_utm_zone"""
    assert epsg_from_utm_zone(-23) == 32723
    assert epsg_from_utm_zone(23) == 32623


def test_candidate_xml_files():
    """test_candidate_xml_files."""

    options = candidate_xml_files("CBERS_4_MUX_20170528_090_084_L2_BAND6.xml")
    assert len(options) == 1
    assert options[0] == "CBERS_4_MUX_20170528_090_084_L2_BAND6.xml"

    # Amazonia1 special case with multiple options
    options = candidate_xml_files("AMAZONIA_1_WFI_20220811_036_018_L4_BAND2.xml")
    assert len(options) == 3
    assert options == [
        "AMAZONIA_1_WFI_20220811_036_018_L4_BAND2.xml",
        "AMAZONIA_1_WFI_20220811_036_018_L4_LEFT_BAND2.xml",
        "AMAZONIA_1_WFI_20220811_036_018_L4_RIGHT_BAND2.xml",
    ]


def test_get_keys_from_cbers4():
    """test_get_keys_from_cbers"""

    # MUX
    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4_MUX_20170528_090_084_L2_BAND6.xml"
    )
    assert meta["mission"] == "CBERS"
    assert meta["number"] == "4"
    assert meta["sensor"] == "MUX"
    assert meta["projection_name"] == "UTM"
    assert meta["origin_longitude"] == "27"
    assert meta["origin_latitude"] == "0"
    assert meta["ct_lat"] == "14.423188"
    assert meta["ct_lon"] == "24.257145"
    assert meta["collection"] == "CBERS4-MUX"
    assert meta["vz"] == "-7131.111624"
    assert meta["band_5_gain"] == "2"

    # AWFI
    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4_AWFI_20170409_167_123_L4_BAND14.xml"
    )
    assert meta["sensor"] == "AWFI"
    assert meta["mission"] == "CBERS"
    assert meta["number"] == "4"
    assert meta["projection_name"] == "UTM"
    assert meta["origin_longitude"] == "-57"
    assert meta["origin_latitude"] == "0"
    assert meta["collection"] == "CBERS4-AWFI"
    assert meta["band_13_gain"] == "2"

    # PAN10
    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4_PAN10M_20190201_180_125_L2_BAND2.xml"
    )
    assert meta["sensor"] == "PAN10M"
    assert meta["mission"] == "CBERS"
    assert meta["number"] == "4"
    assert meta["projection_name"] == "UTM"
    assert meta["origin_longitude"] == "-69"
    assert meta["origin_latitude"] == "0"
    assert meta["collection"] == "CBERS4-PAN10M"

    # PAN5
    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4_PAN5M_20161009_219_050_L2_BAND1.xml"
    )
    assert meta["sensor"] == "PAN5M"
    assert meta["mission"] == "CBERS"
    assert meta["number"] == "4"
    assert meta["projection_name"] == "UTM"
    assert meta["origin_longitude"] == "-93"
    assert meta["origin_latitude"] == "0"
    assert meta["collection"] == "CBERS4-PAN5M"

    # PAN10, no gain attribute for each band
    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4_PAN10M_20160322_156_117_L2_BAND2.xml"
    )
    assert meta["sensor"] == "PAN10M"
    assert meta["mission"] == "CBERS"
    assert meta["number"] == "4"
    assert meta["projection_name"] == "UTM"


def test_get_keys_from_cbers4a():
    """test_get_keys_from_cbers4a"""

    # MUX
    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4A_MUX_20200808_201_137_L4_BAND6.xml"
    )
    assert meta["mission"] == "CBERS"
    assert meta["number"] == "4A"
    assert meta["sensor"] == "MUX"
    assert meta["projection_name"] == "UTM"
    assert meta["origin_longitude"] == "-45"
    assert meta["origin_latitude"] == "0"
    assert meta["collection"] == "CBERS4A-MUX"
    assert meta["vz"] == "-7079.380000"

    # WPM
    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4A_WPM_20200730_209_139_L4_BAND2.xml"
    )
    assert meta["sensor"] == "WPM"
    assert meta["mission"] == "CBERS"
    assert meta["number"] == "4A"
    assert meta["projection_name"] == "UTM"
    assert meta["origin_longitude"] == "-51"
    assert meta["origin_latitude"] == "0"
    assert meta["collection"] == "CBERS4A-WPM"

    # WFI
    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4A_WFI_20200801_221_156_L4_BAND13.xml"
    )
    assert meta["sensor"] == "WFI"
    assert meta["mission"] == "CBERS"
    assert meta["number"] == "4A"
    assert meta["projection_name"] == "UTM"
    assert meta["origin_longitude"] == "-63"
    assert meta["origin_latitude"] == "0"
    assert meta["collection"] == "CBERS4A-WFI"
    assert meta["ur_lat"] == "-31.394870"
    assert meta["ur_lon"] == "-59.238522"
    assert meta["lr_lat"] == "-38.025663"
    assert meta["lr_lon"] == "-60.669294"
    assert meta["ct_lat"] == "-33.625192"
    assert meta["ct_lon"] == "-62.969105"
    assert meta["bb_ll_lat"] == "-38.033425"
    assert meta["bb_ll_lon"] == "-68.887467"
    assert meta["bb_ur_lat"] == "-29.919749"
    assert meta["bb_ur_lon"] == "-59.245969"


def test_get_keys_from_amazonia1():
    """test_get_keys_from_amazonia1."""

    # WFI, both optics
    meta = get_keys_from_cbers_am(
        "test/fixtures/AMAZONIA_1_WFI_20220811_036_018_L4_BAND2.xml"
    )
    assert meta["mission"] == "AMAZONIA"
    assert meta["number"] == "1"
    assert meta["sensor"] == "WFI"
    assert meta["projection_name"] == "UTM"
    assert meta["origin_longitude"] == "-51"
    assert meta["origin_latitude"] == "0"
    assert meta["collection"] == "AMAZONIA1-WFI"
    assert meta["vz"] == "-7159.006657"
    assert meta["vertical_pixel_size"] == "64.000000"
    assert meta["horizontal_pixel_size"] == "64.000000"
    assert meta["no_level_id"] == "AMAZONIA_1_WFI_20220811_036_018"
    assert (
        meta["download_url"]
        == "AMAZONIA1/WFI/036/018/AMAZONIA_1_WFI_20220811_036_018_L4"
    )
    assert meta["sat_sensor"] == "AMAZONIA1/WFI"
    assert meta["sat_number"] == "AMAZONIA-1"
    assert meta["meta_file"] == "AMAZONIA_1_WFI_20220811_036_018_L4_BAND2.xml"
    # Gain information is not used
    assert meta["band_1_gain"] is None
    assert meta["band_4_gain"] is None
    assert meta["optics"] == ""

    # WFI, single optics
    meta = get_keys_from_cbers_am(
        "test/fixtures/AMAZONIA_1_WFI_20220810_033_018_L4_LEFT_BAND2.xml"
    )
    assert meta["mission"] == "AMAZONIA"
    assert meta["number"] == "1"
    assert meta["sensor"] == "WFI"
    assert meta["projection_name"] == "UTM"
    assert meta["origin_longitude"] == "-39"
    assert meta["origin_latitude"] == "0"
    assert meta["collection"] == "AMAZONIA1-WFI"
    assert meta["vz"] == "-7158.599571"
    assert meta["vertical_pixel_size"] == "64.000000"
    assert meta["horizontal_pixel_size"] == "64.000000"
    assert meta["no_level_id"] == "AMAZONIA_1_WFI_20220810_033_018"
    assert (
        meta["download_url"]
        == "AMAZONIA1/WFI/033/018/AMAZONIA_1_WFI_20220810_033_018_L4"
    )
    assert meta["sat_sensor"] == "AMAZONIA1/WFI"
    assert meta["sat_number"] == "AMAZONIA-1"
    assert meta["meta_file"] == "AMAZONIA_1_WFI_20220810_033_018_L4_LEFT_BAND2.xml"
    # Gain information is not used
    assert meta["band_1_gain"] is None
    assert meta["band_4_gain"] is None
    assert meta["optics"] == "_LEFT"


def test_build_awfi_stac_item_keys():
    """test_awfi_build_stac_item_keys"""

    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4_AWFI_20170409_167_123_L4_BAND14.xml"
    )
    buckets = {"metadata": "cbers-meta-pds", "cog": "cbers-pds", "stac": "cbers-stac"}
    smeta = build_stac_item_keys(meta, buckets)

    # id
    assert smeta["id"] == "CBERS_4_AWFI_20170409_167_123_L4"

    # bbox
    assert len(smeta["bbox"]) == 4
    assert smeta["bbox"][1] == -24.425554
    assert smeta["bbox"][0] == -63.157102
    assert smeta["bbox"][3] == -16.364230
    assert smeta["bbox"][2] == -53.027684

    # geometry
    assert len(smeta["geometry"]["coordinates"][0][0]) == 5
    assert smeta["geometry"]["coordinates"][0][0][0][1] == -23.152887
    assert smeta["geometry"]["coordinates"][0][0][0][0] == -63.086835
    assert smeta["geometry"]["coordinates"][0][0][4][1] == -23.152887
    assert smeta["geometry"]["coordinates"][0][0][4][0] == -63.086835

    # properties
    assert smeta["properties"]["datetime"] == "2017-04-09T14:09:23Z"
    assert smeta["properties"]["gsd"] == 64.0

    # properties:view
    assert smeta["properties"]["view:sun_azimuth"] == 43.9164
    assert smeta["properties"]["view:sun_elevation"] == 53.4479
    assert smeta["properties"]["view:off_nadir"] == 0.00828942

    # properties:proj
    assert smeta["properties"]["proj:epsg"] == 32721

    # properties:cbers
    assert smeta["properties"]["cbers:data_type"] == "L4"
    assert smeta["properties"]["cbers:path"] == 167
    assert smeta["properties"]["cbers:row"] == 123

    assert (
        smeta["assets"]["thumbnail"]["href"]
        == "https://s3.amazonaws.com/cbers-meta-pds/CBERS4/AWFI/167/123/"
        "CBERS_4_AWFI_20170409_167_123_L4/CBERS_4_AWFI_20170409_167_123.jpg"
    )
    assert (
        smeta["assets"]["metadata"]["href"]
        == "s3://cbers-pds/CBERS4/AWFI/167/123/CBERS_4_AWFI_20170409_167_123_L4/"
        "CBERS_4_AWFI_20170409_167_123_L4_BAND14.xml"
    )
    assert (
        smeta["assets"]["B13"]["href"]
        == "s3://cbers-pds/CBERS4/AWFI/167/123/CBERS_4_AWFI_20170409_167_123_L4/"
        "CBERS_4_AWFI_20170409_167_123_L4_BAND13.tif"
    )


def test_build_mux_stac_item_keys():
    """test_mux_build_stac_item_keys"""

    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4_MUX_20170528_090_084_L2_BAND6.xml"
    )
    buckets = {"metadata": "cbers-meta-pds", "cog": "cbers-pds", "stac": "cbers-stac"}
    smeta = build_stac_item_keys(meta, buckets)

    # id
    assert smeta["id"] == "CBERS_4_MUX_20170528_090_084_L2"

    # bbox
    assert len(smeta["bbox"]) == 4
    assert smeta["bbox"][1] == 13.700498
    assert smeta["bbox"][0] == 23.465111
    assert smeta["bbox"][3] == 14.988180
    assert smeta["bbox"][2] == 24.812825

    # geometry
    assert len(smeta["geometry"]["coordinates"][0][0]) == 5
    assert smeta["geometry"]["coordinates"][0][0][0][1] == 13.891487
    assert smeta["geometry"]["coordinates"][0][0][0][0] == 23.463987
    assert smeta["geometry"]["coordinates"][0][0][4][1] == 13.891487
    assert smeta["geometry"]["coordinates"][0][0][4][0] == 23.463987

    # properties
    assert smeta["properties"]["datetime"] == "2017-05-28T09:01:17Z"

    # properties:view
    assert smeta["properties"]["view:sun_azimuth"] == 66.2923
    assert smeta["properties"]["view:sun_elevation"] == 70.3079
    assert smeta["properties"]["view:off_nadir"] == 0.00744884

    # properties:proj
    assert smeta["properties"]["proj:epsg"] == 32635

    # properties:cbers
    assert smeta["properties"]["cbers:data_type"] == "L2"
    assert smeta["properties"]["cbers:path"] == 90
    assert smeta["properties"]["cbers:row"] == 84

    # links
    for link in smeta["links"]:
        if link["rel"] == "self":
            assert (
                link["href"] == "https://cbers-stac.s3.amazonaws.com/CBERS4/"
                "MUX/"
                "090/084/CBERS_4_MUX_20170528_090_084_L2.json"
            )
        elif link["rel"] == "parent":
            assert (
                link["href"] == "https://cbers-stac.s3.amazonaws.com/"
                "CBERS4/MUX/090/084/catalog.json"
            )
        elif link["rel"] == "collection":
            assert (
                link["href"] == "https://cbers-stac.s3.amazonaws.com/"
                "CBERS4/MUX/collection.json"
            )
        else:
            pytest.fail(f"Unrecognized rel {link['rel']}")

    # assets
    # 4 bands, 1 metadata, 1 thumbnail
    assert len(smeta["assets"]) == 6

    assert (
        smeta["assets"]["thumbnail"]["href"]
        == "https://s3.amazonaws.com/cbers-meta-pds/CBERS4/MUX/090/084/"
        "CBERS_4_MUX_20170528_090_084_L2/CBERS_4_MUX_20170528_090_084.jpg"
    )
    assert (
        smeta["assets"]["metadata"]["href"]
        == "s3://cbers-pds/CBERS4/MUX/090/084/CBERS_4_MUX_20170528_090_084_L2/"
        "CBERS_4_MUX_20170528_090_084_L2_BAND6.xml"
    )
    assert (
        smeta["assets"]["B5"]["href"]
        == "s3://cbers-pds/CBERS4/MUX/090/084/CBERS_4_MUX_20170528_090_084_L2/"
        "CBERS_4_MUX_20170528_090_084_L2_BAND5.tif"
    )


def test_build_pan10_stac_item_keys():
    """test_pan10_build_stac_item_keys"""

    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4_PAN10M_20190201_180_125_L2_BAND2.xml"
    )
    buckets = {"metadata": "cbers-meta-pds", "cog": "cbers-pds", "stac": "cbers-stac"}
    smeta = build_stac_item_keys(meta, buckets)

    # id
    assert smeta["id"] == "CBERS_4_PAN10M_20190201_180_125_L2"

    # bbox
    assert len(smeta["bbox"]) == 4
    assert smeta["bbox"][1] == -22.882858
    assert smeta["bbox"][0] == -71.601800
    assert smeta["bbox"][3] == -21.746077
    assert smeta["bbox"][2] == -70.762020

    # skipping geometry values, same as other cameras
    assert len(smeta["geometry"]["coordinates"][0][0]) == 5

    # properties
    assert smeta["properties"]["datetime"] == "2019-02-01T14:36:38Z"

    # properties:view
    assert smeta["properties"]["view:sun_azimuth"] == 87.5261
    assert smeta["properties"]["view:sun_elevation"] == 57.0749
    assert smeta["properties"]["view:off_nadir"] == 0.0073997

    # properties:proj
    assert smeta["properties"]["proj:epsg"] == 32719

    # properties:cbers
    assert smeta["properties"]["cbers:data_type"] == "L2"
    assert smeta["properties"]["cbers:path"] == 180
    assert smeta["properties"]["cbers:row"] == 125

    # links
    for link in smeta["links"]:
        if link["rel"] == "self":
            assert (
                link["href"] == "https://cbers-stac.s3.amazonaws.com/"
                "CBERS4/PAN10M/180/125/CBERS_4_PAN10M_"
                "20190201_180_125_L2.json"
            )
        elif link["rel"] == "parent":
            assert (
                link["href"] == "https://cbers-stac.s3.amazonaws.com/"
                "CBERS4/PAN10M/180/125/catalog.json"
            )
        elif link["rel"] == "collection":
            assert (
                link["href"] == "https://cbers-stac.s3.amazonaws.com/"
                "CBERS4/PAN10M/collection.json"
            )
        else:
            pytest.fail(f"Unrecognized rel {link['rel']}")

    # assets
    # 3 bands, 1 metadata, 1 thumbnail
    assert len(smeta["assets"]) == 5

    assert (
        smeta["assets"]["thumbnail"]["href"]
        == "https://s3.amazonaws.com/cbers-meta-pds/CBERS4/PAN10M/180/125/"
        "CBERS_4_PAN10M_20190201_180_125_L2/CBERS_4_PAN10M_20190201_180_125.jpg"
    )
    assert (
        smeta["assets"]["metadata"]["href"]
        == "s3://cbers-pds/CBERS4/PAN10M/180/125/CBERS_4_PAN10M_20190201_180_125_L2/"
        "CBERS_4_PAN10M_20190201_180_125_L2_BAND2.xml"
    )
    assert (
        smeta["assets"]["B2"]["href"]
        == "s3://cbers-pds/CBERS4/PAN10M/180/125/CBERS_4_PAN10M_20190201_180_125_L2/"
        "CBERS_4_PAN10M_20190201_180_125_L2_BAND2.tif"
    )


def test_build_pan5_stac_item_keys():
    """test_pan5_build_stac_item_keys"""

    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4_PAN5M_20161009_219_050_L2_BAND1.xml"
    )
    buckets = {"metadata": "cbers-meta-pds", "cog": "cbers-pds", "stac": "cbers-stac"}
    smeta = build_stac_item_keys(meta, buckets)

    # id
    assert smeta["id"] == "CBERS_4_PAN5M_20161009_219_050_L2"

    # bbox
    # skipping check for valus == same as other cameras
    assert len(smeta["bbox"]) == 4

    # skipping geometry values, same as other cameras
    assert len(smeta["geometry"]["coordinates"][0][0]) == 5

    # properties
    assert smeta["properties"]["datetime"] == "2016-10-09T17:14:38Z"

    # properties:view
    assert smeta["properties"]["view:sun_azimuth"] == 167.751
    assert smeta["properties"]["view:sun_elevation"] == 38.3015
    assert smeta["properties"]["view:off_nadir"] == 0.0050659

    # properties:proj
    assert smeta["properties"]["proj:epsg"] == 32615

    # properties:cbers
    assert smeta["properties"]["cbers:data_type"] == "L2"
    assert smeta["properties"]["cbers:path"] == 219
    assert smeta["properties"]["cbers:row"] == 50

    # links
    for link in smeta["links"]:
        if link["rel"] == "self":
            assert (
                link["href"] == "https://cbers-stac.s3.amazonaws.com/"
                "CBERS4/PAN5M/219/050/CBERS_4_PAN5M_"
                "20161009_219_050_L2.json"
            )
        elif link["rel"] == "parent":
            assert (
                link["href"] == "https://cbers-stac.s3.amazonaws.com/"
                "CBERS4/PAN5M/219/050/catalog.json"
            )
        elif link["rel"] == "collection":
            assert (
                link["href"] == "https://cbers-stac.s3.amazonaws.com/"
                "CBERS4/PAN5M/collection.json"
            )
        else:
            pytest.fail(f"Unrecognized rel {link['rel']}")

    # assets
    # 1 band, 1 metadata, 1 thumbnail
    assert len(smeta["assets"]) == 3
    assert (
        smeta["assets"]["thumbnail"]["href"]
        == "https://s3.amazonaws.com/cbers-meta-pds/CBERS4/PAN5M/219/050/"
        "CBERS_4_PAN5M_20161009_219_050_L2/CBERS_4_PAN5M_20161009_219_050.jpg"
    )
    assert (
        smeta["assets"]["metadata"]["href"]
        == "s3://cbers-pds/CBERS4/PAN5M/219/050/CBERS_4_PAN5M_20161009_219_050_L2/"
        "CBERS_4_PAN5M_20161009_219_050_L2_BAND1.xml"
    )
    assert (
        smeta["assets"]["B1"]["href"]
        == "s3://cbers-pds/CBERS4/PAN5M/219/050/CBERS_4_PAN5M_20161009_219_050_L2/"
        "CBERS_4_PAN5M_20161009_219_050_L2_BAND1.tif"
    )


def test_build_cb4a_wfi_stac_item_keys():
    """test_build_cb4a_wfi_stac_item_keys."""

    meta = get_keys_from_cbers_am(
        "test/fixtures/CBERS_4A_WFI_20200801_221_156_L4_BAND13.xml"
    )
    buckets = {"metadata": "cbers-meta-pds", "cog": "cbers-pds", "stac": "cbers-stac"}
    smeta = build_stac_item_keys(meta, buckets)

    # id
    assert smeta["id"] == "CBERS_4A_WFI_20200801_221_156_L4"

    # bbox
    assert len(smeta["bbox"]) == 4
    assert smeta["bbox"][1] == -38.033425
    assert smeta["bbox"][0] == -68.887467
    assert smeta["bbox"][3] == -29.919749
    assert smeta["bbox"][2] == -59.245969

    # geometry is built like other cameras, correct computation
    # is checked in test_get_keys_from_cbers4a

    # properties
    assert smeta["properties"]["datetime"] == "2020-08-01T14:32:45Z"

    # properties:view
    assert smeta["properties"]["view:sun_elevation"] == 32.8436
    assert smeta["properties"]["view:sun_azimuth"] == 29.477449999999997
    assert smeta["properties"]["view:off_nadir"] == 0.000431506

    # properties:proj
    assert smeta["properties"]["proj:epsg"] == 32720

    # properties:cbers
    assert smeta["properties"]["cbers:data_type"] == "L4"
    assert smeta["properties"]["cbers:path"] == 221
    assert smeta["properties"]["cbers:row"] == 156

    assert (
        smeta["assets"]["thumbnail"]["href"]
        == "https://s3.amazonaws.com/cbers-meta-pds/CBERS4A/WFI/221/156/"
        "CBERS_4A_WFI_20200801_221_156_L4/CBERS_4A_WFI_20200801_221_156.png"
    )
    assert (
        smeta["assets"]["metadata"]["href"]
        == "s3://cbers-pds/CBERS4A/WFI/221/156/CBERS_4A_WFI_20200801_221_156_L4/"
        "CBERS_4A_WFI_20200801_221_156_L4_BAND13.xml"
    )
    assert (
        smeta["assets"]["B13"]["href"]
        == "s3://cbers-pds/CBERS4A/WFI/221/156/CBERS_4A_WFI_20200801_221_156_L4/"
        "CBERS_4A_WFI_20200801_221_156_L4_BAND13.tif"
    )


def test_build_am1_wfi_stac_item_keys():
    """test_build_am1_wfi_stac_item_keys."""

    meta = get_keys_from_cbers_am(
        "test/fixtures/AMAZONIA_1_WFI_20220811_036_018_L4_BAND2.xml"
    )
    buckets = {"metadata": "cbers-meta-pds", "cog": "cbers-pds", "stac": "cbers-stac"}
    smeta = build_stac_item_keys(meta, buckets)

    # id
    assert smeta["id"] == "AMAZONIA_1_WFI_20220811_036_018_L4"

    # bbox
    assert len(smeta["bbox"]) == 4

    # geometry is built like other cameras, correct computation
    # is checked in test_get_keys_from_cbers4a

    # properties
    assert smeta["properties"]["datetime"] == "2022-08-11T14:01:37Z"

    # properties:view
    assert smeta["properties"]["view:sun_elevation"] == 50.042550000000006
    assert smeta["properties"]["view:sun_azimuth"] == 35.9219
    assert smeta["properties"]["view:off_nadir"] == 0.000416261

    # properties:proj
    assert smeta["properties"]["proj:epsg"] == 32722

    # properties:amazonia
    assert smeta["properties"]["amazonia:data_type"] == "L4"
    assert smeta["properties"]["amazonia:path"] == 36
    assert smeta["properties"]["amazonia:row"] == 18

    # assets
    assert (
        smeta["assets"]["thumbnail"]["href"]
        == "https://s3.amazonaws.com/cbers-meta-pds/AMAZONIA1/WFI/036/018/"
        "AMAZONIA_1_WFI_20220811_036_018_L4/AMAZONIA_1_WFI_20220811_036_018.png"
    )
    assert (
        smeta["assets"]["metadata"]["href"]
        == "s3://cbers-pds/AMAZONIA1/WFI/036/018/AMAZONIA_1_WFI_20220811_036_018_L4/"
        "AMAZONIA_1_WFI_20220811_036_018_L4_BAND2.xml"
    )
    assert (
        smeta["assets"]["B2"]["href"]
        == "s3://cbers-pds/AMAZONIA1/WFI/036/018/AMAZONIA_1_WFI_20220811_036_018_L4/"
        "AMAZONIA_1_WFI_20220811_036_018_L4_BAND2.tif"
    )

    # LEFT case
    meta = get_keys_from_cbers_am(
        "test/fixtures/AMAZONIA_1_WFI_20220810_033_018_L4_LEFT_BAND2.xml"
    )
    buckets = {"metadata": "cbers-meta-pds", "cog": "cbers-pds", "stac": "cbers-stac"}
    smeta = build_stac_item_keys(meta, buckets)

    # id
    assert smeta["id"] == "AMAZONIA_1_WFI_20220810_033_018_L4"

    # bbox
    assert len(smeta["bbox"]) == 4

    # geometry is built like other cameras, correct computation
    # is checked in test_get_keys_from_cbers4a

    # properties
    assert smeta["properties"]["datetime"] == "2022-08-10T13:01:35Z"

    # properties:view
    assert smeta["properties"]["view:sun_elevation"] == 48.9478
    assert smeta["properties"]["view:sun_azimuth"] == 38.3485
    assert smeta["properties"]["view:off_nadir"] == 0.000120206

    # properties:proj
    assert smeta["properties"]["proj:epsg"] == 32724

    # properties:amazonia
    assert smeta["properties"]["amazonia:data_type"] == "L4"
    assert smeta["properties"]["amazonia:path"] == 33
    assert smeta["properties"]["amazonia:row"] == 18

    # assets
    assert (
        smeta["assets"]["thumbnail"]["href"]
        == "https://s3.amazonaws.com/cbers-meta-pds/AMAZONIA1/WFI/033/018/"
        "AMAZONIA_1_WFI_20220810_033_018_L4/AMAZONIA_1_WFI_20220810_033_018.png"
    )
    assert (
        smeta["assets"]["metadata"]["href"]
        == "s3://cbers-pds/AMAZONIA1/WFI/033/018/AMAZONIA_1_WFI_20220810_033_018_L4/"
        "AMAZONIA_1_WFI_20220810_033_018_L4_LEFT_BAND2.xml"
    )
    assert (
        smeta["assets"]["B2"]["href"]
        == "s3://cbers-pds/AMAZONIA1/WFI/033/018/AMAZONIA_1_WFI_20220810_033_018_L4/"
        "AMAZONIA_1_WFI_20220810_033_018_L4_LEFT_BAND2.tif"
    )


def test_convert_inpe_to_stac(tmp_path):  # pylint: disable=too-many-statements
    """test_convert_inpe_to_stac"""

    jsv = STACValidator(schema_filename="item.json")

    buckets = {"metadata": "cbers-meta-pds", "cog": "cbers-pds", "stac": "cbers-stac"}

    # AWFI, Amazonia1, both optics
    output_filename = tmp_path / "AMAZONIA_1_WFI_20220811_036_018_L4.json"
    ref_output_filename = "test/fixtures/ref_AMAZONIA_1_WFI_20220811_036_018_L4.json"
    convert_inpe_to_stac(
        inpe_metadata_filename="test/fixtures/AMAZONIA_1_WFI_20220811_036_018_L4_BAND2.xml",
        stac_metadata_filename=output_filename,
        buckets=buckets,
    )
    jsv.validate(output_filename)
    resam1wfi = diff_files(ref_output_filename, output_filename)

    # AWFI, Amazonia1, left optics only
    output_filename = tmp_path / "AMAZONIA_1_WFI_20220810_033_018_L4.json"
    ref_output_filename = "test/fixtures/ref_AMAZONIA_1_WFI_20220810_033_018_L4.json"
    convert_inpe_to_stac(
        inpe_metadata_filename="test/fixtures/AMAZONIA_1_WFI_20220810_033_018_L4_LEFT_BAND2.xml",
        stac_metadata_filename=output_filename,
        buckets=buckets,
    )
    jsv.validate(output_filename)
    resam1wfileft = diff_files(ref_output_filename, output_filename)

    # MUX, CB4
    output_filename = tmp_path / "CBERS_4_MUX_20170528_090_084_L2.json"
    ref_output_filename = "test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2.json"
    convert_inpe_to_stac(
        inpe_metadata_filename="test/fixtures/CBERS_4_MUX_20170528"
        "_090_084_L2_BAND6.xml",
        stac_metadata_filename=output_filename,
        buckets=buckets,
    )
    jsv.validate(output_filename)
    rescb4mux = diff_files(ref_output_filename, output_filename)

    # MUX, CB4 with thumbnail extension override
    output_filename = tmp_path / "CBERS_4_MUX_20170528_090_084_L2.json"
    ref_output_filename = "test/fixtures/ref_CBERS_4_MUX_20170528_090_084_L2_PNG.json"
    convert_inpe_to_stac(
        inpe_metadata_filename="test/fixtures/CBERS_4_MUX_20170528"
        "_090_084_L2_BAND6.xml",
        stac_metadata_filename=output_filename,
        buckets=buckets,
        thumbnail_extension="png",
    )
    jsv.validate(output_filename)
    rescb4mux = diff_files(ref_output_filename, output_filename)

    # AWFI, CB4
    output_filename = tmp_path / "CBERS_4_AWFI_20170409_167_123_L4.json"
    ref_output_filename = "test/fixtures/ref_CBERS_4_AWFI_20170409_167_123_L4.json"
    convert_inpe_to_stac(
        inpe_metadata_filename="test/fixtures/CBERS_4_AWFI_20170409"
        "_167_123_L4_BAND14.xml",
        stac_metadata_filename=output_filename,
        buckets=buckets,
    )
    jsv.validate(output_filename)
    rescb4awfi = diff_files(ref_output_filename, output_filename)

    # PAN10M, CB4
    output_filename = tmp_path / "CBERS_4_PAN10M_20190201_180_125_L2.json"
    ref_output_filename = re.sub(
        r".*/CBERS", r"test/fixtures/ref_CBERS", str(output_filename)
    )
    convert_inpe_to_stac(
        inpe_metadata_filename="test/fixtures/CBERS_4_PAN10M_"
        "20190201_180_125_L2_BAND2.xml",
        stac_metadata_filename=output_filename,
        buckets=buckets,
    )
    jsv.validate(output_filename)
    rescb4pan10 = diff_files(ref_output_filename, output_filename)

    # PAN5M, CB4
    output_filename = tmp_path / "CBERS_4_PAN5M_20161009_219_050_L2.json"
    ref_output_filename = re.sub(
        r".*/CBERS", r"test/fixtures/ref_CBERS", str(output_filename)
    )
    convert_inpe_to_stac(
        inpe_metadata_filename="test/fixtures/CBERS_4_PAN5M_"
        "20161009_219_050_L2_BAND1.xml",
        stac_metadata_filename=output_filename,
        buckets=buckets,
    )
    jsv.validate(output_filename)
    rescb4pan5 = diff_files(ref_output_filename, output_filename)

    # PAN10M CB4, no gain
    output_filename = tmp_path / "CBERS_4_PAN10M_NOGAIN.json"
    ref_output_filename = re.sub(
        r".*/CBERS", r"test/fixtures/ref_CBERS", str(output_filename)
    )
    convert_inpe_to_stac(
        inpe_metadata_filename="test/fixtures/CBERS_4_PAN10M_20160322_156_117_L2_BAND2.xml",
        stac_metadata_filename=output_filename,
        buckets=buckets,
    )
    jsv.validate(output_filename)
    rescb4pan10ng = diff_files(ref_output_filename, output_filename)

    # MUX, CB4A
    output_filename = tmp_path / "CBERS_4A_MUX_20200808_201_137_L4.json"
    ref_output_filename = "test/fixtures/ref_CBERS_4A_MUX_20200808_201_137_L4.json"
    convert_inpe_to_stac(
        inpe_metadata_filename="test/fixtures/CBERS_4A_MUX_"
        "20200808_201_137_L4_BAND6.xml",
        stac_metadata_filename=output_filename,
        buckets=buckets,
    )
    jsv.validate(output_filename)
    rescb4amux = diff_files(ref_output_filename, output_filename)

    # WPM, CB4A
    output_filename = tmp_path / "CBERS_4A_WPM_20200730_209_139_L4.json"
    ref_output_filename = "test/fixtures/ref_CBERS_4A_WPM_20200730_209_139_L4.json"
    convert_inpe_to_stac(
        inpe_metadata_filename="test/fixtures/CBERS_4A_WPM_"
        "20200730_209_139_L4_BAND2.xml",
        stac_metadata_filename=output_filename,
        buckets=buckets,
    )
    jsv.validate(output_filename)
    rescb4awpm = diff_files(ref_output_filename, output_filename)

    # Check all diffs here to make bulk update for reference jsons
    # easier. Check the diff_update_references.sh script
    assert len(rescb4mux) == 0, rescb4mux
    assert len(rescb4awfi) == 0, rescb4awfi
    assert len(rescb4pan10) == 0, rescb4pan10
    assert len(rescb4pan5) == 0, rescb4pan5
    assert len(rescb4pan10ng) == 0, rescb4pan10ng
    assert len(rescb4amux) == 0, rescb4amux
    assert len(rescb4awpm) == 0, rescb4awpm
    assert len(resam1wfi) == 0, resam1wfi
    assert len(resam1wfileft) == 0, resam1wfileft


def test_json_schema():
    """test_json_schema"""

    # Check a fail from the schema validator
    jsv = STACValidator(schema_filename="item.json")
    invalid_filename = "test/fixtures/CBERS_4_MUX_20170528_090_084_L2_error.json"
    with pytest.raises(ValidationError) as context:
        jsv.validate(invalid_filename)
    assert "'links' is a required property" in str(context)
