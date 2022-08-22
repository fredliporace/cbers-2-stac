"""Converts CBERS-4/4A and AMAZONIA1 scene metadata (xml) to stac item"""

import json
import os
import re
import statistics
import sys
import typing
import xml.etree.ElementTree as ET
from collections import OrderedDict
from typing import List

import utm

from cbers2stac.layers.common.utils import (
    BASE_CAMERA,
    CBERS_AM_MISSIONS,
    STAC_VERSION,
    build_absolute_prefix,
    build_collection_name,
)

TIF_XML_REGEX = re.compile(
    r"(?P<satellite>\w+)_(?P<mission>\w+)_(?P<camera>\w+)_"
    r"(?P<date>\d{8})_(?P<path>\d{3})_(?P<row>\d{3})_"
    r"(?P<level>[^\W_]+)(?P<optics>_LEFT|_RIGHT)?_"
    r"BAND(?P<band>\d+)\.(tif|xml)"
)


def epsg_from_utm_zone(zone):
    """
    Returns the WGS-84 EPSG for a given UTM zone
    Input:
    zone(int): Zone, positive values for North
    Output:
    epsg code(int).
    """

    if zone > 0:
        epsg = 32600 + zone
    else:
        epsg = 32700 - zone
    return epsg


@typing.no_type_check
def get_keys_from_cbers_am(  # pylint: disable=too-many-statements,too-many-locals
    cb_am_metadata: str,
):
    """
    Input:
    cb_am_metadata: CBERS/AM metadata file location
    Output:
    dict: Dictionary with stac information
    """

    nsp = {"x": "http://www.gisplan.com.br/xmlsat"}
    metadata = {}

    match = TIF_XML_REGEX.match(cb_am_metadata.split("/")[-1])
    assert match, f"Can't match {cb_am_metadata}"

    tree = ET.parse(cb_am_metadata)
    original_root = tree.getroot()

    # satellite node information, checking for CBERS-04A/AMAZONIA1 WFI
    # special case
    left_root = original_root.find("x:leftCamera", nsp)
    if left_root:
        right_root = original_root.find("x:rightCamera", nsp)
        # We use the left camera for fields that are not camera
        # specific or are not used for STAC fields computation
        root = left_root
    else:
        root = original_root

    satellite = root.find("x:satellite", nsp)

    metadata["mission"] = satellite.find("x:name", nsp).text
    metadata["number"] = satellite.find("x:number", nsp).text
    metadata["sensor"] = satellite.find("x:instrument", nsp).text
    metadata["collection"] = build_collection_name(
        satellite=metadata["mission"],
        mission=metadata["number"],
        camera=metadata["sensor"],
    )
    metadata["optics"] = (
        match.groupdict()["optics"] if match.groupdict()["optics"] else ""
    )

    # image node information
    image = root.find("x:image", nsp)
    metadata["path"] = image.find("x:path", nsp).text
    metadata["row"] = image.find("x:row", nsp).text
    metadata["processing_level"] = image.find("x:level", nsp).text
    metadata["vertical_pixel_size"] = image.find("x:verticalPixelSize", nsp).text
    metadata["horizontal_pixel_size"] = image.find("x:horizontalPixelSize", nsp).text
    metadata["projection_name"] = image.find("x:projectionName", nsp).text
    metadata["origin_latitude"] = image.find("x:originLatitude", nsp).text
    metadata["origin_longitude"] = image.find("x:originLongitude", nsp).text

    imagedata = image.find("x:imageData", nsp)
    metadata["ul_lat"] = imagedata.find("x:UL", nsp).find("x:latitude", nsp).text
    metadata["ul_lon"] = imagedata.find("x:UL", nsp).find("x:longitude", nsp).text
    metadata["ur_lat"] = imagedata.find("x:UR", nsp).find("x:latitude", nsp).text
    metadata["ur_lon"] = imagedata.find("x:UR", nsp).find("x:longitude", nsp).text
    metadata["lr_lat"] = imagedata.find("x:LR", nsp).find("x:latitude", nsp).text
    metadata["lr_lon"] = imagedata.find("x:LR", nsp).find("x:longitude", nsp).text
    metadata["ll_lat"] = imagedata.find("x:LL", nsp).find("x:latitude", nsp).text
    metadata["ll_lon"] = imagedata.find("x:LL", nsp).find("x:longitude", nsp).text
    metadata["ct_lat"] = imagedata.find("x:CT", nsp).find("x:latitude", nsp).text
    metadata["ct_lon"] = imagedata.find("x:CT", nsp).find("x:longitude", nsp).text

    boundingbox = image.find("x:boundingBox", nsp)
    metadata["bb_ul_lat"] = boundingbox.find("x:UL", nsp).find("x:latitude", nsp).text
    metadata["bb_ul_lon"] = boundingbox.find("x:UL", nsp).find("x:longitude", nsp).text
    metadata["bb_ur_lat"] = boundingbox.find("x:UR", nsp).find("x:latitude", nsp).text
    metadata["bb_ur_lon"] = boundingbox.find("x:UR", nsp).find("x:longitude", nsp).text
    metadata["bb_lr_lat"] = boundingbox.find("x:LR", nsp).find("x:latitude", nsp).text
    metadata["bb_lr_lon"] = boundingbox.find("x:LR", nsp).find("x:longitude", nsp).text
    metadata["bb_ll_lat"] = boundingbox.find("x:LL", nsp).find("x:latitude", nsp).text
    metadata["bb_ll_lon"] = boundingbox.find("x:LL", nsp).find("x:longitude", nsp).text

    sun_position = image.find("x:sunPosition", nsp)
    metadata["sun_elevation"] = sun_position.find("x:elevation", nsp).text
    metadata["sun_azimuth"] = sun_position.find("x:sunAzimuth", nsp).text

    if left_root:
        # Update fields for CB04A / AMAZONIA WFI special case
        lidata = left_root.find("x:image", nsp).find("x:imageData", nsp)
        ridata = right_root.find("x:image", nsp).find("x:imageData", nsp)
        metadata["ur_lat"] = ridata.find("x:UR", nsp).find("x:latitude", nsp).text
        metadata["ur_lon"] = ridata.find("x:UR", nsp).find("x:longitude", nsp).text
        metadata["lr_lat"] = ridata.find("x:LR", nsp).find("x:latitude", nsp).text
        metadata["lr_lon"] = ridata.find("x:LR", nsp).find("x:longitude", nsp).text
        metadata["ct_lat"] = str(
            statistics.mean(
                [
                    float(lidata.find("x:CT", nsp).find("x:latitude", nsp).text),
                    float(ridata.find("x:CT", nsp).find("x:latitude", nsp).text),
                ]
            )
        )
        metadata["ct_lon"] = str(
            statistics.mean(
                [
                    float(lidata.find("x:CT", nsp).find("x:longitude", nsp).text),
                    float(ridata.find("x:CT", nsp).find("x:longitude", nsp).text),
                ]
            )
        )

        spleft = left_root.find("x:image", nsp).find("x:sunPosition", nsp)
        spright = right_root.find("x:image", nsp).find("x:sunPosition", nsp)

        metadata["sun_elevation"] = str(
            statistics.mean(
                [
                    float(spleft.find("x:elevation", nsp).text),
                    float(spright.find("x:elevation", nsp).text),
                ]
            )
        )

        metadata["sun_azimuth"] = str(
            statistics.mean(
                [
                    float(spleft.find("x:sunAzimuth", nsp).text),
                    float(spright.find("x:sunAzimuth", nsp).text),
                ]
            )
        )

        bbleft = left_root.find("x:image", nsp).find("x:boundingBox", nsp)
        bbright = right_root.find("x:image", nsp).find("x:boundingBox", nsp)

        metadata["bb_ll_lat"] = str(
            min(
                float(bbleft.find("x:LL", nsp).find("x:latitude", nsp).text),
                float(bbright.find("x:LL", nsp).find("x:latitude", nsp).text),
            )
        )
        metadata["bb_ll_lon"] = str(
            min(
                float(bbleft.find("x:LL", nsp).find("x:longitude", nsp).text),
                float(bbright.find("x:LL", nsp).find("x:longitude", nsp).text),
            )
        )

        metadata["bb_ur_lat"] = str(
            max(
                float(bbleft.find("x:UR", nsp).find("x:latitude", nsp).text),
                float(bbright.find("x:UR", nsp).find("x:latitude", nsp).text),
            )
        )
        metadata["bb_ur_lon"] = str(
            max(
                float(bbleft.find("x:UR", nsp).find("x:longitude", nsp).text),
                float(bbright.find("x:UR", nsp).find("x:longitude", nsp).text),
            )
        )

    # attitude node information
    attitudes = image.find("x:attitudes", nsp)
    for attitude in attitudes.findall("x:attitude", nsp):
        metadata["roll"] = attitude.find("x:roll", nsp).text
        break

    # ephemeris node information
    ephemerides = image.find("x:ephemerides", nsp)
    for ephemeris in ephemerides.findall("x:ephemeris", nsp):
        metadata["vz"] = ephemeris.find("x:vz", nsp).text
        break

    # availableBands node information
    available_bands = root.find("x:availableBands", nsp)
    metadata["bands"] = []
    for band in available_bands.findall("x:band", nsp):
        metadata["bands"].append(band.text)
        key = f"band_{band.text}_gain"
        metadata[key] = band.attrib.get("gain")

    # viewing node information
    viewing = root.find("x:viewing", nsp)
    metadata["acquisition_date"] = viewing.find("x:center", nsp).text.replace("T", " ")
    metadata["acquisition_day"] = metadata["acquisition_date"].split(" ")[0]

    # derived fields
    metadata["no_level_id"] = (
        f"{metadata['mission']}_{metadata['number']}_{metadata['sensor']}_"
        f"{metadata['acquisition_day'].replace('-', '')}_"
        f"{int(metadata['path']):03d}_{int(metadata['row']):03d}"
    )

    # example: CBERS4/MUX/071/092/CBERS_4_MUX_20171105_071_092_L2
    metadata["download_url"] = (
        "%s%s/"
        "%s/"
        "%03d/%03d/"
        "%s"
        % (
            metadata["mission"],
            metadata["number"],
            metadata["sensor"],
            int(metadata["path"]),
            int(metadata["row"]),
            re.sub(
                r"(_LEFT|_RIGHT)?_BAND\d+.xml", "", os.path.basename(cb_am_metadata)
            ),
        )
    )
    metadata[
        "sat_sensor"
    ] = f"{metadata['mission']}{metadata['number']}/{metadata['sensor']}"
    metadata["sat_number"] = f"{metadata['mission']}-{metadata['number']}"
    metadata["meta_file"] = os.path.basename(cb_am_metadata)

    return metadata


def build_link(rel, href):
    """
    Build a rel, href link object
    """
    link = OrderedDict()
    link["rel"] = rel
    link["href"] = href
    return link


def build_asset(
    href, sat_number, title=None, asset_type=None, band_id=None, properties=None
):  # pylint: disable=too-many-arguments
    """
    Build a asset entry
    """
    asset = OrderedDict()
    asset["href"] = href
    if title:
        asset["title"] = title
    if asset_type:
        asset["type"] = asset_type
    if properties:
        for key in properties:
            asset[key] = properties[key]
    if band_id is not None:
        eo_band = OrderedDict()
        eo_band["name"] = band_id
        eo_band["common_name"] = CBERS_AM_MISSIONS[sat_number]["band"][band_id][
            "common_name"
        ]
        asset["eo:bands"] = [eo_band]
    return asset


def build_stac_item_keys(cbers_am, buckets):
    """
    Builds a STAC item dict based on CBERS_AM metadata

    Input:
    cbers_am(dict): CBERS_AM metadata
    buckets(dict): buckets identification
    """

    stac_item = OrderedDict()

    stac_item["stac_version"] = STAC_VERSION
    stac_item["stac_extensions"] = [
        "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
        "https://stac-extensions.github.io/view/v1.0.0/schema.json",
        "https://stac-extensions.github.io/eo/v1.0.0/schema.json",
        "https://stac-extensions.github.io/sat/v1.0.0/schema.json",
    ]
    stac_item["id"] = (
        "%s_%s_%s_%s_"  # pylint: disable=consider-using-f-string
        "%03d_%03d_L%s"
        % (
            cbers_am["mission"],
            cbers_am["number"],
            cbers_am["sensor"],
            cbers_am["acquisition_day"].replace("-", ""),
            int(cbers_am["path"]),
            int(cbers_am["row"]),
            cbers_am["processing_level"],
        )
    )

    stac_item["type"] = "Feature"

    stac_item["geometry"] = OrderedDict()
    stac_item["geometry"]["type"] = "MultiPolygon"
    stac_item["geometry"]["coordinates"] = [
        [
            [
                (float(cbers_am["ll_lon"]), float(cbers_am["ll_lat"])),
                (float(cbers_am["lr_lon"]), float(cbers_am["lr_lat"])),
                (float(cbers_am["ur_lon"]), float(cbers_am["ur_lat"])),
                (float(cbers_am["ul_lon"]), float(cbers_am["ul_lat"])),
                (float(cbers_am["ll_lon"]), float(cbers_am["ll_lat"])),
            ]
        ]
    ]

    # Order is lower left lon, lat; upper right lon, lat
    stac_item["bbox"] = (
        float(cbers_am["bb_ll_lon"]),
        float(cbers_am["bb_ll_lat"]),
        float(cbers_am["bb_ur_lon"]),
        float(cbers_am["bb_ur_lat"]),
    )

    # Collection
    stac_item["collection"] = cbers_am["collection"]

    stac_item["properties"] = OrderedDict()
    datetime = cbers_am["acquisition_date"].replace(" ", "T")
    datetime = re.sub(r"\.\d+", "Z", datetime)
    stac_item["properties"]["datetime"] = datetime

    # Common metadata
    stac_item["properties"]["platform"] = cbers_am["sat_number"].lower()
    stac_item["properties"]["instruments"] = [cbers_am["sensor"]]
    stac_item["properties"]["gsd"] = BASE_CAMERA[
        f"{cbers_am['mission']}{cbers_am['number']}"
    ][cbers_am["sensor"]]["summaries"]["gsd"][0]

    # Links
    meta_prefix = f"https://s3.amazonaws.com/{buckets['metadata']}/"
    main_prefix = f"s3://{buckets['cog']}/"
    stac_prefix = f"https://{buckets['stac']}.s3.amazonaws.com/"
    # https://s3.amazonaws.com/cbers-meta-pds/CBERS4/MUX/066/096/CBERS_4_MUX_20170522_066_096_L2/CBERS_4_MUX_20170522_066_096.jpg
    stac_item["links"] = []

    # links, self
    stac_item["links"].append(
        build_link(
            "self",
            build_absolute_prefix(
                buckets["stac"],
                cbers_am["sat_sensor"],
                int(cbers_am["path"]),
                int(cbers_am["row"]),
            )
            + stac_item["id"]
            + ".json",
        )
    )

    # links, parent
    stac_item["links"].append(
        build_link(
            "parent",
            build_absolute_prefix(
                buckets["stac"],
                cbers_am["sat_sensor"],
                int(cbers_am["path"]),
                int(cbers_am["row"]),
            )
            + "catalog.json",
        )
    )

    # link, collection
    stac_item["links"].append(
        build_link(
            rel="collection",
            href=stac_prefix
            + cbers_am["mission"]
            + cbers_am["number"]
            + "/"
            + cbers_am["sensor"]
            + "/collection.json",
        )
    )

    # EO section
    # Missing fields (not available from CBERS metadata)
    # eo:cloud_cover

    # VIEW extension
    stac_item["properties"]["view:sun_azimuth"] = float(cbers_am["sun_azimuth"])
    stac_item["properties"]["view:sun_elevation"] = float(cbers_am["sun_elevation"])
    stac_item["properties"]["view:off_nadir"] = abs(float(cbers_am["roll"]))

    # PROJECTION extension
    assert cbers_am["projection_name"] == "UTM", (
        "Unsupported projection " + cbers_am["projection_name"]
    )
    utm_zone = int(
        utm.from_latlon(float(cbers_am["ct_lat"]), float(cbers_am["ct_lon"]))[2]
    )
    if float(cbers_am["ct_lat"]) < 0.0:
        utm_zone *= -1
    stac_item["properties"]["proj:epsg"] = int(epsg_from_utm_zone(utm_zone))

    # SATELLITE extension
    stac_item["properties"][
        "sat:platform_international_designator"
    ] = CBERS_AM_MISSIONS[cbers_am["sat_number"]]["international_designator"]
    stac_item["properties"]["sat:orbit_state"] = (
        "descending" if float(cbers_am["vz"]) < 0 else "ascending"
    )

    # CBERS section
    stac_item["properties"][f"{cbers_am['mission'].lower()}:data_type"] = (
        "L" + cbers_am["processing_level"]
    )
    stac_item["properties"][f"{cbers_am['mission'].lower()}:path"] = int(
        cbers_am["path"]
    )
    stac_item["properties"][f"{cbers_am['mission'].lower()}:row"] = int(cbers_am["row"])

    # Assets
    stac_item["assets"] = OrderedDict()
    stac_item["assets"]["thumbnail"] = build_asset(
        meta_prefix
        + cbers_am["download_url"]
        + "/"
        + cbers_am["no_level_id"]
        + "."
        + CBERS_AM_MISSIONS[cbers_am["sat_number"]]["quicklook"]["extension"],
        cbers_am["sat_number"],
        asset_type="image/"
        + CBERS_AM_MISSIONS[cbers_am["sat_number"]]["quicklook"]["type"],
    )

    stac_item["assets"]["metadata"] = build_asset(
        main_prefix + cbers_am["download_url"] + "/" + cbers_am["meta_file"],
        cbers_am["sat_number"],
        asset_type="text/xml",
        title="INPE original metadata",
    )
    for band in cbers_am["bands"]:
        band_id = "B" + band
        gsd = CBERS_AM_MISSIONS[cbers_am["sat_number"]]["band"][band_id].get("gsd")
        if gsd:
            properties = {"gsd": gsd}
        else:
            properties = None
        stac_item["assets"][band_id] = build_asset(
            main_prefix
            + cbers_am["download_url"]
            + "/"
            + stac_item["id"]
            + cbers_am["optics"]
            + "_BAND"
            + band
            + ".tif",
            cbers_am["sat_number"],
            asset_type="image/tiff; application=geotiff; " "profile=cloud-optimized",
            band_id=band_id,
            properties=properties,
        )
    return stac_item


def create_json_item(stac_item, filename: str) -> None:
    """
    Dumps STAC item into json file
    Input:
    stac_item(dict): stac item dictionary
    filename(string): ditto
    """

    with open(filename, "w", encoding="utf-8") as outfile:
        json.dump(stac_item, outfile, indent=2)


def candidate_xml_files(xml_file: str) -> List[str]:
    """
    Return a list of candidate names for xml files, including
    optics for Amazonia-1 xml files.

    Args:
      xml_file: XML filename
    Return:
      List with options for XML filenames.
    """
    match = TIF_XML_REGEX.match(xml_file.split("/")[-1])
    assert match, f"Can't match {xml_file}"
    group = match.groupdict()
    xml_options = []
    if group["satellite"] == "AMAZONIA":
        for optics in ["", "_LEFT", "_RIGHT"]:
            xml_options.append(re.sub(r"_L(\d+)_", f"_L\\g<1>{optics}_", xml_file))
    else:
        xml_options.append(xml_file)
    return xml_options


def convert_inpe_to_stac(inpe_metadata_filename, stac_metadata_filename, buckets):
    """
    Generate STAC item in stac_metadata from inpe_metadata.

    Input:
    inpe_metadata(string): CBERS metadata (INPE format) file
    stac_metadata(string): STAC item metadata file to be written, if None then
                           no file is written.
    buckets: buckets dictionary

    Return:
    Dictionary based on INPE's metadata
    """

    meta = get_keys_from_cbers_am(inpe_metadata_filename)
    stac_meta = build_stac_item_keys(meta, buckets)
    if stac_metadata_filename:
        create_json_item(stac_meta, stac_metadata_filename)
    return stac_meta


if __name__ == "__main__":
    # Command line arguments
    # inpe_metadata filename (1)
    # stac_metadata filename (2)

    BUCKETS = {"metadata": "cbers-meta-pds", "cog": "cbers-pds", "stac": "cbers-stac"}

    assert sys.argv == 3
    convert_inpe_to_stac(
        inpe_metadata_filename=sys.argv[1],
        stac_metadata_filename=sys.argv[2],
        buckets=BUCKETS,
    )
