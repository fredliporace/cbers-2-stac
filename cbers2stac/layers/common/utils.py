"""
utils.py
Definitions for base item, catalog and collections
"""

import copy
import os
from collections import OrderedDict
from typing import Any, Dict, List

import boto3

# TODO: This is a singleton, check for more elegant, pythonic way # pylint: disable=fixme
# Dictionary for aws clients, service is the key
CLIENT = dict()  # type: dict
RESOURCE = dict()  # type: dict


def get_client(service: str) -> boto3.client:
    """
    Create localstack or production client

    service is the AWS service identification, "sqs", "s3", etc.
    """

    global CLIENT  #  pylint: disable=global-statement
    if not CLIENT.get(service):
        if os.environ.get("LOCALSTACK_HOSTNAME"):
            CLIENT[service] = boto3.client(
                service,
                endpoint_url="http://{}:4566".format(os.environ["LOCALSTACK_HOSTNAME"]),
            )
        else:
            CLIENT[service] = boto3.client(service)
    return CLIENT[service]


def get_resource(service: str) -> boto3.client:
    """
    Create localstack or production resource

    service is the AWS service identification, "sqs", "s3", etc.
    """

    global RESOURCE  #  pylint: disable=global-statement
    if not RESOURCE.get(service):
        if os.environ.get("LOCALSTACK_HOSTNAME"):
            RESOURCE[service] = boto3.resource(
                service,
                endpoint_url="http://{}:4566".format(os.environ["LOCALSTACK_HOSTNAME"]),
            )
        else:
            RESOURCE[service] = boto3.resource(service)
    return RESOURCE[service]


# Collections are currently hard-coded here, this is not
# an issue for CBERS on AWS since this does not frequently change
COLLECTIONS = {
    "CBERS4-MUX": "CBERS4/MUX/collection.json",
    "CBERS4-AWFI": "CBERS4/AWFI/collection.json",
    "CBERS4-PAN10M": "CBERS4/PAN10M/collection.json",
    "CBERS4-PAN5M": "CBERS4/PAN5M/collection.json",
    "CBERS4A-MUX": "CBERS4A/MUX/collection.json",
    "CBERS4A-WFI": "CBERS4A/WFI/collection.json",
    "CBERS4A-WPM": "CBERS4A/WPM/collection.json",
}

STAC_VERSION = "1.0.0-rc.2"

BASE_CATALOG = OrderedDict(
    {"stac_version": STAC_VERSION, "id": None, "description": None,}
)

# Base template for root documents
STAC_DOC_TEMPLATE: Dict[str, Any] = {
    "stac_version": None,
    "id": "CBERS",
    "description": "Catalogs of CBERS 4 & 4A mission's imagery on AWS",
    "title": "CBERS 4 & CBERS 4A on AWS",
    "links": [],
}

COG_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"

# CBERS general missions definitions
# This requires Python3.8
# class Mission(TypedDict):  # pylint: disable=too-few-public-methods
#     """
#     Mission parameters
#     """

#     interval: List[List]
#     quicklook: Dict[str, str]
#     instruments: List[str]
#     band: Dict[str, Dict[str, str]]


# CBERS_MISSIONS: Dict[str, Mission] = {
CBERS_MISSIONS: Dict[str, Any] = {
    "CBERS-4": {
        "interval": [["2014-12-08T00:00:00Z", None]],
        "quicklook": {"extension": "jpg", "type": "jpeg"},
        "instruments": ["MUX", "AWFI", "PAN5M", "PAN10M"],
        "band": {
            "B1": {"common_name": "pan"},
            "B2": {"common_name": "green"},
            "B3": {"common_name": "red"},
            "B4": {"common_name": "nir"},
            "B5": {"common_name": "blue"},
            "B6": {"common_name": "green"},
            "B7": {"common_name": "red"},
            "B8": {"common_name": "nir"},
            "B13": {"common_name": "blue"},
            "B14": {"common_name": "green"},
            "B15": {"common_name": "red"},
            "B16": {"common_name": "nir"},
        },
    },
    "CBERS-4A": {
        "interval": [["2019-12-20T00:00:00Z", None]],
        "quicklook": {"extension": "png", "type": "png"},
        "instruments": ["WPM", "MUX", "WFI"],
        "band": {
            "B0": {"common_name": "pan",},
            "B1": {
                # gsd is only defined for values greater than
                # what is defined at collection level
                "common_name": "blue",
                "gsd": 8,
            },
            "B2": {"common_name": "green", "gsd": 8},
            "B3": {"common_name": "red", "gsd": 8},
            "B4": {"common_name": "nir", "gsd": 8},
            "B5": {"common_name": "blue"},
            "B6": {"common_name": "green"},
            "B7": {"common_name": "red"},
            "B8": {"common_name": "nir"},
            "B13": {"common_name": "blue"},
            "B14": {"common_name": "green"},
            "B15": {"common_name": "red"},
            "B16": {"common_name": "nir"},
        },
    },
}

# Ugh...using this while there are accesses as both CBERS-4
# and CBERS4, refactor and unify keys...someday
# Accesses shold be always using SATELLITE-MISSION
# One approach to do that is to encapsulate all globals within
# this module and only allow access through functions such
# as get_satmissions()
CBERS_MISSIONS["CBERS4"] = CBERS_MISSIONS["CBERS-4"]
CBERS_MISSIONS["CBERS4A"] = CBERS_MISSIONS["CBERS-4A"]

BASE_COLLECTION = OrderedDict(
    {
        "stac_extensions": [
            "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json"
        ],
        "license": "CC-BY-SA-3.0",
        "providers": [
            {
                "name": "Instituto Nacional de Pesquisas Espaciais, INPE",
                "roles": ["producer"],
                "url": "http://www.cbers.inpe.br",
            },
            {
                "name": "AMS Kepler",
                "roles": ["processor"],
                "description": "Convert INPE's original TIFF to COG "
                "and copy to Amazon Web Services",
                "url": "https://github.com/fredliporace/cbers-on-aws",
            },
            {
                "name": "Amazon Web Services",
                "roles": ["host"],
                "url": "https://registry.opendata.aws/cbers/",
            },
        ],
        "extent": {
            "spatial": {"bbox": [[-180.0, -83.0, 180.0, 83.0]],},
            "temporal": {"interval": None},
        },
        "links": None,
        "properties": {"gsd": None, "platform": None, "instruments": None,},
        "item_assets": None,
    }
)

BASE_CAMERA = {
    "CBERS4": {
        "MUX": {
            "properties": {"gsd": 20.0, "platform": "CBERS-4", "instruments": ["MUX"],},
            "item_assets": {
                "thumbnail": {"title": "Thumbnail", "type": "image/jpeg"},
                "metadata": {"title": "INPE original metadata", "type": "text/xml"},
                "B5": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B5", "common_name": "blue"}],
                },
                "B6": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B6", "common_name": "green"}],
                },
                "B7": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B7", "common_name": "red"}],
                },
                "B8": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B8", "common_name": "nir"}],
                },
            },
        },
        "AWFI": {
            "properties": {
                "gsd": 64.0,
                "platform": "CBERS-4",
                "instruments": ["AWFI"],
            },
            "item_assets": {
                "thumbnail": {"title": "Thumbnail", "type": "image/jpeg"},
                "metadata": {"title": "INPE original metadata", "type": "text/xml"},
                "B13": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B13", "common_name": "blue"}],
                },
                "B14": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B14", "common_name": "green"}],
                },
                "B15": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B15", "common_name": "red"}],
                },
                "B16": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B16", "common_name": "nir"}],
                },
            },
        },
        "PAN5M": {
            "properties": {
                "gsd": 5.0,
                "platform": "CBERS-4",
                "instruments": ["PAN5M"],
            },
            "item_assets": {
                "thumbnail": {"title": "Thumbnail", "type": "image/jpeg"},
                "metadata": {"title": "INPE original metadata", "type": "text/xml"},
                "B1": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B1", "common_name": "pan"}],
                },
            },
        },
        "PAN10M": {
            "properties": {
                "gsd": 10.0,
                "platform": "CBERS-4",
                "instruments": ["PAN10M"],
            },
            "item_assets": {
                "thumbnail": {"title": "Thumbnail", "type": "image/jpeg"},
                "metadata": {"title": "INPE original metadata", "type": "text/xml"},
                "B2": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B2", "common_name": "green"}],
                },
                "B3": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B3", "common_name": "red"}],
                },
                "B4": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B4", "common_name": "nir"}],
                },
            },
        },
    },
    "CBERS4A": {
        "MUX": {
            "properties": {
                "gsd": 16.5,
                "platform": "CBERS-4A",
                "instruments": ["MUX"],
            },
            "item_assets": {
                "thumbnail": {"title": "Thumbnail", "type": "image/png"},
                "metadata": {"title": "INPE original metadata", "type": "text/xml"},
                "B5": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B5", "common_name": "blue"}],
                },
                "B6": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B6", "common_name": "green"}],
                },
                "B7": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B7", "common_name": "red"}],
                },
                "B8": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B8", "common_name": "nir"}],
                },
            },
        },
        "WFI": {
            "properties": {
                "gsd": 55.0,
                "platform": "CBERS-4A",
                "instruments": ["WFI"],
            },
            "item_assets": {
                "thumbnail": {"title": "Thumbnail", "type": "image/png"},
                "metadata": {"title": "INPE original metadata", "type": "text/xml"},
                "B13": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B13", "common_name": "blue"}],
                },
                "B14": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B14", "common_name": "green"}],
                },
                "B15": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B15", "common_name": "red"}],
                },
                "B16": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B16", "common_name": "nir"}],
                },
            },
        },
        "WPM": {
            "properties": {"gsd": 2.0, "platform": "CBERS-4A", "instruments": ["WPM"],},
            "item_assets": {
                "thumbnail": {"title": "Thumbnail", "type": "image/png"},
                "metadata": {"title": "INPE original metadata", "type": "text/xml"},
                "B0": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B0", "common_name": "pan"}],
                },
                "B1": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B1", "common_name": "blue"}],
                },
                "B2": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B2", "common_name": "green"}],
                },
                "B3": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B3", "common_name": "red"}],
                },
                "B4": {
                    "type": COG_TYPE,
                    "eo:bands": [{"name": "B4", "common_name": "nir"}],
                },
            },
        },
    },
}


def build_collection_name(satellite: str, camera: str, mission: str = None):
    """
    Centralized method to build collection names. If mission is absent
    then we assume that it is already concatenated with satellite
    """
    if not mission:
        return "{}-{}".format(satellite, camera)
    return "{}{}-{}".format(satellite, mission, camera)


def static_to_api_collection(collection: dict, event: dict):
    """
    Transform static collection objects to API collection by
    - removing all 'parent' and 'child' links

    :param collection dict: input static collection
    :param event dict: API gateway lambda event struct
    :rtype: dict
    :return: API collection
    """

    parsed = parse_api_gateway_event(event)
    # collection['links'] = [v for v in collection['links'] \
    #                       if v['rel'] != 'child' and v['rel'] != 'parent']
    collection["links"] = list()
    collection["links"].append(
        {
            "rel": "self",
            "href": "{phost}/{prefix}/collections/{cid}".format(
                phost=parsed["phost"], prefix=parsed["prefix"], cid=collection["id"]
            ),
        }
    )
    collection["links"].append({"rel": "parent", "href": parsed["spath"]})
    collection["links"].append({"rel": "root", "href": parsed["spath"]})
    collection["links"].append(
        {
            "rel": "items",
            "href": "{phost}/{prefix}/collections/{cid}/items".format(
                phost=parsed["phost"], prefix=parsed["prefix"], cid=collection["id"]
            ),
        }
    )

    return collection


def parse_api_gateway_event(event: dict):
    """
    Extract fields from API gateway event and place them
    in a dictionary

    :param event dict: API gateway lambda event struct
    :rtype: dict
    :return: dict with extracted fields, for example
             phost: https://stac.amskepler.com
             ppath: https://stac.amskepler.com/v07/stac (access path)
             ppath: https://stac.amskepler.com/v07/stac (fixed STAC root)
             vpath: https://stac.amskepler.com/v07
             prefix: v07
    """

    parsed = dict()
    parsed["phost"] = "{protocol}://{host}".format(
        protocol=event["headers"]["X-Forwarded-Proto"], host=event["headers"]["Host"]
    )
    parsed["ppath"] = "{phost}{path}".format(phost=parsed["phost"], path=event["path"])
    parsed["prefix"] = event["path"].split("/")[1]
    parsed["vpath"] = "{phost}/{prefix}".format(
        phost=parsed["phost"], prefix=parsed["prefix"]
    )
    parsed["spath"] = "{phost}/{prefix}/stac".format(
        phost=parsed["phost"], prefix=parsed["prefix"]
    )
    return parsed


def get_api_stac_root(event: dict):
    """
    Return STAC api root document

    :param event dict: lambda event struct
    :rtype: dict
    :return: STAC root document as dict
    """

    doc = copy.deepcopy(STAC_DOC_TEMPLATE)
    parsed = parse_api_gateway_event(event)
    doc["stac_version"] = STAC_VERSION
    doc["links"].append({"self": parsed["ppath"]})
    for collection in COLLECTIONS:
        doc["links"].append(
            {
                "child": "{phost}/{prefix}/collections/{collection}".format(
                    phost=parsed["phost"],
                    prefix=parsed["prefix"],
                    collection=collection,
                )
            }
        )
    return doc


def build_absolute_prefix(bucket, sat_sensor=None, path=None, row=None):
    """Returns the absolute prefix given the bucket/satellite/path/row"""

    prefix = "https://%s.s3.amazonaws.com" % bucket
    if sat_sensor:
        prefix += "/" + sat_sensor
    if path:
        prefix += "/%03d" % int(path)
    if row:
        prefix += "/%03d" % int(row)
    prefix += "/"
    return prefix


def get_collection_ids():
    """Return list of collection ids"""
    return COLLECTIONS.keys()


def get_collection_s3_key(collection_id: str):
    """
    Prefix for a given collection id

    :param collection_id str: ditto
    :rtype: str
    :return: bucket key
    """
    return COLLECTIONS[collection_id]


def get_collections_for_satmission(satellite: str, mission: str) -> List[str]:
    """
    Returns all collections for a given satellite mission, e.g., CBERS-4
    """
    return CBERS_MISSIONS[f"{satellite}-{mission}"]["instruments"]


def get_satmissions(use_hyphen: bool) -> List[str]:
    """
    Return all supported satmissions.
    Hyphens are used or not depending on use_hyphen parameter
    """
    ret: List[str] = list()
    for satmission in CBERS_MISSIONS:
        if "-" not in satmission:
            continue
        if use_hyphen:
            ret.append(satmission)
        else:
            satellite = satmission.split("-")[0]
            mission = satmission.split("-")[1]
            ret.append(f"{satellite}{mission}")
    return ret
