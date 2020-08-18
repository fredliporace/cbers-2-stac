"""Definitions for base catalog and collections"""

from collections import OrderedDict

STAC_VERSION = "1.0.0-beta.2"

BASE_CATALOG = OrderedDict({
    "stac_version": STAC_VERSION,
    "id": None,
    "description": None,
})

BASE_COLLECTION = OrderedDict({
    "license": "CC-BY-SA-3.0",
    "providers": [
        {
            "name": "Instituto Nacional de Pesquisas Espaciais, INPE",
            "roles": ["producer"],
            "url": "http://www.cbers.inpe.br"
        },
        {
            "name": "AMS Kepler",
            "roles": ["processor"],
            "description": "Convert INPE's original TIFF to COG and copy to Amazon Web Services",
            "url": "https://github.com/fredliporace/cbers-on-aws"
        },
        {
            "name": "Amazon Web Services",
            "roles": ["host"],
            "url": "https://registry.opendata.aws/cbers/"
        }
    ],
    "extent": {
        "spatial": {
            "bbox": [[
                -180.0,
                -83.0,
                180.0,
                83.0
            ]],
        },
        "temporal": {
            "interval": [["2014-12-08T00:00:00Z", None]]
        }
    },
    "links": None,
    "properties": {
        "eo:gsd": None,
        "eo:platform": None,
        "eo:instrument": None,
        "eo:bands": None
    }
})

CAMERA_PROPERTIES = {
    "MUX":{
        "eo:gsd": 20.0,
        "eo:platform": "CBERS-4",
        "eo:instrument": "MUX",
        "eo:bands": [
            {
                "name": "B5",
                "common_name": "blue"
            },
            {
                "name": "B6",
                "common_name": "green"
            },
            {
                "name": "B7",
                "common_name": "red"
            },
            {
                "name": "B8",
                "common_name": "nir"
            }
        ]
    },
    "AWFI":{
        "eo:gsd": 64.0,
        "eo:platform": "CBERS-4",
        "eo:instrument": "AWFI",
        "eo:bands": [
            {
                "name": "B13",
                "common_name": "blue"
            },
            {
                "name": "B14",
                "common_name": "green"
            },
            {
                "name": "B15",
                "common_name": "red"
            },
            {
                "name": "B16",
                "common_name": "nir"
            }
        ]
    },
    "PAN5M":{
        "eo:gsd": 5.0,
        "eo:platform": "CBERS-4",
        "eo:instrument": "PAN5M",
        "eo:bands": [
            {
                "name": "B1",
                "common_name": "pan"
            }
        ]
    },
    "PAN10M":{
        "eo:gsd": 10.0,
        "eo:platform": "CBERS-4",
        "eo:instrument": "PAN10M",
        "eo:bands": [
            {
                "name": "B2",
                "common_name": "green"
            },
            {
                "name": "B3",
                "common_name": "red"
            },
            {
                "name": "B4",
                "common_name": "nir"
            }
        ]
    }
}
