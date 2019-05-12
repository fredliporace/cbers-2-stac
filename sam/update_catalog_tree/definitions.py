"""Definitions for base catalog and collections"""

from collections import OrderedDict

STAC_VERSION = "0.7.0"

BASE_CATALOG = OrderedDict({
    "stac_version": STAC_VERSION,
    "id": None,
    "description": None,
})

BASE_COLLECTION = OrderedDict({
    "stac_version": STAC_VERSION,
    "id": None,
    "title": None,
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
        "spatial": [
            -180.0,
            -83.0,
            180.0,
            83.0
        ],
        "temporal": [
            "2014-12-08T00:00:00Z",
            None
        ]
    },
    "links": None,
    "properties": {
        "eo:gsd": None,
        "eo:platform": None,
        "eo:instrument": None,
        "eo:bands": None
    }
})


