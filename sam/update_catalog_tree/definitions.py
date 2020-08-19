"""Definitions for base catalog and collections"""

from collections import OrderedDict

STAC_VERSION = "1.0.0-beta.2"

BASE_CATALOG = OrderedDict({
    "stac_version": STAC_VERSION,
    "id": None,
    "description": None,
})

BASE_COLLECTION = OrderedDict({
    "stac_extensions": [
        "eo",
        "item-assets"
    ],
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
        "gsd": None,
        "platform": None,
        "instruments": None,
    },
    "item_assets": None
})

BASE_CAMERA = {
    "MUX":{
        "properties": {
            "gsd": 20.0,
            "platform": "CBERS-4",
            "instruments": ["MUX"],
        },
        "item_assets":{
            "thumbnail": {
                "title": "Thumbnail",
                "type": "image/jpeg"
            },
            "metadata": {
                "title": "INPE original metadata",
                "type": "text/xml"
            },
            "B5": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "eo:bands": [
                    {
                        "name": "B5",
                        "common_name": "blue"
                    }
                ]
            },
            "B6": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "eo:bands": [
                    {
                        "name": "B6",
                        "common_name": "green"
                    }
                ]
            },
            "B7": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "eo:bands": [
                    {
                        "name": "B7",
                        "common_name": "red"
                    }
                ]
            },
            "B8": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "eo:bands": [
                    {
                        "name": "B8",
                        "common_name": "nir"
                    }
                ]
            }
        }
    },
    "AWFI":{
        "properties": {
            "gsd": 64.0,
            "platform": "CBERS-4",
            "instruments": ["AWFI"],
        },
        "item_assets":{
            "thumbnail": {
                "title": "Thumbnail",
                "type": "image/jpeg"
            },
            "metadata": {
                "title": "INPE original metadata",
                "type": "text/xml"
            },
            "B13": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "eo:bands": [
                    {
                        "name": "B13",
                        "common_name": "blue"
                    }
                ]
            },
            "B14": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "eo:bands": [
                    {
                        "name": "B14",
                        "common_name": "green"
                    }
                ]
            },
            "B15": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "eo:bands": [
                    {
                        "name": "B15",
                        "common_name": "red"
                    }
                ]
            },
            "B16": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "eo:bands": [
                    {
                        "name": "B16",
                        "common_name": "nir"
                    }
                ]
            }
        }
    },
    "PAN5M":{
        "properties": {
            "gsd": 5.0,
            "platform": "CBERS-4",
            "instruments": ["PAN5M"],
        },
        "item_assets":{
            "thumbnail": {
                "title": "Thumbnail",
                "type": "image/jpeg"
            },
            "metadata": {
                "title": "INPE original metadata",
                "type": "text/xml"
            },
            "B1": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "eo:bands": [
                    {
                        "name": "B1",
                        "common_name": "pan"
                    }
                ]
            }
        }
    },
    "PAN10M":{
        "properties": {
            "gsd": 10.0,
            "platform": "CBERS-4",
            "instruments": ["PAN10M"],
        },
        "item_assets":{
            "thumbnail": {
                "title": "Thumbnail",
                "type": "image/jpeg"
            },
            "metadata": {
                "title": "INPE original metadata",
                "type": "text/xml"
            },
            "B2": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "eo:bands": [
                    {
                        "name": "B2",
                        "common_name": "green"
                    }
                ]
            },
            "B3": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "eo:bands": [
                    {
                        "name": "B3",
                        "common_name": "red"
                    }
                ]
            },
            "B4": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "eo:bands": [
                    {
                        "name": "B4",
                        "common_name": "nir"
                    }
                ]
            }
        }
    }
}
