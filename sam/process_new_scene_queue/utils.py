"""utils.py"""

import copy

# Collections are currently hard-coded here, this is not
# an issue for CBERS on AWS since this does not frequently change
COLLECTIONS = {
    "CBERS4MUX":"CBERS4/MUX/collection.json",
    "CBERS4AWFI":"CBERS4/AWFI/collection.json",
    "CBERS4PAN10M":"CBERS4/PAN10M/collection.json",
    "CBERS4PAN5M":"CBERS4/PAN5M/collection.json"}

# Base template for root documents
STAC_DOC_TEMPLATE = {
    "stac_version": "0.7.0",
    "id": "CBERS",
    "description": "Catalogs of CBERS mission's imagery on AWS",
    "title": "CBERS 4 on AWS",
    "links": []
}

# CBERS general missions definitions
CBERS_MISSIONS = {
    "CBERS-4": {
        "instruments": ["MUX", "AWFI", "PAN5M", "PAN10M"],
        "band": {
            "B1": {
                "common_name": "pan"
            },
            "B2": {
                "common_name": "green"
            },
            "B3": {
                "common_name": "red"
            },
            "B4": {
                "common_name": "nir"
            },
            "B5": {
                "common_name": "blue"
            },
            "B6": {
                "common_name": "green"
            },
            "B7": {
                "common_name": "red"
            },
            "B8": {
                "common_name": "nir"
            },
            "B13": {
                "common_name": "blue"
            },
            "B14": {
                "common_name": "green"
            },
            "B15": {
                "common_name": "red"
            },
            "B16": {
                "common_name": "nir"
            }
        }
    }
}

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
    #collection['links'] = [v for v in collection['links'] \
    #                       if v['rel'] != 'child' and v['rel'] != 'parent']
    collection['links'] = list()
    collection['links'].append({'rel':'self',
                                'href':'{phost}/{prefix}/collections/{cid}'.\
                                format(phost=parsed['phost'],
                                       prefix=parsed['prefix'],
                                       cid=collection['id'])})
    collection['links'].append({'rel':'parent',
                                'href':parsed['spath']})
    collection['links'].append({'rel':'root',
                                'href':parsed['spath']})
    collection['links'].append({'rel':'items',
                                'href':'{phost}/{prefix}/collections/{cid}/items'.\
                                format(phost=parsed['phost'],
                                       prefix=parsed['prefix'],
                                       cid=collection['id'])})

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
    parsed['phost'] = '{protocol}://{host}'.format(protocol=event['headers']['X-Forwarded-Proto'],
                                                   host=event['headers']['Host'])
    parsed['ppath'] = '{phost}{path}'.format(phost=parsed['phost'],
                                             path=event['path'])
    parsed['prefix'] = event['path'].split('/')[1]
    parsed['vpath'] = '{phost}/{prefix}'.format(phost=parsed['phost'],
                                                prefix=parsed['prefix'])
    parsed['spath'] = '{phost}/{prefix}/stac'.format(phost=parsed['phost'],
                                                     prefix=parsed['prefix'])
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
    doc['links'].append({"self":parsed['ppath']})
    for collection in COLLECTIONS:
        doc['links'].append({"child":'{phost}/{prefix}/collections/{collection}'.\
                             format(phost=parsed['phost'],
                                    prefix=parsed['prefix'],
                                    collection=collection)})
    return doc

def build_absolute_prefix(bucket, sat_sensor=None, path=None, row=None):
    """Returns the absolute prefix given the bucket/satellite/path/row"""

    prefix = 'https://%s.s3.amazonaws.com' % bucket
    if sat_sensor:
        prefix += '/' + sat_sensor
    if path:
        prefix += '/%03d' % int(path)
    if row:
        prefix += '/%03d' % int(row)
    prefix += '/'
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
