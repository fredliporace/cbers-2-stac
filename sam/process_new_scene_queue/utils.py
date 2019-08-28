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

def static_to_api_collection(collection: dict):
    """
    Transform static collection objects to API collection by
    - removing all 'parent' and 'child' links

    :param collection dict: input static collection
    :rtype: dict
    :return: API collection
    """
    collection['links'] = [v for v in collection['links'] \
                           if v['rel'] != 'child' and v['rel'] != 'parent']
    return collection

def get_api_stac_root(event: dict):
    """
    Return STAC api root document

    :param event dict: lambda event struct
    :rtype: dict
    :return: STAC root document as dict
    """

    doc = copy.deepcopy(STAC_DOC_TEMPLATE)
    phost = '{protocol}://{host}'.format(protocol=event['headers']['X-Forwarded-Proto'],
                                         host=event['headers']['Host'])
    ppath = '{phost}{path}'.format(phost=phost,
                                   path=event['path'])
    prefix = event['path'].split('/')[1]
    doc['links'].append({"self":ppath})
    for collection in COLLECTIONS:
        doc['links'].append({"child":'{phost}/{prefix}/collections/{collection}'.\
                             format(phost=phost,
                                    prefix=prefix,
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
