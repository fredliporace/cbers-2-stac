"""utils.py"""

# Collections are currently hard-coded here, this is not
# an issue for CBERS on AWS since this does not frequently change
COLLECTIONS = {
    "CBERS4MUX":"CBERS4/MUX/collection.json",
    "CBERS4AWFI":"CBERS4/AWFI/collection.json",
    "CBERS4PAN10M":"CBERS4/PAN10M/collection.json",
    "CBERS4PAN5M":"CBERS4/PAN5M/collection.json"}

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
