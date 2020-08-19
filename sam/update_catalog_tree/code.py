"""update_catalog_tree"""

import os
import re
from operator import itemgetter
from collections import OrderedDict
import json
from copy import deepcopy

import boto3
#from botocore.errorfactory import ClientError

from utils import build_absolute_prefix
from definitions import BASE_CATALOG, BASE_COLLECTION, BASE_CAMERA

S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client('sqs')

def write_catalog_to_s3(bucket, prefix, catalog):
    """
    Uploads a catalog represented as a dictionary to bucket
    with prefix/catalog.json key.
    """

    if 'license' not in catalog:
        s3_catalog_file = prefix + '/catalog.json'
    else:
        s3_catalog_file = prefix + '/collection.json'

    S3_CLIENT.put_object(Body=json.dumps(catalog, indent=2),
                         Bucket=bucket,
                         Key=s3_catalog_file)

def build_catalog_from_s3(bucket, prefix, response=None):
    """
    Returns a catalog for a given prefix. The catalog is represented
    as a dictionary.

    Input:
    bucket(string): bucket name
    prefix(string): key prefix (no trailing /)
    response(dict): S3 output from list_objects_v2, used for unit testing
    """
    catalog_info = get_catalog_info(prefix)
    catalog = base_stac_catalog(bucket,
                                satellite=catalog_info['satellite+mission'],
                                mission=None,
                                camera=catalog_info['camera'],
                                path=catalog_info['path'],
                                row=catalog_info['row'])
    if catalog_info['level'] == 0:
        new_links = get_items_from_s3(bucket, prefix + '/', response)
    else:
        new_links = get_catalogs_from_s3(bucket, prefix +'/', response)
    catalog['links'] += new_links

    return catalog

def get_catalog_info(key):
    """
    Returns an dict representing the catalog level, instrument, path and
    row. Keys are:
     - level(int), 0 is the deepest level, satellite/camera/path/row
     - satellite, mission, camera, path, row
    represents the key containing the items. Value is increased
    for each parent dir.
     - is_collection: bool if catalog at the level is a collection

    Input:
    key(string): 'directory' key
    """

    ret = dict()

    # Level
    levels = key.split('/')
    level = 4 - len(levels)
    assert 0 <= level <= 2, "Unsupported level " + str(level)
    ret['level'] = level
    ret['satellite+mission'] = None
    ret['camera'] = None
    ret['path'] = None
    ret['row'] = None
    # @todo not currently used, check for removal
    ret['is_collection'] = (level == 2)

    keys = ['satellite+mission', 'camera', 'path', 'row']
    for index, key_id in enumerate(levels):
        ret[keys[index]] = key_id

    return ret

def get_catalogs_from_s3(bucket, prefix, response=None):
    """
    Return a list with catalog (catalog.json files) located in S3
    prefix. Assumes every subdir contains a catalog.json file

    Input:
    bucket(string): bucket name
    prefix(string): key prefix
    response(dict): S3 output from list_objects_v2, used for unit testing
    """
    ret = list()
    if not response:
        response = S3_CLIENT.list_objects_v2(Bucket=bucket,
                                             Delimiter='/',
                                             Prefix=prefix)
        assert not response['IsTruncated'], "Truncated S3 listing"
    for item in response['CommonPrefixes']:
        key = item['Prefix'].split('/')[-2] + '/catalog.json'
        ret.append({"rel":"child",
                    "href":key})
    return sorted(ret, key=itemgetter('href'))

def get_items_from_s3(bucket, prefix, response=None):
    """
    Return a list with items (.json files) located in S3
    prefix.

    Input:
    bucket(string): bucket name
    prefix(string): key prefix
    response(dict): S3 output from list_objects_v2, used for unit testing
    """
    ret = list()
    if not response:
        response = S3_CLIENT.list_objects_v2(Bucket=bucket,
                                             Delimiter='/',
                                             Prefix=prefix)
        assert not response['IsTruncated'], "Truncated S3 listing"
    for item in response['Contents']:
        key = item['Key'].split('/')[-1]
        # Skip catalog.json files, including only L\d{1}.json
        if re.match(r'.*L\d{1}.json', key):
            ret.append({"rel":"item",
                        "href":key})
    return sorted(ret, key=itemgetter('href'))

def sqs_messages(queue):
    """
    Generator for SQS messages.

    Input:
    queue(string): SQS URL.

    Ouput:
    dict with the following keys:
      key: Quicklook s3 key
      ReceiptHandle: Message receipt handle
    """

    while True:
        response = SQS_CLIENT.receive_message(
            QueueUrl=queue)
        if 'Messages' not in response:
            break
        retd = dict()
        retd['stac_item'] = response['Messages'][0]['Body']
        retd['ReceiptHandle'] = response['Messages'][0]['ReceiptHandle']
        yield retd

def get_base_collection(camera: str):
    """
    Return the base collection for the camera.

    :param camera str: camera
    :rtype: dict
    :return: base collection
    """

    collection = deepcopy(BASE_CATALOG)
    collection.update(BASE_COLLECTION)
    collection['properties'] = BASE_CAMERA[camera]['properties']
    collection['item_assets'] = BASE_CAMERA[camera]['item_assets']

    return collection

def base_stac_catalog(bucket, satellite,
                      mission=None, camera=None,
                      path=None, row=None):
    """JSON STAC catalog or collection with common items"""

    # Checks if on collection level
    in_collection = camera and not path and not row

    if in_collection:
        json_filename = 'collection.json'
        stac_catalog = get_base_collection(camera)
    else:
        json_filename = 'catalog.json'
        stac_catalog = BASE_CATALOG

    name = satellite
    description = name

    # If mission is not defined we do not include the satellite
    # name into self href.
    if mission:
        sat_sensor = satellite
    else:
        # Check for satellite+mission in first parameter
        # @todo change this to always separate satellite and mission
        if satellite == 'CBERS':
            sat_sensor = None
        else:
            sat_sensor = satellite

    if mission:
        name += mission
        description += mission
        sat_sensor += mission
    if camera:
        name += ' %s' % (camera)
        description += ' %s camera' % (camera)
        sat_sensor += '/%s' % camera
    if path:
        name += ' %s' % (path)
        description += ' path %s' % (path)
    if row:
        name += '/%s' % (row)
        description += ' row %s' % (row)
    description += ' catalog'

    # @todo the collection id is also built in the stac item,
    # should be unified
    # @todo currently must support two cases: satellite concatenated
    # or not with mission. Clean up.
    if in_collection:
        if mission:
            stac_catalog['id'] = satellite + mission + camera
        else:
            stac_catalog['id'] = satellite + camera
    else:
        stac_catalog['id'] = name
    stac_catalog['description'] = description

    stac_catalog['links'] = list()

    # Checks if on collection level
    if camera and not path and not row:
        json_filename = 'collection.json'
    else:
        json_filename = 'catalog.json'

    # @todo use common function to build links, see item construction
    self_link = OrderedDict()
    self_link['rel'] = 'self'
    self_link['href'] = build_absolute_prefix(bucket,
                                              sat_sensor,
                                              path, row) + json_filename
    stac_catalog['links'].append(self_link)

    root_link = OrderedDict()
    root_link['rel'] = 'root'
    root_link['href'] = build_absolute_prefix(bucket) + 'catalog.json'
    stac_catalog['links'].append(root_link)

    if path and not row:
        parent_json_filename = 'collection.json'
    else:
        parent_json_filename = 'catalog.json'

    if mission or camera or path or row:

        parent_link = OrderedDict()
        parent_link['rel'] = 'parent'
        parent_link['href'] = '../' + parent_json_filename
        stac_catalog['links'].append(parent_link)

    return stac_catalog

def trigger_handler(event, context):  # pylint: disable=unused-argument
    """Lambda entry point for SQS trigger integration
    Event keys:
    """
    for record in event['Records']:
        prefix = record['body']
        print("Processing " + prefix)
        catalog = build_catalog_from_s3(bucket=os.environ['CBERS_STAC_BUCKET'],
                                        prefix=prefix)
        write_catalog_to_s3(bucket=os.environ['CBERS_STAC_BUCKET'],
                            prefix=prefix,
                            catalog=catalog)
