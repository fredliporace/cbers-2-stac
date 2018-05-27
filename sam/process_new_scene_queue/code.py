"""process_new_scene_queue"""

import os
import re
from collections import OrderedDict
import json

import boto3
from botocore.errorfactory import ClientError

from cbers_2_stac import convert_inpe_to_stac

# CBERS metadata
CMETA = {
    'MUX': {
        'reference': 5,
        'num_bands': 4,
        'quicklook_pixel_size': 200,
        'red': 7,
        'green': 6,
        'blue': 5,
        'meta_band': 6
    },
    'AWFI': {
        'reference': 13,
        'num_bands': 4,
        'quicklook_pixel_size': 640,
        'red': 15,
        'green': 14,
        'blue': 13,
        'meta_band': 14
    },
    'PAN5M': {
        'reference': 1,
        'num_bands': 1,
        'quicklook_pixel_size': 50,
        'red': 1,
        'green': 1,
        'blue': 1,
        'meta_band': 1
    },
    'PAN10M': {
        'reference': 2,
        'num_bands': 3,
        'quicklook_pixel_size': 100,
        'red': 3,
        'green': 4,
        'blue': 2,
        'meta_band': 4
    }
}

S3_CLIENT = boto3.client('s3')

SQS_CLIENT = boto3.client('sqs')

def parse_quicklook_key(key):
    """
    Parse quicklook key and return dictionary with
    relevant fields.

    Input:
    key(string): quicklook key

    Output:
    dict with the following keys:
      satellite(string): e.g. CBERS4
      camera(string): e.g. MUX
      path(string): 0 padded path, 3 digits
      row(int): 0 padded row, 3 digits
      scene_id(string): e.g. CBERS_4_AWFI_20170515_155_135_L2
    """

    # Example input
    # CBERS4/AWFI/155/135/CBERS_4_AWFI_20170515_155_135_L2/CBERS_4_AWFI_20170515_155_135.jpg

    match = re.search(r'(?P<satellite>\w+)/(?P<camera>\w+)/'
                      r'(?P<path>\d{3})/(?P<row>\d{3})/(?P<scene_id>\w+)/',
                      key)
    assert match, "Could not match " + key
    return {
        'satellite':match.group('satellite'),
        'camera':match.group('camera'),
        'path':match.group('path'),
        'row':match.group('row'),
        'scene_id':match.group('scene_id')
    }

def get_s3_keys(quicklook_key):
    """
    Get S3 keys associated with quicklook
    key parameter.

    Input:
    quicklook_key(string): quicklook key

    Ouput:
    dict with the following keys:
      stac(string): STAC item file
      inpe_metadata(string): INPE original metadata file
    """

    qdict = parse_quicklook_key(quicklook_key)
    stac_key = "%s/%s/%s/%s/%s.json" % (qdict['satellite'], qdict['camera'],
                                        qdict['path'], qdict['row'],
                                        qdict['scene_id'])
    inpe_metadata_key = "%s/%s/%s/%s/%s/%s_BAND%s.xml" % (qdict['satellite'], qdict['camera'],
                                                          qdict['path'], qdict['row'],
                                                          qdict['scene_id'],
                                                          qdict['scene_id'],
                                                          CMETA[qdict['camera']]['meta_band'])
    return {
        'stac':stac_key,
        'inpe_metadata':inpe_metadata_key
    }

def sqs_messages(queue):
    """
    Generator for SQS messages.

    Input:
    queue(string): SQS URL.

    Ouput:
    dict with the following keys:
      key: Quicklook s3 key
    """

    while True:
        response = SQS_CLIENT.receive_message(
            QueueUrl=queue)
        if 'Messages' not in response:
            break
        msg = json.loads(response['Messages'][0]['Body'])
        records = json.loads(msg['Message'])
        retd = dict()
        retd['key'] = records['Records'][0]['s3']['object']['key']
        yield retd

def base_stac_catalog(satellite, mission=None, camera=None, path=None, row=None):
    """JSON SATC catalog with common itens"""

    stac_catalog = OrderedDict()

    name = satellite
    description = name

    if mission:
        name += mission
        description += mission
    if camera:
        name += ' %s' % (camera)
        description += ' %s camera' % (camera)
    if path:
        name += ' %s' % (path)
        description += ' path %s' % (path)
    if row:
        name += '/%s' % (row)
        description += ' row %s' % (row)
    description += ' catalog'

    stac_catalog['name'] = name
    stac_catalog['description'] = description

    stac_catalog['links'] = list()

    self_link = OrderedDict()
    self_link['rel'] = 'self'
    self_link['href'] = 'catalog.json'
    stac_catalog['links'].append(self_link)

    if mission or camera or path or row:
        parent_link = OrderedDict()
        parent_link['rel'] = 'parent'
        parent_link['href'] = '../catalog.json'
        stac_catalog['links'].append(parent_link)

    return stac_catalog

def update_catalog_tree(stac_item, buckets):
    """
    Traverse STAC catalog tree and update links
    """

    catalog_path = os.path.dirname(stac_item)
    stac_filename = os.path.basename(stac_item)

    match = re.match(r'(?P<satellite>\w+)_(?P<mission>\w+)_'
                     r'(?P<camera>\w+)_(?P<YHD>\w+)_(?P<path>\w+)_'
                     r'(?P<row>\w+)_(?P<level>\w+).json', stac_filename)
    assert match, "Can't match %s" % (stac_filename)
    satellite = match.group('satellite')
    mission = match.group('mission')
    camera = match.group('camera')
    path = match.group('path')
    row = match.group('row')

    # SAT/MISSION/CAMERA/PATH/ROW level
    local_catalog_file = '/tmp/catalog.json'
    local_updated_catalog_file = '/tmp/updated_catalog.json'
    s3_catalog_file = '%s/catalog.json' % (catalog_path)
    try:
        with open(local_catalog_file, 'wb') as data:
            S3_CLIENT.download_fileobj(buckets['stac'],
                                       s3_catalog_file, data)
    except ClientError:
        # File needs to be created
        with open(local_catalog_file, 'w') as data:
            json.dump(base_stac_catalog(satellite, mission, camera,
                                        path, row),
                      data,
                      indent=2)
    stac_catalog = None
    with open(local_catalog_file, 'r') as data:
        stac_catalog = json.load(data)
        stac_item = {'rel':'item', 'href':stac_filename}
        if stac_item not in stac_catalog['links']:
            stac_catalog['links'].append(stac_item)
    with open(local_updated_catalog_file, 'w') as data:
        json.dump(stac_catalog, data, indent=2)
    with open(local_updated_catalog_file, 'rb') as data:
        S3_CLIENT.upload_fileobj(data, buckets['stac'],
                                 s3_catalog_file)

    # SAT/MISSION/CAMERA/PATH level
    local_catalog_file = '/tmp/catalog.json'
    local_updated_catalog_file = '/tmp/updated_catalog.json'
    child_catalog = '%s/catalog.json' % (row)
    catalog_path = '%s%s/%s/%s' % (satellite, mission, camera, path)
    s3_catalog_file = '%s/catalog.json' % (catalog_path)
    try:
        with open(local_catalog_file, 'wb') as data:
            S3_CLIENT.download_fileobj(buckets['stac'],
                                       s3_catalog_file, data)
    except ClientError:
        # File needs to be created
        with open(local_catalog_file, 'w') as data:
            json.dump(base_stac_catalog(satellite, mission, camera,
                                        path),
                      data,
                      indent=2)
    stac_catalog = None
    with open(local_catalog_file, 'r') as data:
        stac_catalog = json.load(data)
        stac_item = {'rel':'child', 'href':child_catalog}
        if stac_item not in stac_catalog['links']:
            stac_catalog['links'].append(stac_item)
    with open(local_updated_catalog_file, 'w') as data:
        json.dump(stac_catalog, data, indent=2)
    with open(local_updated_catalog_file, 'rb') as data:
        S3_CLIENT.upload_fileobj(data, buckets['stac'],
                                 s3_catalog_file)

    # SAT/MISSION/CAMERA level
    local_catalog_file = '/tmp/catalog.json'
    local_updated_catalog_file = '/tmp/updated_catalog.json'
    child_catalog = '%s/catalog.json' % (path)
    catalog_path = '%s%s/%s' % (satellite, mission, camera)
    s3_catalog_file = '%s/catalog.json' % (catalog_path)
    try:
        with open(local_catalog_file, 'wb') as data:
            S3_CLIENT.download_fileobj(buckets['stac'],
                                       s3_catalog_file, data)
    except ClientError:
        # File needs to be created
        with open(local_catalog_file, 'w') as data:
            json.dump(base_stac_catalog(satellite, mission, camera),
                      data,
                      indent=2)
    stac_catalog = None
    with open(local_catalog_file, 'r') as data:
        stac_catalog = json.load(data)
        stac_item = {'rel':'child', 'href':child_catalog}
        if stac_item not in stac_catalog['links']:
            stac_catalog['links'].append(stac_item)
    with open(local_updated_catalog_file, 'w') as data:
        json.dump(stac_catalog, data, indent=2)
    with open(local_updated_catalog_file, 'rb') as data:
        S3_CLIENT.upload_fileobj(data, buckets['stac'],
                                 s3_catalog_file)

def process_queue(cbers_pds_bucket,
                  cbers_stac_bucket,
                  cbers_meta_pds_bucket,
                  queue):
    """
    Read quicklook queue and create STAC items if necessary.

    Input:
    cbers_pds_bucket(string): ditto
    queue(string): SQS URL
    """

    buckets = {'cog':cbers_pds_bucket,
               'stac':cbers_stac_bucket,
               'metadata':cbers_meta_pds_bucket}
    for msg in sqs_messages(queue):
        print(msg['key'])
        metadata_keys = get_s3_keys(msg['key'])
        local_inpe_metadata = '/tmp/' + \
                              metadata_keys['inpe_metadata'].split('/')[-1]
        local_stac_item = '/tmp/' + \
                          metadata_keys['stac'].split('/')[-1]
        # Download INPE metadata and generate STAC item file
        with open(local_inpe_metadata, 'wb') as data:
            S3_CLIENT.download_fileobj(cbers_pds_bucket,
                                       metadata_keys['inpe_metadata'], data)
        convert_inpe_to_stac(inpe_metadata_filename=local_inpe_metadata,
                             stac_metadata_filename=local_stac_item,
                             buckets=buckets)
        # Upload STAC item file
        with open(local_stac_item, 'rb') as data:
            S3_CLIENT.upload_fileobj(data, cbers_stac_bucket,
                                     metadata_keys['stac'])
        # Update catalog tree
        update_catalog_tree(stac_item=metadata_keys['stac'],
                            buckets=buckets)

def handler(event, context):
    """Lambda entry point
    Event keys:
    """

    return process_queue(cbers_pds_bucket=os.environ['CBERS_PDS_BUCKET'],
                         cbers_stac_bucket=os.environ['CBERS_STAC_BUCKET'],
                         cbers_meta_pds_bucket=os.environ['CBERS_META_PDS_BUCKET'],
                         queue=event['queue'])
