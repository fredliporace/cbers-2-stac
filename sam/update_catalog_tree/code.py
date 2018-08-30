"""process_new_scene_queue"""

import os
import re
from collections import OrderedDict
import json

import boto3
from botocore.errorfactory import ClientError

S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client('sqs')

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
                  queue,
                  message_batch_size,
                  delete_processed_messages=False):
    """
    Read messages from catalog update queue and update catalogs.

    Input:
      cbers_pds_bucket(string): ditto
      cbers_stac_bucket(string): ditto
      cbers_meta_pds_bucket(string): ditto
      queue(string): SQS URL
      message_batch_size: maximum number of messages to be processed, 0 for
                          all messages.
      delete_processed_messages: if True messages are deleted from queue
                                 after processing
    """

    buckets = {'cog':cbers_pds_bucket,
               'stac':cbers_stac_bucket,
               'metadata':cbers_meta_pds_bucket}
    processed_messages = 0
    for msg in sqs_messages(queue):

        # Update catalog tree
        update_catalog_tree(stac_item=msg['stac_item'],
                            buckets=buckets)

        # Remove message from queue
        if delete_processed_messages:
            SQS_CLIENT.delete_message(
                QueueUrl=queue,
                ReceiptHandle=msg['ReceiptHandle']
            )

        processed_messages += 1
        if processed_messages == message_batch_size:
            break

def handler(event, context):
    """Lambda entry point for actively consuming messages from update catalog.
    Event keys:
    """

    process_queue(cbers_pds_bucket=os.environ['CBERS_PDS_BUCKET'],
                  cbers_stac_bucket=os.environ['CBERS_STAC_BUCKET'],
                  cbers_meta_pds_bucket=os.environ['CBERS_META_PDS_BUCKET'],
                  queue=os.environ['CATALOG_UPDATE_QUEUE'],
                  message_batch_size=int(os.environ['MESSAGE_BATCH_SIZE']),
                  delete_processed_messages=int(os.environ['DELETE_MESSAGES']) == 1)
