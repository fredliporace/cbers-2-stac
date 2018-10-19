"""process_new_scene_queue"""

import os
import re
import json
import datetime

import boto3

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

SNS_CLIENT = boto3.client('sns')

DB_CLIENT = boto3.client('dynamodb')

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
      quicklook_keys: Dictionary obtained from quicklook filename,
        see parse_quicklook_key()
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
        'inpe_metadata':inpe_metadata_key,
        'quicklook_keys':qdict
    }

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
        msg = json.loads(response['Messages'][0]['Body'])
        records = json.loads(msg['Message'])
        retd = dict()
        retd['key'] = records['Records'][0]['s3']['object']['key']
        retd['ReceiptHandle'] = response['Messages'][0]['ReceiptHandle']
        yield retd

def build_sns_topic_msg_attributes(stac_item):
    """Builds SNS message attributed from stac_item dictionary"""
    message_attr = {
        'properties.datetime': {
            'DataType': 'String',
            'StringValue': stac_item['properties']['datetime']
        },
        'bbox.ll_lon': {
            'DataType': 'Number',
            'StringValue': str(stac_item['bbox'][0])
        },
        'bbox.ll_lat': {
            'DataType': 'Number',
            'StringValue': str(stac_item['bbox'][1])
        },
        'bbox.ur_lon': {
            'DataType': 'Number',
            'StringValue': str(stac_item['bbox'][2])
        },
        'bbox.ur_lat': {
            'DataType': 'Number',
            'StringValue': str(stac_item['bbox'][3])
        },
        'links.self.href': {
            'DataType': 'String',
            'StringValue': (item['href'] for item in stac_item['links'] \
                            if item['rel'] == 'self').__next__()
        }
    }
    return message_attr

def process_message(msg, buckets, sns_target_arn, catalog_update_queue,
                    catalog_update_table):
    """
    Process a single message. Generate STAC item, send STAC item to SNS topic,
    write key into DynamoDB table and, optionally, send key to queue for
    further processing.

    Input:
      msg(dict): message (quicklook) to be processed, key is 'key'.
      buckets(dict): buckets for 'cog', 'stac' and 'metadata'
      sns_target_arn(string): SNS arn for new stac items topic
      catalog_update_queue(string): URL of queue that receives new STAC items for
        updating the catalog structure, None if not used.
      catalog_update_table: DynamoDB that hold the catalog update requests
    """

    print(msg['key'])
    metadata_keys = get_s3_keys(msg['key'])
    # Currently only MUX and AWFI
    if metadata_keys['quicklook_keys']['camera'] not in ('MUX', 'AWFI'):
        return
    local_inpe_metadata = '/tmp/' + \
        metadata_keys['inpe_metadata'].split('/')[-1]
    local_stac_item = '/tmp/' + \
        metadata_keys['stac'].split('/')[-1]
    # Download INPE metadata and generate STAC item file
    with open(local_inpe_metadata, 'wb') as data:
        S3_CLIENT.download_fileobj(buckets['cog'],
                                   metadata_keys['inpe_metadata'], data)
    stac_meta = convert_inpe_to_stac(inpe_metadata_filename=local_inpe_metadata,
                                     stac_metadata_filename=local_stac_item,
                                     buckets=buckets)
    # Upload STAC item file
    with open(local_stac_item, 'rb') as data:
        S3_CLIENT.upload_fileobj(data, buckets['stac'],
                                 metadata_keys['stac'])

    # Publish to SNS topic, if defined
    if sns_target_arn:
        SNS_CLIENT.publish(TargetArn=sns_target_arn,
                           Message=json.dumps(stac_meta),
                           MessageAttributes=build_sns_topic_msg_attributes(stac_meta))

    # Send message to update catalog tree queue
    if catalog_update_queue:
        SQS_CLIENT.send_message(QueueUrl=catalog_update_queue,
                                MessageBody=metadata_keys['stac'])

    # Request catalog update
    catalog_update_request(table_name=catalog_update_table,
                           stac_item_key=metadata_keys['stac'])

def catalog_update_request(table_name, stac_item_key):
    """
    Generate a catalog structure update request by recording
    register into DynamoDB table.

    Input:
      stac_item_key(string): ditto
      table_name(string): DynamoDB table name
    """

    DB_CLIENT.put_item(
        TableName=table_name,
        Item={
            'stacitem': {'S': stac_item_key},
            'datetime': {'S': str(datetime.datetime.now())}
        })

def process_trigger(cbers_pds_bucket,
                    cbers_stac_bucket,
                    cbers_meta_pds_bucket,
                    event,
                    sns_target_arn,
                    catalog_update_queue,
                    catalog_update_table):
    """
    Read quicklook queue and create STAC items if necessary.

    Input:
      cbers_pds_bucket(string): ditto
      cbers_stac_bucket(string): ditto
      cbers_meta_pds_bucket(string): ditto
      event(dict): event dictionary generated by trigger
      sns_target_arn: SNS arn for new stac items topic
      catalog_update_queue(string): URL of queue that receives new STAC items for
        updating the catalog structure
      catalog_update_table: DynamoDB that hold the catalog update requests
    """

    buckets = {'cog':cbers_pds_bucket,
               'stac':cbers_stac_bucket,
               'metadata':cbers_meta_pds_bucket}
    for record in event['Records']:
        message = json.loads(json.loads(record['body'])['Message'])
        for rec in message['Records']:
            process_message({'key':rec['s3']['object']['key']},
                            buckets, sns_target_arn, catalog_update_queue,
                            catalog_update_table)

def process_queue(cbers_pds_bucket,
                  cbers_stac_bucket,
                  cbers_meta_pds_bucket,
                  queue,
                  message_batch_size,
                  sns_target_arn,
                  catalog_update_queue,
                  catalog_update_table,
                  delete_processed_messages=False):
    """
    Read quicklook queue and create STAC items if necessary.

    Input:
      cbers_pds_bucket(string): ditto
      cbers_stac_bucket(string): ditto
      cbers_meta_pds_bucket(string): ditto
      queue(string): SQS URL
      message_batch_size: maximum number of messages to be processed, 0 for
                          all messages.
      sns_target_arn: SNS arn for new stac items topic
      catalog_update_queue(string): URL of queue that receives new STAC items for
        updating the catalog structure
      catalog_update_table: DynamoDB that hold the catalog update requests
      delete_processed_messages: if True messages are deleted from queue
                                 after processing
    """

    buckets = {'cog':cbers_pds_bucket,
               'stac':cbers_stac_bucket,
               'metadata':cbers_meta_pds_bucket}
    processed_messages = 0
    for msg in sqs_messages(queue):

        process_message(msg, buckets, sns_target_arn, catalog_update_queue,
                        catalog_update_table)

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
    """Lambda entry point for actively consuming messages from queue.
    Event keys:
    """

    if 'queue' in event:
        # Lambda is being invoked to read messages directly from queue
        process_queue(cbers_pds_bucket=os.environ['CBERS_PDS_BUCKET'],
                      cbers_stac_bucket=os.environ['CBERS_STAC_BUCKET'],
                      cbers_meta_pds_bucket=os.environ['CBERS_META_PDS_BUCKET'],
                      queue=event['queue'],
                      message_batch_size=int(os.environ['MESSAGE_BATCH_SIZE']),
                      sns_target_arn=os.environ['SNS_TARGET_ARN'],
                      catalog_update_queue=os.environ.get('CATALOG_UPDATE_QUEUE'),
                      catalog_update_table=os.environ['CATALOG_UPDATE_TABLE'],
                      delete_processed_messages=int(os.environ['DELETE_MESSAGES']) == 1)
    else:
        # Lambda is being invoked as trigger to SQS
        process_trigger(cbers_pds_bucket=os.environ['CBERS_PDS_BUCKET'],
                        cbers_stac_bucket=os.environ['CBERS_STAC_BUCKET'],
                        cbers_meta_pds_bucket=os.environ['CBERS_META_PDS_BUCKET'],
                        event=event,
                        sns_target_arn=os.environ['SNS_TARGET_ARN'],
                        catalog_update_queue=os.environ.get('CATALOG_UPDATE_QUEUE'),
                        catalog_update_table=os.environ['CATALOG_UPDATE_TABLE'])
