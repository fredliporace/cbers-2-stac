"""stac_endpoint"""

import os
import json
import boto3

S3_CLIENT = boto3.client('s3')

def get_root_catalog(bucket: str):
    """
    Return the root catalog
    """

    catalog = S3_CLIENT.get_object(Bucket=bucket,
                                   Key='catalog.json')['Body'].read()
    return json.loads(catalog)

def handler(event, context): # pylint: disable=unused-argument
    """
    Lambda entry point
    """

    if event['path'] == '/stac':
        document = get_root_catalog(bucket=os.environ['CBERS_STAC_BUCKET'])
    else:
        raise ValueError("Path unknown: {path}".format(path=event['path']))

    retmsg = {
        'statusCode': '200',
        'body': json.dumps(document, indent=2),
        'headers': {
            'Content-Type': 'application/json',
        }
    }

    return retmsg
