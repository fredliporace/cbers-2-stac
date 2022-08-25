"""process_new_scene_queue"""

import datetime
import json
import logging
import os
import re
from typing import Any, Dict, Generator

from botocore.exceptions import ClientError

from cbers2stac.layers.common.cbers_2_stac import (
    candidate_xml_files,
    convert_inpe_to_stac,
)
from cbers2stac.layers.common.utils import CBERS_AM_MISSIONS, get_client

# Get rid of "Found credentials in environment variables" messages
logging.getLogger("botocore.credentials").disabled = True
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def parse_quicklook_key(key: str) -> Dict[str, Any]:
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

    match = re.search(
        r"(?P<satellite>\w+)/(?P<camera>\w+)/"
        r"(?P<path>\d{3})/(?P<row>\d{3})/(?P<scene_id>\w+)/",
        key,
    )
    assert match, "Could not match " + key
    return {
        "satellite": match.group("satellite"),
        "camera": match.group("camera"),
        "path": match.group("path"),
        "row": match.group("row"),
        "scene_id": match.group("scene_id"),
        "collection": match.group("satellite") + match.group("camera"),
    }


def get_s3_keys(quicklook_key: str) -> Dict[str, Any]:
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
    stac_key = "%s/%s/%s/%s/%s.json" % (  # pylint: disable=consider-using-f-string
        qdict["satellite"],
        qdict["camera"],
        qdict["path"],
        qdict["row"],
        qdict["scene_id"],
    )
    inpe_metadata_key = (
        "%s/%s/%s/%s/%s/%s_BAND%s.xml"  # pylint: disable=consider-using-f-string
        % (
            qdict["satellite"],
            qdict["camera"],
            qdict["path"],
            qdict["row"],
            qdict["scene_id"],
            qdict["scene_id"],
            CBERS_AM_MISSIONS[qdict["satellite"]][qdict["camera"]]["meta_band"],
        )
    )
    return {
        "stac": stac_key,
        "inpe_metadata": inpe_metadata_key,
        "quicklook_keys": qdict,
    }


def sqs_messages(queue: str) -> Generator[Dict[str, Any], None, None]:
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
        response = get_client("sqs").receive_message(QueueUrl=queue)
        if "Messages" not in response:
            break
        msg = json.loads(response["Messages"][0]["Body"])
        records = json.loads(msg["Message"])
        retd = {}
        retd["key"] = records["Records"][0]["s3"]["object"]["key"]
        retd["bucket"] = records["Records"][0]["s3"]["bucket"]["name"]
        retd["ReceiptHandle"] = response["Messages"][0]["ReceiptHandle"]
        yield retd


def build_sns_topic_msg_attributes(stac_item):
    """Builds SNS message attributed from stac_item dictionary"""
    message_attr = {
        "properties.datetime": {
            "DataType": "String",
            "StringValue": stac_item["properties"]["datetime"],
        },
        "bbox.ll_lon": {"DataType": "Number", "StringValue": str(stac_item["bbox"][0])},
        "bbox.ll_lat": {"DataType": "Number", "StringValue": str(stac_item["bbox"][1])},
        "bbox.ur_lon": {"DataType": "Number", "StringValue": str(stac_item["bbox"][2])},
        "bbox.ur_lat": {"DataType": "Number", "StringValue": str(stac_item["bbox"][3])},
        "links.self.href": {
            "DataType": "String",
            "StringValue": next(
                item["href"] for item in stac_item["links"] if item["rel"] == "self"
            ),
        },
    }
    return message_attr


def process_message(
    msg: Dict[str, Any],
    buckets,
    sns_target_arn: str,
    catalog_update_queue: str,
    catalog_update_table: str,
) -> None:
    """
    Process a single message. Generate STAC item, send STAC item to SNS topic,
    write key into DynamoDB table and, optionally, send key to queue for
    further processing.

    Input:
      msg(dict): message (quicklook) to be processed, quicklook s3 key is 'key'.
      buckets(dict): buckets for 'cog', 'stac' and 'metadata'
      sns_target_arn(string): SNS arn for stac items. Items are always published
      catalog_update_queue(string): URL of queue that receives new STAC items
        for updating the catalog structure, None if not used.
      catalog_update_table: DynamoDB table name that hold the catalog update requests
    """

    LOGGER.info(msg["key"])
    metadata_keys = get_s3_keys(msg["key"])

    assert metadata_keys["quicklook_keys"]["camera"] in (
        "MUX",
        "AWFI",
        "PAN10M",
        "PAN5M",
        "WPM",
        "WFI",
    ), ("Unrecognized key: " + metadata_keys["quicklook_keys"]["camera"])

    local_stac_item = "/tmp/" + metadata_keys["stac"].split("/")[-1]
    found = False
    # Check candidates for XML, required to try LEFT and RIGHT for Amazonia1
    for inpe_metadata_option in candidate_xml_files(
        metadata_keys["inpe_metadata"].split("/")[-1]
    ):
        local_inpe_metadata = "/tmp/" + inpe_metadata_option
        # Download INPE metadata and generate STAC item file
        try:
            with open(local_inpe_metadata, "wb") as data:
                get_client("s3").download_fileobj(
                    buckets["cog"],
                    "/".join(metadata_keys["inpe_metadata"].split("/")[0:-1])
                    + "/"
                    + inpe_metadata_option,
                    data,
                    ExtraArgs={"RequestPayer": "requester"},
                )
            found = True
            break
        except ClientError:
            pass
    assert found, f"Can't find metadata for {metadata_keys['inpe_metadata']}"
    stac_meta = convert_inpe_to_stac(
        inpe_metadata_filename=local_inpe_metadata,
        stac_metadata_filename=local_stac_item,
        buckets=buckets,
    )
    # Upload STAC item file
    with open(local_stac_item, "rb") as data:
        get_client("s3").upload_fileobj(data, buckets["stac"], metadata_keys["stac"])

    # Publish to SNS topic
    get_client("sns").publish(
        TargetArn=sns_target_arn,
        Message=json.dumps(stac_meta),
        MessageAttributes=build_sns_topic_msg_attributes(stac_meta),
    )

    # Send message to update catalog tree queue
    if catalog_update_queue:
        get_client("sqs").send_message(
            QueueUrl=catalog_update_queue, MessageBody=metadata_keys["stac"]
        )

    # Request catalog update
    catalog_update_request(
        table_name=catalog_update_table, stac_item_key=metadata_keys["stac"]
    )


def catalog_update_request(table_name: str, stac_item_key: str):
    """
    Generate a catalog structure update request by recording
    register into DynamoDB table.

    Input:
      stac_item_key(string): ditto
      table_name(string): DynamoDB table name
    """

    get_client("dynamodb").put_item(
        TableName=table_name,
        Item={
            "stacitem": {"S": stac_item_key},
            "datetime": {"S": str(datetime.datetime.now())},
        },
    )


def process_trigger(
    *,
    stac_bucket: str,
    cog_pds_meta_pds: Dict[str, str],
    event: Dict[str, Any],
    sns_target_arn: str,
    sns_reconcile_target_arn: str,
    catalog_update_queue: str,
    catalog_update_table: str,
):
    """
    Read quicklook queue and create STAC items if necessary.

    Input:
      stac_bucket: ditto
      cog_pds_meta_pds: maps COG bucket name to metadata bucket name
      event: event dictionary generated by trigger
      sns_target_arn: SNS arn for new stac items topic
      sns_reconcile_target_arn: SNS arn for reconciled stac items topic
      catalog_update_queue: URL of queue that receives new
                            STAC items for updating the
                            catalog structure
      catalog_update_table: DynamoDB that hold the catalog update requests
    """

    buckets = {
        "stac": stac_bucket,
    }
    for record in event["Records"]:
        message = json.loads(json.loads(record["body"])["Message"])
        for rec in message["Records"]:
            if rec["s3"]["object"].get("reconcile"):
                eff_sns_target_arn = sns_reconcile_target_arn
            else:
                eff_sns_target_arn = sns_target_arn
            process_message(
                {"key": rec["s3"]["object"]["key"]},
                {
                    **buckets,
                    **{
                        "cog": rec["s3"]["bucket"]["name"],
                        "metadata": cog_pds_meta_pds[rec["s3"]["bucket"]["name"]],
                    },
                },
                eff_sns_target_arn,
                catalog_update_queue,
                catalog_update_table,
            )


def process_queue(
    *,
    stac_bucket: str,
    cog_pds_meta_pds: Dict[str, str],
    queue: str,
    message_batch_size: int,
    sns_reconcile_target_arn: str,
    catalog_update_queue: str,
    catalog_update_table: str,
    delete_processed_messages: bool = False,
):
    """
    Read quicklook queue and create STAC items if necessary.

    Input:
      stac_bucket: ditto
      cog_pds_meta_pds: maps COG pds to metadata PDS
      queue: SQS URL
      message_batch_size: maximum number of messages to be processed, 0 for
                          all messages.
      sns_reconcile_target_arn: SNS arn for reconciled stac items topic
      catalog_update_queue: URL of queue that receives new STAC
                                    items for updating the catalog structure
      catalog_update_table: DynamoDB that hold the catalog update requests
      delete_processed_messages: if True messages are deleted from queue
                                 after processing
    """

    buckets = {
        "stac": stac_bucket,
    }
    processed_messages = 0
    for msg in sqs_messages(queue):

        process_message(
            msg,
            {
                **buckets,
                **{"cog": msg["bucket"], "metadata": cog_pds_meta_pds[msg["bucket"]]},
            },
            sns_reconcile_target_arn,
            catalog_update_queue,
            catalog_update_table,
        )

        # Remove message from queue
        if delete_processed_messages:
            get_client("sqs").delete_message(
                QueueUrl=queue, ReceiptHandle=msg["ReceiptHandle"]
            )

        processed_messages += 1
        if processed_messages == message_batch_size:
            break


def handler(event, context):  # pylint: disable=unused-argument
    """Lambda entry point for actively consuming messages from queue.
    Event keys:
    """

    if "queue" in event:
        # Lambda is being invoked to read messages directly from queue URL
        # In that mode SNS events are always sent to the internal
        # reconcile topic
        process_queue(
            stac_bucket=os.environ["STAC_BUCKET"],
            cog_pds_meta_pds=json.loads(os.environ["COG_PDS_META_PDS"]),
            queue=event["queue"],
            message_batch_size=int(os.environ["MESSAGE_BATCH_SIZE"]),
            sns_reconcile_target_arn=os.environ["SNS_RECONCILE_TARGET_ARN"],
            catalog_update_queue=os.environ.get("CATALOG_UPDATE_QUEUE"),
            catalog_update_table=os.environ["CATALOG_UPDATE_TABLE"],
            delete_processed_messages=int(os.environ["DELETE_MESSAGES"]) == 1,
        )
    else:
        # Lambda is being invoked as trigger to SQS
        process_trigger(
            stac_bucket=os.environ["STAC_BUCKET"],
            cog_pds_meta_pds=json.loads(os.environ["COG_PDS_META_PDS"]),
            event=event,
            sns_target_arn=os.environ["SNS_TARGET_ARN"],
            sns_reconcile_target_arn=os.environ["SNS_RECONCILE_TARGET_ARN"],
            catalog_update_queue=os.environ.get("CATALOG_UPDATE_QUEUE"),
            catalog_update_table=os.environ["CATALOG_UPDATE_TABLE"],
        )
