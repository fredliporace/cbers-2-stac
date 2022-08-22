"""process_new_scene_test"""

import pathlib

import pytest

from cbers2stac.consume_reconcile_queue.code import populate_queue_with_quicklooks
from cbers2stac.layers.common.dbtable import DBTable
from cbers2stac.process_new_scene_queue.code import (  # process_queue
    build_sns_topic_msg_attributes,
    convert_inpe_to_stac,
    get_s3_keys,
    parse_quicklook_key,
    process_message,
    sqs_messages,
)


def populate_bucket_test_case_1(bucket: str, s3_client):
    """populate_bucket_test_case_1."""
    fixture_prefix = "test/fixtures/cbers_pds_bucket_structure/"
    paths = pathlib.Path(fixture_prefix).rglob("*")
    jpegs = [str(f.relative_to(fixture_prefix)) for f in paths if "jpg" in str(f)]
    for jpeg in jpegs:
        s3_client.upload_file(
            Filename=fixture_prefix + "/" + jpeg, Bucket=bucket, Key=jpeg
        )


def test_parse_quicklook_key():
    """parse_quicklook_key_test"""

    keys = parse_quicklook_key(
        "CBERS4/AWFI/155/135/"
        "CBERS_4_AWFI_20170515_155_135_L2/"
        "CBERS_4_AWFI_20170515_155_135.jpg"
    )
    assert keys["satellite"] == "CBERS4"
    assert keys["camera"] == "AWFI"
    assert keys["path"] == "155"
    assert keys["row"] == "135"
    assert keys["scene_id"] == "CBERS_4_AWFI_20170515_155_135_L2"
    assert keys["collection"] == "CBERS4AWFI"

    keys = parse_quicklook_key(
        "CBERS4A/WPM/209/139/"
        "CBERS_4A_WPM_20200730_209_139_L4/"
        "CBERS_4A_WPM_20200730_209_139.png"
    )
    assert keys["satellite"] == "CBERS4A"
    assert keys["camera"] == "WPM"
    assert keys["path"] == "209"
    assert keys["row"] == "139"
    assert keys["scene_id"] == "CBERS_4A_WPM_20200730_209_139_L4"
    assert keys["collection"] == "CBERS4AWPM"

    # Amazonia1 case
    keys = parse_quicklook_key(
        "AMAZONIA1/WFI/035/020/"
        "AMAZONIA_1_WFI_20220814_035_020_L4/"
        "AMAZONIA_1_WFI_20220814_035_020.png"
    )
    assert keys["satellite"] == "AMAZONIA1"
    assert keys["camera"] == "WFI"
    assert keys["path"] == "035"
    assert keys["row"] == "020"
    assert keys["scene_id"] == "AMAZONIA_1_WFI_20220814_035_020_L4"
    assert keys["collection"] == "AMAZONIA1WFI"


def test_get_s3_keys():
    """get_s3_keys_test"""

    s3_keys = get_s3_keys(
        "CBERS4/AWFI/155/135/"
        "CBERS_4_AWFI_20170515_155_135_L2/"
        "CBERS_4_AWFI_20170515_155_135.jpg"
    )
    assert (
        s3_keys["stac"] == "CBERS4/AWFI/155/135/CBERS_4_AWFI_20170515_155_135_L2.json"
    )
    assert (
        s3_keys["inpe_metadata"]
        == "CBERS4/AWFI/155/135/CBERS_4_AWFI_20170515_155_135_L2/"
        "CBERS_4_AWFI_20170515_155_135_L2_BAND14.xml"
    )
    assert s3_keys["quicklook_keys"]["camera"] == "AWFI"

    s3_keys = get_s3_keys(
        "CBERS4A/WPM/209/139/"
        "CBERS_4A_WPM_20200730_209_139_L4/"
        "CBERS_4A_WPM_20200730_209_139.png"
    )
    assert (
        s3_keys["stac"] == "CBERS4A/WPM/209/139/CBERS_4A_WPM_20200730_209_139_L4.json"
    )
    assert (
        s3_keys["inpe_metadata"]
        == "CBERS4A/WPM/209/139/CBERS_4A_WPM_20200730_209_139_L4/"
        "CBERS_4A_WPM_20200730_209_139_L4_BAND2.xml"
    )
    assert s3_keys["quicklook_keys"]["camera"] == "WPM"

    s3_keys = get_s3_keys(
        "CBERS4A/MUX/219/125/"
        "CBERS_4A_MUX_20210416_219_125_L4/"
        "CBERS_4A_MUX_20210416_219_125.png"
    )
    assert (
        s3_keys["stac"] == "CBERS4A/MUX/219/125/CBERS_4A_MUX_20210416_219_125_L4.json"
    )
    assert (
        s3_keys["inpe_metadata"]
        == "CBERS4A/MUX/219/125/CBERS_4A_MUX_20210416_219_125_L4/"
        "CBERS_4A_MUX_20210416_219_125_L4_BAND6.xml"
    )
    assert s3_keys["quicklook_keys"]["camera"] == "MUX"

    # Amazonia1 case
    s3_keys = get_s3_keys(
        "AMAZONIA1/WFI/035/020/"
        "AMAZONIA_1_WFI_20220814_035_020_L4/"
        "AMAZONIA_1_WFI_20220814_035_020.png"
    )
    assert (
        s3_keys["stac"]
        == "AMAZONIA1/WFI/035/020/AMAZONIA_1_WFI_20220814_035_020_L4.json"
    )
    assert (
        s3_keys["inpe_metadata"]
        == "AMAZONIA1/WFI/035/020/AMAZONIA_1_WFI_20220814_035_020_L4/"
        "AMAZONIA_1_WFI_20220814_035_020_L4_BAND2.xml"
    )
    assert s3_keys["quicklook_keys"]["camera"] == "WFI"


def test_sns_topic_msg_attr_test():
    """build_sns_topic_msg_attributes_test"""

    buckets = {"metadata": "cbers-meta-pds", "cog": "cbers-pds", "stac": "cbers-stac"}

    stac_meta = convert_inpe_to_stac(
        inpe_metadata_filename="test/fixtures/CBERS_4_MUX_20170528"
        "_090_084_L2_BAND6.xml",
        stac_metadata_filename=None,
        buckets=buckets,
    )
    msg_attr = build_sns_topic_msg_attributes(stac_meta)
    assert msg_attr["links.self.href"] == {
        "DataType": "String",
        "StringValue": "https://cbers-stac.s3.amazonaws.com/CBERS4/"
        "MUX/090/084/CBERS_4_MUX_20170528_090_084_L2.json",
    }


@pytest.mark.s3_bucket_args("cbers-stac")
@pytest.mark.sqs_queue_args("queue")
def test_sqs_mesages(s3_bucket, sqs_queue):
    """
    test_sqs_mesages
    """

    queue = sqs_queue
    s3_client, _ = s3_bucket

    populate_bucket_test_case_1(bucket="cbers-stac", s3_client=s3_client)
    populate_queue_with_quicklooks(
        prefix="CBERS4/MUX/", bucket="cbers-stac", queue=queue.url, suffix=r"\.jpg"
    )
    gen = sqs_messages(queue.url)
    msg = next(gen)
    assert {"bucket", "key"} <= set(msg)
    assert msg["bucket"] == "cbers-stac"


@pytest.mark.s3_buckets_args(["cog", "stac"])
@pytest.mark.sqs_queues_args(["quicklook-queue", "catup_queue"])
@pytest.mark.dynamodb_table_args({**(DBTable.schema())})
def test_process_mesage(s3_buckets, sqs_queues, sns_topic, dynamodb_table):
    """
    test_process_mesage
    """

    s3_client, _ = s3_buckets
    quicklook_queue = sqs_queues[0]
    catup_queue = sqs_queues[1]
    _, topic = sns_topic
    db_table = dynamodb_table

    fixture_prefix = "test/fixtures/cbers_amazonia_pds_bucket_structure/"
    paths = pathlib.Path(fixture_prefix).rglob("*")
    files = [
        str(f.relative_to(fixture_prefix))
        for f in paths
        if any(ext in str(f) for ext in ["png", "xml"])
    ]
    for upfile in files:
        s3_client.upload_file(
            Filename=fixture_prefix + "/" + upfile, Bucket="cog", Key=upfile
        )

    # CB4A MUX case
    populate_queue_with_quicklooks(
        bucket="cog", prefix="CBERS4A/MUX/", queue=quicklook_queue.url, suffix=r"\.png"
    )
    gen = sqs_messages(quicklook_queue.url)
    msg = next(gen)
    process_message(
        msg=msg,
        buckets={"cog": "cog", "stac": "stac", "metadata": "cbers-stac"},
        sns_target_arn=topic["TopicArn"],
        catalog_update_queue=catup_queue.url,
        catalog_update_table=DBTable.schema()["TableName"],
    )
    assert len(db_table.scan()["Items"]) == 1
    assert (
        db_table.scan()["Items"][0]["stacitem"]
        == "CBERS4A/MUX/222/116/CBERS_4A_MUX_20220810_222_116_L4.json"
    )

    # AMAZONIA1 WFI cases, LEFT and both optics
    populate_queue_with_quicklooks(
        bucket="cog",
        prefix="AMAZONIA1/WFI/",
        queue=quicklook_queue.url,
        suffix=r"\.png",
    )
    gen = sqs_messages(quicklook_queue.url)
    msg = next(gen)
    process_message(
        msg=msg,
        buckets={"cog": "cog", "stac": "stac", "metadata": "cbers-stac"},
        sns_target_arn=topic["TopicArn"],
        catalog_update_queue=catup_queue.url,
        catalog_update_table=DBTable.schema()["TableName"],
    )
    assert len(db_table.scan()["Items"]) == 2
    assert "AMAZONIA1/WFI/033/018/AMAZONIA_1_WFI_20220810_033_018_L4.json" in [
        item["stacitem"] for item in db_table.scan()["Items"]
    ]
