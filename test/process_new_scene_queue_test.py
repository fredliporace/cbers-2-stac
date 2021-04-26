"""process_new_scene_test"""

import pytest

from cbers2stac.process_new_scene_queue.code import (  # process_queue
    build_sns_topic_msg_attributes,
    convert_inpe_to_stac,
    get_s3_keys,
    parse_quicklook_key,
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


@pytest.mark.skip("Test needs update to include SNS and DynamoDB")
def process_queue_test():
    """process_queue_test"""
    # process_queue(cbers_pds_bucket='cbers-pds',
    #               cbers_stac_bucket='cbers-stac',
    #               cbers_meta_pds_bucket='cbers-meta-pds',
    #               queue='https://sqs.us-east-1.amazonaws.com/769537946825/'
    #               'NewAWFIQuicklookMonitor',
    #               message_batch_size=1,
    #               sns_target_arn=None)
