"""generate_catalog_levels_to_be_updated_test"""

import datetime

import pytest

from cbers2stac.generate_catalog_levels_to_be_updated.code import (
    GenerateCatalogLevelsToBeUpdated,
    get_catalog_levels,
)
from cbers2stac.layers.common.dbtable import DBTable


def test_get_catalog_levels():
    """
    test_get_catalog_levels
    """

    levels = get_catalog_levels(
        "CBERS4/AWFI/141/123/CBERS_4_AWFI_20160506_141_123_L2.json"
    )
    assert levels == ["CBERS4/AWFI/141/123", "CBERS4/AWFI/141", "CBERS4/AWFI"]


@pytest.mark.dynamodb_table_args({**(DBTable.schema())})
@pytest.mark.sqs_queue_args("queue")
def test_process(dynamodb_table, sqs_queue):
    """process_test"""

    db_table = dynamodb_table
    queue = sqs_queue

    stacitems = [
        # 2 levels to be updated
        "CBERS4/AWFI/141/123/CBERS_4_AWFI_20160506_141_123_L2.json",
        # ... +1 level(s) to be updated
        "CBERS4/AWFI/182/123/CBERS_4_AWFI_20170128_182_123_L4.json",
        "CBERS4/PAN10M/163/121/CBERS_4_PAN10M_20171020_163_121_L2.json",
        "CBERS4/PAN10M/163/122/CBERS_4_PAN10M_20171020_163_122_L2.json",
        "CBERS4/PAN10M/163/123/CBERS_4_PAN10M_20171020_163_123_L2.json",
        "CBERS4/PAN10M/163/124/CBERS_4_PAN10M_20171020_163_124_L2.json",
        "CBERS4/PAN10M/163/125/CBERS_4_PAN10M_20171020_163_125_L2.json",
        "CBERS4/PAN10M/163/126/CBERS_4_PAN10M_20171020_163_126_L2.json",
        "CBERS4/PAN10M/167/127/CBERS_4_PAN10M_20170912_167_127_L2.json",
        "CBERS4/PAN5M/111/082/CBERS_4_PAN5M_20180325_111_082_L2.json",
        "CBERS4/PAN5M/158/130/CBERS_4_PAN5M_20170217_158_130_L4.json",
        "CBERS4/MUX/098/097/CBERS_4_MUX_20150507_098_097_L2.json",
        "CBERS4/MUX/161/136/CBERS_4_MUX_20161027_161_136_L2.json",
    ]

    for stacitem in stacitems:
        db_table.put_item(
            Item={"stacitem": stacitem, "datetime": str(datetime.datetime.now())}
        )
    assert len(db_table.scan()["Items"]) == len(stacitems)

    test_table_name = DBTable.schema()["TableName"]
    gcl = GenerateCatalogLevelsToBeUpdated(
        input_table=test_table_name, output_table=None, queue=queue.url, iterations=11,
    )
    gcl.process()

    # Table should be empty after call
    assert len(db_table.scan()["Items"]) == 0

    all_messages = []
    while True:
        batch_size = 10
        messages = queue.receive_messages(MaxNumberOfMessages=batch_size)
        all_messages.append([msg.body for msg in messages])
        if len(messages) < batch_size:
            break
    flat = [item for sublist in all_messages for item in sublist]
    assert len(flat) == 25
