"""
DBTable
"""

from dataclasses import dataclass
from typing import Any, Dict

from cbers2stac.layers.common.utils import get_resource


@dataclass
class DBTable:
    """
    The DynamoDB table and services
    """

    table_ = None
    pk_attr_name_: str = "stacitem"

    def __init__(self, db_table=None) -> None:
        """
        Ctor.

        Uses db_table if not None. Otherwise the table is created.
        """
        if not db_table:
            resource = get_resource("dynamodb")
            self.table_ = resource.create_table(**DBTable.schema())
            # Wait until the table exists.
            self.table_.meta.client.get_waiter("table_exists").wait(
                TableName=DBTable.schema()["TableName"]
            )
        else:
            self.table_ = db_table

    def table(self):
        """
        Table getter (testing purposes only)
        """
        return self.table_

    @staticmethod
    def schema() -> Dict[str, Any]:
        """
        Schema getter (testing and CDK deployment)
        """
        return {
            "TableName": "CatalogUpdateTable",
            "KeySchema": [
                {
                    # HASH is the partition key
                    "AttributeName": DBTable.pk_attr_name_,
                    "KeyType": "HASH",
                },
            ],
            "AttributeDefinitions": [
                {"AttributeName": DBTable.pk_attr_name_, "AttributeType": "S"},
            ],
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 50,
                "WriteCapacityUnits": 50,
            },
        }
