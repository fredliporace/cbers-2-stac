"""
STAC validator aggregator
"""

import json
import os
import subprocess
import tempfile
from shutil import which
from typing import Any, Dict, Union

from jsonschema import RefResolver, validate
from pydantic import BaseModel
from retry import retry


class STACValidator(BaseModel):
    """
    Validates a JSON document against a schema
    """

    schema_filename: str
    schema_path: str = ""
    resolver: RefResolver = None
    c_schema: Union[Dict[Any, Any], None] = None
    stac_node_validator_available: bool = False

    class Config:  # pylint: disable=too-few-public-methods
        """Required by RefResolver type"""

        arbitrary_types_allowed = True

    def __init__(self, **data: Any):
        """
        Initialize derived fields
        """
        super().__init__(**data)

        fid = self.schema_filename.split(".")[-2]
        json_schema_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"json_schema/{fid}-spec/json-schema",
        )
        self.schema_path: str = os.path.join(json_schema_path, self.schema_filename)
        self.resolver = RefResolver("file://" + json_schema_path + "/", None)
        with open(self.schema_path) as fp_schema:
            self.c_schema = json.load(fp_schema)

        # Check if stac_node_validator is available
        self.stac_node_validator_available = which("stac-node-validator") is not None

    def validate_schema(self, filename: str):
        """
        Validate against STAC schema
        """
        with open(filename) as fp_in:
            assert (
                validate(json.load(fp_in), self.c_schema, resolver=self.resolver)
                is None
            )

    @staticmethod
    def validate_pystac(filename: str):
        """
        Validate using Pystac
        """
        with open(filename) as fname:
            jsd = json.load(fname)  # pylint: disable=unused-variable
            # See issue#47
            # validate_dict(jsd)

    # Retry needed when there is a schema download error
    @retry(subprocess.CalledProcessError, tries=5, delay=1)
    def validate_node(self, filename: str):
        """
        Validate using stac-node-validator
        """
        assert self.stac_node_validator_available
        # With shell=False I was getting lots of name resolution errors
        # when downloading the schemas
        # subprocess.run(["stac-node-validator", filename], check=True)
        subprocess.run(f"stac-node-validator {filename}", check=True, shell=True)

    def validate(
        self,
        filename: str,
        use_schema: bool = True,
        use_pystac: bool = True,
        use_node_validator: bool = True,
    ) -> None:
        """
        Validate file
        Raise exception if JSON is not validated by any of the available validators
        """

        # Validate with STAC schema
        if use_schema:
            self.validate_schema(filename)

        # Validate with PySTAC
        if use_pystac:
            STACValidator.validate_pystac(filename)

        # Validate with stac-node-validator
        if use_node_validator and self.stac_node_validator_available:
            self.validate_node(filename)

    def validate_dict(self, item: Dict[Any, Any], **kwargs):
        """
        Validate Dict
        Raise exception if JSON is not validated by any of the available validators
        """

        tfile = tempfile.NamedTemporaryFile(delete=True)
        with open(tfile.name, "w") as tfl:
            json.dump(item, tfl, indent=2)
        self.validate(tfile.name, **kwargs)
