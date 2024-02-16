"""stack config"""

from typing import Dict, List, Optional

import pydantic


class StackSettings(pydantic.BaseSettings):  # pylint: disable=too-few-public-methods
    """Application settings"""

    name: str = "cbers2stac"
    description: Optional[str] = "CBERS 4/4A and Amazonia 1 STAC catalogs"
    stage: str = "production"
    operator_email: str
    cost_center: Optional[str]

    backup_queue_retention_days: Optional[int] = 1

    stac_bucket_name: str
    stac_bucket_enable_cors: Optional[bool] = False
    stac_bucket_public_read: Optional[bool] = False
    stac_bucket_prune: bool = False
    cog_pds_meta_pds: str

    enable_api: Optional[bool] = False
    es_instance_type: str
    es_volume_size: int
    es_data_nodes: int

    topics: List[str]

    deploy_static_catalog_structure: Optional[bool] = True

    additional_env: Dict[str, str] = {}

    class Config:  # pylint: disable=too-few-public-methods
        """model config"""

        env_file = ".env"
        env_prefix = "STACK_"
