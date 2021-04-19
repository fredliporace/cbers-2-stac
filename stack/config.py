"""stack config"""

from typing import Dict, Optional

import pydantic


class StackSettings(pydantic.BaseSettings):  # pylint: disable=too-few-public-methods
    """Application settings"""

    name: str = "cbers2stac"
    description: Optional[str] = "CBERS 4/4A STAC static catalog"
    stage: str = "production"
    operator_email: str
    cost_center: Optional[str]

    stac_bucket_name: Optional[str] = None

    additional_env: Dict[str, str] = {}

    class Config:  # pylint: disable=too-few-public-methods
        """model config"""

        env_file = ".env"
        env_prefix = "STACK_"
