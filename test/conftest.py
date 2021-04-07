"""
pytest conftest
"""

# import pytest

# @pytest.fixture(autouse=True)
# def testing_env_var(monkeypatch):
#     """Environment for testing"""

#     # Set fake env to make sure we don't hit AWS services
#     monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fsl")
#     monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "mao")
#     monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
#     monkeypatch.delenv("AWS_PROFILE", raising=False)
#     monkeypatch.setenv("AWS_CONFIG_FILE", "/tmp/noconfigheere")
#     monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/tmp/noconfighereeither")
#     monkeypatch.setenv("LOCALSTACK_HOSTNAME", "localhost")
#     monkeypatch.setenv("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
