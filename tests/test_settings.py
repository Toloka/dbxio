import pytest

from dbxio.core.cloud import CloudProvider
from dbxio.core.settings import CLOUD_PROVIDER_ENV_VAR, Settings


def test_settings_default_cloud_provider():
    settings = Settings()
    assert settings.cloud_provider == CloudProvider.AZURE


def test_settings_custom_cloud_provider():
    settings = Settings(cloud_provider=CloudProvider.NEBIUS_OVER_AZURE)
    assert settings.cloud_provider == CloudProvider.NEBIUS_OVER_AZURE


def test_settings_invalid_cloud_provider():
    with pytest.raises(ValueError):
        Settings(cloud_provider='invalid_cloud_provider')


def test_settings_unsupported_cloud_provider():
    with pytest.raises(ValueError):
        Settings(cloud_provider=CloudProvider.AWS)


def test_settings_cloud_provider_from_env(monkeypatch):
    monkeypatch.setenv(CLOUD_PROVIDER_ENV_VAR, 'nebius_over_azure')
    settings = Settings()
    assert settings.cloud_provider == 'nebius_over_azure'
