import os

import attrs

from dbxio.core.cloud import AZURE_ClOUD_PROVIDER_NAME, NEBIUS_OVER_AZURE_ClOUD_PROVIDER_NAME

_DEFAULT_CLOUD_PROVIDER = AZURE_ClOUD_PROVIDER_NAME  # TODO: get provider from env or remove this default at all
_SUPPORTED_CLOUD_PROVIDERS = [AZURE_ClOUD_PROVIDER_NAME, NEBIUS_OVER_AZURE_ClOUD_PROVIDER_NAME]

CLOUD_PROVIDER_ENV_VAR = 'CLOUD_PROVIDER'


def _cloud_provider_factory() -> str:
    return os.getenv(CLOUD_PROVIDER_ENV_VAR, _DEFAULT_CLOUD_PROVIDER).lower()


@attrs.define
class Settings:
    cloud_provider: str = attrs.field(factory=_cloud_provider_factory)

    @cloud_provider.validator
    def _validate_cloud_provider(self, attribute, value):
        if value not in _SUPPORTED_CLOUD_PROVIDERS:
            raise ValueError(
                f'Unsupported cloud provider: {value}. Supported providers: {_SUPPORTED_CLOUD_PROVIDERS}. '
                f'Please raise an issue if you want to add a new provider '
                f'(https://github.com/Toloka/dbxio/issues/new/choose)'
            )
