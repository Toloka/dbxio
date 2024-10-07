import os
from typing import Type, Union

import attrs

from dbxio.core.cloud import CloudProvider

_DEFAULT_CLOUD_PROVIDER = CloudProvider.AZURE  # TODO: get provider from env or remove this default at all
_SUPPORTED_CLOUD_PROVIDERS = [
    CloudProvider.AZURE,
]

CLOUD_PROVIDER_ENV_VAR = 'CLOUD_PROVIDER'


def _cloud_provider_factory() -> CloudProvider:
    return CloudProvider(os.getenv(CLOUD_PROVIDER_ENV_VAR, _DEFAULT_CLOUD_PROVIDER).lower())


@attrs.frozen
class RetryConfig:
    max_attempts: int = attrs.field(default=7, validator=[attrs.validators.instance_of(int), attrs.validators.ge(1)])
    exponential_backoff_multiplier: Union[float, int] = attrs.field(
        default=1.0,
        validator=[attrs.validators.instance_of((float, int)), attrs.validators.ge(0)],
    )
    extra_exceptions_to_retry: tuple[Type[BaseException]] = attrs.field(
        factory=tuple,
        validator=attrs.validators.deep_iterable(
            member_validator=attrs.validators.instance_of(type),
            iterable_validator=attrs.validators.instance_of(tuple),
        ),
    )


@attrs.define
class Settings:
    cloud_provider: CloudProvider = attrs.field(factory=_cloud_provider_factory)

    retry_config: RetryConfig = attrs.field(factory=RetryConfig)

    @cloud_provider.validator
    def _validate_cloud_provider(self, attribute, value):
        if not isinstance(value, CloudProvider):
            value = CloudProvider(value)
        if value not in _SUPPORTED_CLOUD_PROVIDERS:
            raise ValueError(
                f'Unsupported cloud provider: {value}. Supported providers: {_SUPPORTED_CLOUD_PROVIDERS}. '
                f'Please raise an issue if you want to add a new provider '
                f'(https://github.com/Toloka/dbxio/issues/new/choose)'
            )
