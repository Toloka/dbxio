from abc import ABC, abstractmethod
from functools import cache
from typing import Optional, Union

import attrs
from azure.identity import DefaultAzureCredential
from cachetools import TTLCache, cachedmethod

from dbxio.core.auth import (
    AZ_CRED_PROVIDER_TYPE,
    ClusterCredentials,
    SemiConfiguredClusterCredentials,
    check_is_airflow_installed,
    get_airflow_variables,
    get_local_variables,
)
from dbxio.core.exceptions import InsufficientCredentialsError, UnavailableAuthError
from dbxio.utils.databricks import ClusterType
from dbxio.utils.logging import get_logger

logger = get_logger()


class BaseAuthProvider(ABC):
    """
    Interface for the authentication provider.
    The authentication provider is responsible for fetching credentials to Databricks.
    """

    def __init__(self, **kwargs):
        raise NotImplementedError

    @property
    @abstractmethod
    def cluster_type(self) -> ClusterType:
        raise NotImplementedError

    @property
    @abstractmethod
    def az_cred_provider(self) -> AZ_CRED_PROVIDER_TYPE:
        raise NotImplementedError

    @property
    @abstractmethod
    def semi_configured_credentials(self) -> Optional[SemiConfiguredClusterCredentials]:
        raise NotImplementedError

    @abstractmethod
    def get_credentials(
        self,
        semi_configured_credentials: SemiConfiguredClusterCredentials = None,
    ) -> ClusterCredentials:
        raise NotImplementedError

    @classmethod
    def from_semi_configured_credentials(
        cls,
        http_path: str,
        server_hostname: str,
        cluster_type: ClusterType,
        az_cred_provider: AZ_CRED_PROVIDER_TYPE = None,
    ) -> 'BaseAuthProvider':
        semi_configured_credentials = cls.build_semi_configured_credentials(http_path, server_hostname)
        assert semi_configured_credentials is not None, 'http_path and server_hostname must be provided together'

        return cls(
            cluster_type=cluster_type,
            az_cred_provider=az_cred_provider or DefaultAzureCredential(),
            semi_configured_credentials=semi_configured_credentials,
        )

    @staticmethod
    def build_semi_configured_credentials(
        http_path: str = None,
        server_hostname: str = None,
    ) -> Union[SemiConfiguredClusterCredentials, None]:
        if http_path is None or server_hostname is None:
            return None

        return SemiConfiguredClusterCredentials(
            http_path=http_path,
            server_hostname=server_hostname,
        )


@attrs.define(slots=True)
class ClusterEnvAuthProvider(BaseAuthProvider):
    """
    Environment variables credential provider for Databricks.
    Assumes that either the environment variables are set or http path and server hostname are provided.
    """

    cluster_type: ClusterType = attrs.field(validator=attrs.validators.instance_of(ClusterType))
    az_cred_provider: AZ_CRED_PROVIDER_TYPE = attrs.field(factory=DefaultAzureCredential)

    semi_configured_credentials: Optional[SemiConfiguredClusterCredentials] = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(SemiConfiguredClusterCredentials)),
    )

    _cache: TTLCache = attrs.Factory(lambda: TTLCache(maxsize=1024, ttl=60 * 15))

    @cachedmethod(lambda self: self._cache)
    def get_credentials(self) -> ClusterCredentials:
        try:
            return get_local_variables(self.az_cred_provider, self.semi_configured_credentials)
        except (TypeError, ValueError):
            logger.debug('ClusterEnvAuthProvider is not available. Not all environment variables are configured.')
            raise InsufficientCredentialsError


@attrs.define(slots=True)
class ClusterAirflowAuthProvider(BaseAuthProvider):
    """
    Credential provider for Databricks using Airflow variables.
    """

    cluster_type: ClusterType = attrs.field(validator=attrs.validators.instance_of(ClusterType))
    az_cred_provider: AZ_CRED_PROVIDER_TYPE = attrs.field(factory=DefaultAzureCredential)

    semi_configured_credentials: Optional[SemiConfiguredClusterCredentials] = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(SemiConfiguredClusterCredentials)),
    )

    _cache: TTLCache = attrs.Factory(lambda: TTLCache(maxsize=1024, ttl=60 * 15))

    @cachedmethod(lambda self: self._cache)
    def get_credentials(
        self,
    ) -> ClusterCredentials:
        try:
            if not check_is_airflow_installed():
                logger.debug('ClusterAirflowAuthProvider is not available. Airflow is not installed.')
                raise InsufficientCredentialsError
            return get_airflow_variables(self.cluster_type, self.semi_configured_credentials)
        except TypeError:
            logger.debug('ClusterAirflowAuthProvider is not available. Some environment variables are not configured.')
            raise InsufficientCredentialsError
        except KeyError:
            logger.debug('ClusterAirflowAuthProvider is not available. Airflow variables are not configured.')
            raise InsufficientCredentialsError


class DefaultCredentialProvider:
    """
    Default credential provider for Databricks.
    It tries to get the credentials from all registered providers. If credentials are provided, it stops.
    """

    def __init__(
        self,
        cluster_type: ClusterType,
        az_cred_provider: AZ_CRED_PROVIDER_TYPE = None,
        http_path: str = None,
        server_hostname: str = None,
        lazy: bool = True,
    ):
        self.cluster_type = cluster_type
        self.az_cred_provider = az_cred_provider or DefaultAzureCredential()
        self.semi_configured_credentials = BaseAuthProvider.build_semi_configured_credentials(
            http_path, server_hostname
        )
        if http_path or server_hostname:
            assert (
                self.semi_configured_credentials is not None
            ), 'http_path and server_hostname must be provided together'

        self._chain = [ClusterAirflowAuthProvider, ClusterEnvAuthProvider]

        self._successful_provider: Union[BaseAuthProvider, None] = None
        if not lazy:
            assert (
                self.semi_configured_credentials is not None
            ), 'semi_configured_credentials must be provided if not lazy'
            self.ensure_set_auth_provider()

    def ensure_set_auth_provider(self) -> None:
        for provider_type in self._chain:
            try:
                provider = provider_type(
                    cluster_type=self.cluster_type,
                    az_cred_provider=self.az_cred_provider,
                    semi_configured_credentials=self.semi_configured_credentials,
                )
                provider.get_credentials()
                self._successful_provider = provider
                return
            except InsufficientCredentialsError:
                pass
        raise UnavailableAuthError('No available provider found')

    def get_credentials(self) -> ClusterCredentials:
        if self._successful_provider is None:
            self.ensure_set_auth_provider()

        assert self._successful_provider is not None
        return self._successful_provider.get_credentials()


@attrs.frozen(slots=True)
class BareAuthProvider(BaseAuthProvider):
    """
    Bare credentials provider for Databricks. This provider is used when the user provides the credentials directly.
    """

    access_token: str = attrs.field(validator=attrs.validators.instance_of(str))
    server_hostname: str = attrs.field(validator=attrs.validators.instance_of(str))
    http_path: str = attrs.field(validator=attrs.validators.instance_of(str))
    cluster_type: ClusterType = attrs.field(validator=attrs.validators.instance_of(ClusterType))
    az_cred_provider: AZ_CRED_PROVIDER_TYPE = attrs.field(factory=DefaultAzureCredential)

    semi_configured_credentials: None = attrs.field(default=None, init=False)

    @cache
    def get_credentials(self, **kwargs) -> ClusterCredentials:
        return ClusterCredentials(
            access_token=self.access_token,
            server_hostname=self.server_hostname,
            http_path=self.http_path,
        )
