import enum

from dbxio.core.cloud.aws import AWS_ClOUD_PROVIDER_NAME
from dbxio.core.cloud.azure import AZURE_ClOUD_PROVIDER_NAME
from dbxio.core.cloud.client.object_storage import ObjectStorageClient
from dbxio.core.cloud.gcp import GCP_ClOUD_PROVIDER_NAME


@enum.unique
class CloudProvider(str, enum.Enum):
    AWS = AWS_ClOUD_PROVIDER_NAME
    AZURE = AZURE_ClOUD_PROVIDER_NAME
    GCP = GCP_ClOUD_PROVIDER_NAME


__all__ = [
    'ObjectStorageClient',
    'CloudProvider',
]
