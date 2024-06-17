from dbxio.core.cloud.azure.object_storage import _AzureBlobStorageClientImpl
from dbxio.core.cloud.nebius_over_azure import NEBIUS_OVER_AZURE_ClOUD_PROVIDER_NAME
from dbxio.utils.logging import get_logger

logger = get_logger()


class _NebiusOverAzureBlobStorageClientImpl(_AzureBlobStorageClientImpl):
    cloud_provider_name: str = NEBIUS_OVER_AZURE_ClOUD_PROVIDER_NAME
