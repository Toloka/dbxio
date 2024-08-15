from enum import Enum
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from dbxio import DbxIOClient

DEFAULT_EXTERNAL_LOCATION = 'default_external_location'


class ClusterType(Enum):
    ALL_PURPOSE = 'ALL_PURPOSE'
    SQL_WAREHOUSE = 'SQL_WAREHOUSE'


def get_external_location_storage_url(catalog_name: str, client: 'DbxIOClient') -> str:
    catalog = client.workspace_api.catalogs.get(catalog_name)
    catalog_properties = catalog.properties if catalog.properties else {}

    default_external_location = catalog_properties.get(DEFAULT_EXTERNAL_LOCATION)
    if not default_external_location:
        raise ValueError(
            'Catalog does not have a default external location (aka container in storage account). '
            'Ask data engineers for creation of a default external location for your catalog.'
        )
    storage_url = client.workspace_api.external_locations.get(default_external_location).url
    assert storage_url, f'External location {default_external_location} does not have a URL'

    return storage_url


def get_volume_url(catalog_name: str, schema_name: str, volume_name: str, path: Optional[str] = None) -> str:
    url = f'/Volumes/{catalog_name}/{schema_name}/{volume_name}'
    if path:
        url = f'{url}/{path}'
    return url
