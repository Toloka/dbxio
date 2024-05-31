import os
import uuid
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Union

import attrs
from databricks.sdk.service.catalog import VolumeType

from dbxio.blobs.block_upload import upload_file
from dbxio.sql.results import _FutureBaseResult
from dbxio.utils.blobs import blobs_registries, get_blob_servie_client
from dbxio.utils.databricks import compile_full_storage_location_path, get_storage_name_from_external_location
from dbxio.utils.logging import get_logger

if TYPE_CHECKING:
    from dbxio.core.client import DbxIOClient

DEFAULT_EXTERNAL_LOCATION = 'default_external_location'

logger = get_logger()


@attrs.define(slots=True)
class Volume:
    """
    Represents a volume in Databricks.

    Storage_location is required for external volumes.
    It must satisfy the following pattern:
        abfss://<container_name>@<storage_name>.dfs.core.windows.net/<blobs_path>}
    """

    catalog: str = attrs.field(validator=attrs.validators.instance_of(str))
    schema: str = attrs.field(validator=attrs.validators.instance_of(str))
    name: str = attrs.field(validator=attrs.validators.instance_of(str))
    volume_type: VolumeType = attrs.field(
        default=VolumeType.MANAGED,
        validator=attrs.validators.instance_of(VolumeType),
    )
    storage_location: Union[str, None] = None

    def __attrs_post_init__(self):
        if self.volume_type == VolumeType.EXTERNAL and not self.storage_location:
            raise ValueError('storage_location is required for external volumes')

    @property
    def full_name(self):
        return f'{self.catalog}.{self.schema}.{self.name}'

    @property
    def safe_full_name(self):
        return f'`{self.catalog}`.`{self.schema}`.`{self.name}`'

    @property
    def mount_path(self):
        return f'/Volumes/{self.catalog}/{self.schema}/{self.name}'

    def is_managed(self):
        return self.volume_type is VolumeType.MANAGED

    def is_external(self):
        return self.volume_type is VolumeType.EXTERNAL


def create_volume(volume: Volume, client: 'DbxIOClient') -> None:
    client.workspace_api.volumes.create(
        catalog_name=volume.catalog,
        schema_name=volume.schema,
        name=volume.name,
        volume_type=volume.volume_type,
        storage_location=volume.storage_location,
    )
    logger.info(f'Volume {volume.safe_full_name} was successfully created.')


def _exists_volume(volume: Volume, client: 'DbxIOClient') -> bool:
    for v in client.workspace_api.volumes.list(catalog_name=volume.catalog, schema_name=volume.schema):
        if v.name == volume.name:
            return True

    return False


def write_volume(
    path: Union[str, Path],
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    client: 'DbxIOClient',
    create_volume_if_not_exists: bool = True,
    max_concurrency: int = 1,
    force: bool = False,
) -> None:
    """
    writes the contents of a local directory to a volume in Databricks.

    :param path: local directory or file to upload
    :param catalog_name: databricks catalog name
    :param schema_name: databricks schema name
    :param volume_name: volume name
    :param client: dbxio client
    :param create_volume_if_not_exists: flag to create the volume if it does not exist
    :param max_concurrency: the maximum number of threads to use for uploading files
    :param force: a flag to break leases if the blob is already in use
    """
    path = Path(path)

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
    storage_name = get_storage_name_from_external_location(storage_url, default_external_location)

    blob_service_client = get_blob_servie_client(storage_name, client.credential_provider.az_cred_provider)

    operation_uuid = str(uuid.uuid4())
    with blobs_registries(blob_service_client, default_external_location, keep_blobs=True) as (blobs, metablobs):
        # here all files in the path, including subdirectories, are uploaded to the blob storage.
        # only "hidden" files (those starting with a dot) are skipped
        for file in path.glob('**/*'):
            if file.is_file() and not file.name.startswith('.'):
                upload_file(
                    path=file,
                    local_path=path,
                    blob_service_client=blob_service_client,
                    container_name=default_external_location,
                    operation_uuid=operation_uuid,
                    blobs=blobs,
                    metablobs=metablobs,
                    max_concurrency=max_concurrency,
                    force=force,
                )

        storage_location = compile_full_storage_location_path(
            container_name=default_external_location,
            storage_name=storage_name,
            blobs_path=os.path.commonpath(blobs),
        )
    volume = Volume(
        catalog=catalog_name,
        schema=schema_name,
        name=volume_name,
        volume_type=VolumeType.EXTERNAL,
        storage_location=storage_location,
    )

    if not _exists_volume(volume, client):
        if create_volume_if_not_exists:
            create_volume(volume=volume, client=client)
        else:
            raise ValueError(
                f'Volume {volume_name} does not exist in schema {schema_name} of catalog '
                f'{catalog_name} and create_volume_if_not_exists is False.'
            )


def set_tags_on_volume(volume: Volume, tags: dict[str, str], client: 'DbxIOClient') -> _FutureBaseResult:
    """
    Sets tags on a volume.
    Each tag is a key-value pair of strings.
    """
    assert tags, 'tags must be a non-empty dictionary'

    set_tags_query = dedent(f"""
    ALTER VOLUME {volume.safe_full_name}
    SET TAGS ({', '.join([f'"{k}" = "{v}"' for k, v in tags.items()])})
    """)

    return client.sql(set_tags_query)


def unset_tags_on_volume(volume: Volume, tags: list[str], client: 'DbxIOClient') -> _FutureBaseResult:
    """
    Unsets tags on a volume.
    """
    assert tags, 'tags must be a non-empty list'

    unset_tags_query = dedent(f"""
    ALTER VOLUME {volume.safe_full_name}
    UNSET TAGS ({', '.join([f'"{tag}"' for tag in tags])})
    """)

    return client.sql(unset_tags_query)


def get_tags_on_volume(volume: Volume, client: 'DbxIOClient') -> dict[str, str]:
    """
    Returns the tags on a volume.
    """
    information_schema_query = dedent(f"""
    select tag_name, tag_value
    from system.information_schema.volume_tags
    where
        catalog_name = '{volume.catalog}'
        and schema_name = '{volume.schema}'
        and volume_name = '{volume.name}'
    """)

    tags = {}
    with client.sql(information_schema_query) as result:
        for row in result:
            tags[row['tag_name']] = row['tag_value']

    return tags


def set_comment_on_volume(
    volume: Volume,
    comment: Union[str, None],
    client: 'DbxIOClient',
) -> _FutureBaseResult:
    """
    Sets a comment on a volume.
    Description comment supports Markdown.

    If the comment is None, the comment will be removed.
    """
    set_comment_query = dedent(
        f"""COMMENT ON VOLUME {volume.safe_full_name} IS {f'"{comment}"' if comment else 'NULL'}"""
    )

    return client.sql(set_comment_query)


def unset_comment_on_volume(volume: Volume, client: 'DbxIOClient') -> _FutureBaseResult:
    """
    Unsets the comment on a volume.
    """
    return set_comment_on_volume(volume=volume, comment=None, client=client)


def get_comment_on_volume(volume: Volume, client: 'DbxIOClient') -> Union[str, None]:
    """
    Returns the comment on a volume.
    """
    information_schema_query = dedent(f"""
    select comment
    from system.information_schema.volumes
    where
        catalog_name = '{volume.catalog}'
        and schema_name = '{volume.schema}'
        and volume_name = '{volume.name}'
    """)

    with client.sql(information_schema_query) as result:
        for row in result:
            return row['comment']

    return None
