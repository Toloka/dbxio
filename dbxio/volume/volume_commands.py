from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Optional, Union

import attrs
from databricks.sdk.errors.platform import NotFound, ResourceDoesNotExist
from databricks.sdk.service.catalog import VolumeType

from dbxio.blobs.block_upload import upload_file
from dbxio.blobs.download import download_blob_tree
from dbxio.core.cloud.client.object_storage import ObjectStorageClient
from dbxio.sql.results import _FutureBaseResult
from dbxio.utils.blobs import blobs_registries
from dbxio.utils.databricks import get_external_location_storage_url, get_volume_url
from dbxio.utils.logging import get_logger

if TYPE_CHECKING:
    from dbxio.core.client import DbxIOClient

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
    path: str = attrs.field(default='', validator=attrs.validators.instance_of(str))
    volume_type: VolumeType = attrs.field(
        default=VolumeType.MANAGED,
        validator=attrs.validators.instance_of(VolumeType),
    )
    storage_location: Union[str, None] = None

    @classmethod
    def from_url(cls, url: str, client: 'DbxIOClient') -> 'Volume':
        """
        Creates a Volume object from a URL.
        URL pattern must be:
            /Volumes/<catalog>/<schema>/<volume_name>/path
        """
        if not url.startswith('/Volumes/'):
            raise ValueError('URL must start with /Volumes/')

        _, catalog, schema, name, *path = url.lstrip('/').split('/')
        raw_volume_info = client.retrying(client.workspace_api.volumes.read, f'{catalog}.{schema}.{name}')
        return cls(
            catalog=catalog,
            schema=schema,
            name=name,
            path='/'.join(path),
            volume_type=VolumeType(raw_volume_info.volume_type),
            storage_location=raw_volume_info.storage_location,
        )

    def __truediv__(self, path: str):
        return Volume(
            catalog=self.catalog,
            schema=self.schema,
            name=self.name,
            path=str(Path(self.path) / Path(path)),
            volume_type=self.volume_type,
            storage_location=self.storage_location,
        )

    @property
    def full_name(self):
        return f'{self.catalog}.{self.schema}.{self.name}'

    @property
    def safe_full_name(self):
        return f'`{self.catalog}`.`{self.schema}`.`{self.name}`'

    @property
    def mount_path(self):
        return str(Path(f'/Volumes/{self.catalog}/{self.schema}/{self.name}') / self.path)

    @property
    def mount_start_point(self):
        return f'volume__{self.name}'

    def is_managed(self):
        return self.volume_type is VolumeType.MANAGED

    def is_external(self):
        return self.volume_type is VolumeType.EXTERNAL


def create_volume(volume: Volume, client: 'DbxIOClient', skip_if_exists: bool = True) -> None:
    if skip_if_exists and exists_volume(volume.catalog, volume.schema, volume.name, client):
        logger.info(f'Volume {volume.safe_full_name} already exists, skipping creation.')
        return
    client.retrying(
        client.workspace_api.volumes.create,
        catalog_name=volume.catalog,
        schema_name=volume.schema,
        name=volume.name,
        volume_type=volume.volume_type,
        storage_location=volume.storage_location,
    )
    logger.info(f'Volume {volume.safe_full_name} was successfully created.')


def exists_volume(catalog_name: str, schema_name: str, volume_name: str, client: 'DbxIOClient') -> bool:
    for v in client.retrying(client.workspace_api.volumes.list, catalog_name=catalog_name, schema_name=schema_name):
        if v.name == volume_name:
            return True

    return False


def _download_external_volume(local_path: Path, storage_location: str, volume_path: str, client: 'DbxIOClient') -> None:
    object_storage = ObjectStorageClient.from_url(storage_location)
    assert object_storage.blobs_path, f'Object storage client must have a blobs path, got {object_storage=}'
    download_blob_tree(
        object_storage_client=object_storage,
        local_path=local_path,
        prefix_path=str(Path(object_storage.blobs_path) / Path(volume_path)),
        client=client,
    )


def _download_single_file_from_managed_volume(local_path: Path, file_path: str, client: 'DbxIOClient'):
    with open(local_path / Path(file_path).name, 'wb') as f:
        response_content = client.retrying(client.workspace_api.files.download, file_path).contents
        if response_content:
            f.write(response_content.read())
        else:
            raise ValueError(f'Failed to download file {file_path}, got None')


def _check_if_path_is_remote_file(path: str, client: 'DbxIOClient') -> bool:
    try:
        client.retrying(client.workspace_api.files.get_metadata, path)
        return True
    except NotFound:
        return False


def _download_managed_volume(local_path: Path, volume: Volume, client: 'DbxIOClient'):
    if _check_if_path_is_remote_file(volume.mount_path, client):
        _download_single_file_from_managed_volume(local_path, volume.mount_path, client)
        return
    for file in client.retrying(client.workspace_api.files.list_directory_contents, volume.mount_path):
        if file.name is None or file.path is None:
            raise ValueError(f'File {file} has no name or path')

        if file.is_directory:
            (local_path / file.name).mkdir(parents=True, exist_ok=True)
            _download_managed_volume(local_path / file.name, volume / file.name, client)
        else:
            _download_single_file_from_managed_volume(local_path, file.path, client)


def download_volume(
    path: Union[str, Path],
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    client: 'DbxIOClient',
    volume_path: Optional[str] = None,
) -> Path:
    """
    Downloads files from a volume in Databricks to a local directory.
    Volume must be an external volume.

    :param path: local directory to download files to. Must be a directory.
    :param catalog_name: databricks catalog name
    :param schema_name: databricks schema name
    :param volume_name: volume name
    :param client: dbxio client
    :param volume_path: path in the volume to download from.
    """
    path = Path(path)
    if not path.exists() or not path.is_dir():
        raise ValueError('Path must be an existing directory')

    volume = Volume.from_url(get_volume_url(catalog_name, schema_name, volume_name, volume_path), client)
    logger.info(f'Downloading volume: {volume.full_name}')
    logger.debug(f'Volume type: {volume.volume_type}')

    if volume.volume_type == VolumeType.EXTERNAL:
        assert volume.storage_location, f'External volume must have a storage location, got {volume=}'
        _download_external_volume(
            local_path=path,
            storage_location=str(volume.storage_location),
            volume_path=volume.path,
            client=client,
        )
    elif volume.volume_type == VolumeType.MANAGED:
        _download_managed_volume(local_path=path, volume=volume, client=client)
    else:
        raise ValueError(f'Unknown volume type: {volume.volume_type}')

    logger.info(f'Volume {volume.full_name} was successfully downloaded to {path}')

    return path / volume.path


def _write_external_volume(
    path: Path,
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    volume_path: str,
    client: 'DbxIOClient',
    max_concurrency: int,
    create_volume_if_not_exists: bool,
    force: bool,
):
    volume_exists = exists_volume(catalog_name, schema_name, volume_name, client)
    if volume_exists:
        volume = Volume.from_url(get_volume_url(catalog_name, schema_name, volume_name), client=client)
        assert volume.storage_location, f'External volume must have a storage location, got {volume=}'
        object_storage_client = ObjectStorageClient.from_url(volume.storage_location)
        assert (
            object_storage_client.blobs_path
        ), f'Object storage client must have a blobs path, got {object_storage_client=}'
        prefix_blob_path = str(Path(object_storage_client.blobs_path.rstrip('/')) / volume_path)
    else:
        volume = Volume(
            catalog=catalog_name,
            schema=schema_name,
            name=volume_name,
            path=volume_path,
            volume_type=VolumeType.EXTERNAL,
        )
        object_storage_client = ObjectStorageClient.from_url(get_external_location_storage_url(catalog_name, client))
        prefix_blob_path = str(Path(volume.mount_start_point) / volume_path)

    with blobs_registries(object_storage_client, retrying=client.retrying, keep_blobs=True) as (blobs, metablobs):
        # here all files in the path, including subdirectories, are uploaded to the blob storage.
        # only "hidden" files (those starting with a dot) are skipped
        files_to_upload = path.glob('**/*') if path.is_dir() else [path]
        for file in files_to_upload:
            if file.is_file() and not file.name.startswith('.'):
                client.retrying(
                    upload_file,
                    path=file,
                    local_path=path,
                    prefix_blob_path=prefix_blob_path,
                    object_storage_client=object_storage_client,
                    blobs=blobs,
                    metablobs=metablobs,
                    max_concurrency=max_concurrency,
                    force=force,
                )
    if volume_exists:
        return

    object_storage_client.blobs_path = volume.mount_start_point
    volume.storage_location = object_storage_client.to_url()
    create_volume(volume=volume, client=client)


def _write_managed_volume(
    path: Path,
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    volume_path: str,
    client: 'DbxIOClient',
    max_concurrency: int,
    create_volume_if_not_exists: bool,
    force: bool,
):
    volume = Volume(
        catalog=catalog_name,
        schema=schema_name,
        name=volume_name,
        path=volume_path,
        volume_type=VolumeType.MANAGED,
    )
    create_volume(volume=volume, client=client)

    files_to_upload = path.glob('**/*') if path.is_dir() else [path]
    for file in files_to_upload:
        if file.is_file() and not file.name.startswith('.'):
            file_name: Union[Path, str] = file.relative_to(path) if file != path else file.name
            volume_file_path = str(Path(volume.mount_path) / Path(file_name))
            client.retrying(
                client.workspace_api.files.upload,
                file_path=volume_file_path,
                contents=file.open('rb'),
                overwrite=force,
            )


def write_volume(
    path: Union[str, Path],
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    client: 'DbxIOClient',
    volume_type: VolumeType,
    volume_path: Optional[str] = None,
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
    :param volume_path: path in the volume to write to.
        if None, the root of the volume is used.
    :param client: dbxio client
    :param volume_type: volume type (external or managed)
    :param create_volume_if_not_exists: flag to create the volume if it does not exist
    :param max_concurrency: the maximum number of threads to use for uploading files
    :param force: a flag to break leases if the blob is already in use
    """
    path = Path(path)

    if volume_type is VolumeType.EXTERNAL:
        _write_method = _write_external_volume
    elif volume_type is VolumeType.MANAGED:
        _write_method = _write_managed_volume
    else:
        raise ValueError(f'Unknown volume type: {volume_type}')

    _write_method(
        path=path,
        catalog_name=catalog_name,
        schema_name=schema_name,
        volume_name=volume_name,
        volume_path=volume_path or '',
        client=client,
        max_concurrency=max_concurrency,
        create_volume_if_not_exists=create_volume_if_not_exists,
        force=force,
    )


def set_tags_on_volume(volume: Volume, tags: dict[str, str], client: 'DbxIOClient') -> _FutureBaseResult:
    """
    Sets tags on a volume.
    Each tag is a key-value pair of strings.
    """
    if not tags:
        raise ValueError('tags must be a non-empty dictionary')

    set_tags_query = dedent(f"""
    ALTER VOLUME {volume.safe_full_name}
    SET TAGS ({', '.join([f'"{k}" = "{v}"' for k, v in tags.items()])})
    """)

    return client.sql(set_tags_query)


def unset_tags_on_volume(volume: Volume, tags: list[str], client: 'DbxIOClient') -> _FutureBaseResult:
    """
    Unsets tags on a volume.
    """
    if not tags:
        raise ValueError('tags must be a non-empty list')

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


def drop_volume(volume: Volume, client: 'DbxIOClient', force: bool = False) -> None:
    """
    Deletes a volume in Databricks.
    If the volume is external, it will also delete all blobs in the storage location.
    """
    if volume.volume_type is VolumeType.EXTERNAL:
        assert volume.storage_location, f'External volume must have a storage location, got {volume.storage_location=}'
        object_storage = ObjectStorageClient.from_url(volume.storage_location)
        for blob in object_storage.list_blobs(object_storage.blobs_path):
            object_storage.try_delete_blob(blob.name)
            logger.debug(f'Blob {blob.name} was successfully deleted.')

        logger.info(f'External volume {volume.safe_full_name} was successfully cleaned up.')
    try:
        client.retrying(client.workspace_api.volumes.delete, volume.full_name)
    except ResourceDoesNotExist as e:
        if not force:
            raise e
    logger.info(f'Volume {volume.safe_full_name} was successfully dropped.')
