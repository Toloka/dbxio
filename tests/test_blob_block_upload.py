from pathlib import Path

from dbxio.blobs.block_upload import _local_path_to_blob_name


def test_local_path_to_blob_name():
    assert _local_path_to_blob_name(Path('/folder/file.txt'), Path('/folder'), '') == 'file.txt'
    assert _local_path_to_blob_name(Path('/folder/file.txt'), Path('/folder'), 'prefix') == 'prefix/file.txt'
    assert _local_path_to_blob_name(Path('/folder/file.txt'), Path('/folder/file.txt'), '') == 'file.txt'
    assert _local_path_to_blob_name(Path('/folder/file.txt'), Path('/folder/file.txt'), 'prefix') == 'prefix/file.txt'
    assert _local_path_to_blob_name(Path('/folder/folder2/file.txt'), Path('/folder'), '') == 'folder2/file.txt'
    assert (
        _local_path_to_blob_name(Path('/folder/folder2/file.txt'), Path('/folder'), 'prefix')
        == 'prefix/folder2/file.txt'
    )
