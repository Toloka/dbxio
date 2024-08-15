from dbxio.utils.databricks import get_volume_url


def test_get_volume_url():
    assert get_volume_url('catalog', 'schema', 'volume') == '/Volumes/catalog/schema/volume'
    assert get_volume_url('catalog', 'schema', 'volume', 'path') == '/Volumes/catalog/schema/volume/path'
    assert get_volume_url('catalog', 'schema', 'volume', 'path/to/dir') == '/Volumes/catalog/schema/volume/path/to/dir'
