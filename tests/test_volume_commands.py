from textwrap import dedent
from unittest import TestCase
from unittest.mock import patch

import pytest

from dbxio import ClusterType
from dbxio.core.client import DbxIOClient
from dbxio.volume.volume_commands import (
    Volume,
    get_comment_on_volume,
    get_tags_on_volume,
    set_comment_on_volume,
    set_tags_on_volume,
    unset_comment_on_volume,
    unset_tags_on_volume,
)
from tests.mocks.azure import MockDefaultAzureCredential
from tests.mocks.sql import flatten_query, sql_mock


class TestVolumeCommands(TestCase):
    def setUp(self):
        self.volume = Volume(catalog='catalog', schema='schema', name='volume')
        self.client = DbxIOClient.from_cluster_settings(
            http_path='test_sql_endpoint_path',
            server_hostname='test_host_name',
            cluster_type=ClusterType.SQL_WAREHOUSE,
            az_cred_provider=MockDefaultAzureCredential(),
        )

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_set_comment_on_volume(self, mock_sql):
        set_comment_on_volume(self.volume, 'test_comment', self.client)
        mock_sql.assert_called_once_with('''COMMENT ON VOLUME `catalog`.`schema`.`volume` IS "test_comment"''')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_set_comment_null_on_volume(self, mock_sql):
        set_comment_on_volume(self.volume, None, self.client)
        mock_sql.assert_called_once_with('COMMENT ON VOLUME `catalog`.`schema`.`volume` IS NULL')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_unset_comment_on_volume(self, mock_sql):
        unset_comment_on_volume(self.volume, self.client)
        mock_sql.assert_called_once_with('COMMENT ON VOLUME `catalog`.`schema`.`volume` IS NULL')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_get_comment_on_volume(self, mock_sql):
        get_comment_on_volume(self.volume, self.client)
        expected_query = dedent("""
        select comment
        from system.information_schema.volumes
        where
            catalog_name = 'catalog'
            and schema_name = 'schema'
            and volume_name = 'volume'
        """)
        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_set_tags_on_volume(self, mock_sql):
        set_tags_on_volume(self.volume, {'tag1': 'value1', 'tag2': 'value2'}, self.client)
        expected_query = dedent("""
        ALTER VOLUME `catalog`.`schema`.`volume`
        SET TAGS ("tag1" = "value1", "tag2" = "value2")
        """)
        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_set_tags_on_volume_non_stings(self, mock_sql):
        set_tags_on_volume(self.volume, {'tag1': 1, 'tag2': 2, 1: [1, 2, 3]}, self.client)
        expected_query = dedent("""
        ALTER VOLUME `catalog`.`schema`.`volume`
        SET TAGS ("tag1" = "1", "tag2" = "2", "1" = "[1, 2, 3]")
        """)
        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)

    def test_set_tags_on_volume_empty_dict(self):
        with pytest.raises(ValueError):
            set_tags_on_volume(self.volume, {}, self.client)

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_unset_tags_on_volume(self, mock_sql):
        unset_tags_on_volume(self.volume, ['tag1', 'tag2'], self.client)
        expected_query = dedent("""
        ALTER VOLUME `catalog`.`schema`.`volume`
        UNSET TAGS ("tag1", "tag2")
        """)
        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)

    def test_unset_tags_on_volume_empty_list(self):
        with pytest.raises(ValueError):
            unset_tags_on_volume(self.volume, [], self.client)

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_get_tags_on_volume(self, mock_sql):
        get_tags_on_volume(self.volume, self.client)
        expected_query = dedent("""
        select tag_name, tag_value
        from system.information_schema.volume_tags
        where
            catalog_name = 'catalog'
            and schema_name = 'schema'
            and volume_name = 'volume'
        """)
        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)
