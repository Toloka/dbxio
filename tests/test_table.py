import unittest

import dbxio


class TestTable(unittest.TestCase):
    def setUp(self) -> None:
        self.managed_table = dbxio.Table(table_identifier='database.schema.table')
        self.unmanaged_table = dbxio.Table(
            table_identifier='database.schema.table_name',
            attributes=dbxio.TableAttributes(location='/mnt/some/location'),
        )

    def test_init_from_self_managed(self):
        table = dbxio.Table.from_obj(self.managed_table)
        self.assertFalse(table.is_unmanaged)
        self.assertEqual(table.attributes, dbxio.TableAttributes())
        self.assertEqual(table.table_format, dbxio.TableFormat.DELTA)
        self.assertIsNone(table.schema)

    def test_init_from_str_managed(self):
        table = dbxio.Table.from_obj('database.schema.table')
        self.assertFalse(table.is_unmanaged)
        self.assertEqual(table.attributes, dbxio.TableAttributes())
        self.assertEqual(table.table_format, dbxio.TableFormat.DELTA)
        self.assertIsNone(table.schema)

    def test_init_from_self_unmanaged(self):
        table = dbxio.Table.from_obj(self.unmanaged_table)
        self.assertEqual(table.table_identifier, self.unmanaged_table.table_identifier)
        self.assertTrue(table.is_unmanaged)
        self.assertEqual(table.attributes, self.unmanaged_table.attributes)
        self.assertEqual(table.table_format, dbxio.TableFormat.DELTA)
        self.assertIsNone(table.schema)

    def test_unmanaged_table_name(self):
        self.assertEqual(
            self.unmanaged_table.full_external_path,
            f'{self.unmanaged_table.table_format.name}.`{self.unmanaged_table.attributes.location}`',
        )

    def test_safe_table_identifier(self):
        self.assertEqual(
            dbxio.Table(table_identifier='database.schema.table').safe_table_identifier,
            '`database`.`schema`.`table`',
        )
        self.assertEqual(
            dbxio.Table(table_identifier='database!.schema.table+').safe_table_identifier,
            '`database_`.`schema`.`table_`',
        )
