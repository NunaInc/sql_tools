#
# nuna_sql_tools: Copyright 2022 Nuna Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Tests conversion to schema from clickhouse sql."""
import unittest
from dataschema import python2schema, schema_test_data, schema2sql
from sql_analyze.grammars.ClickHouse import parse_sql_lib as parse_sql_ch


class CreateSql2SchemaTest(unittest.TestCase):

    def test_convert(self):
        table = python2schema.ConvertDataclass(schema_test_data.TestProto)
        sql = schema2sql.TableConverter(table).to_sql(
            table_name='schema_test_data.TestProto', replication_params='')
        stmt = parse_sql_ch.parse_clickhouse_sql_create(sql)
        self.assertTrue(stmt is not None)
        self.assertTrue(stmt.schema is not None)
        print(stmt.schema)
        sql2 = schema2sql.TableConverter(stmt.schema).to_sql(
            table_name='schema_test_data.TestProto', replication_params='')
        self.assertEqual(sql, sql2)


if __name__ == '__main__':
    unittest.main()
