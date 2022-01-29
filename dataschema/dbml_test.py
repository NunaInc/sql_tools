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
"""Tests the conversion to dbml."""

import unittest
from dataschema import (python2schema, schema2dbml, schema_example,
                        schema_test_data)

DBML_SNIPPET = """Table TestProto {
    id Int64 [not null, primary key]
    fint32 UInt32 [not null]
    fsint32 Int32 [not null]
    fint64 UInt64 [not null]
    fsint64 Int64
    fdouble Float64
    ffloat Float32
    fstring LowCardinality(String)
    fbytes String
    fdate Date
    ftimestamp DateTime64
    fdqannotated String
    frep_seq Array(String)
    frep_array Array(String)
    frep_set Array(String [not null])
    fdecimal_bigint Decimal(30, 0)
    fdecimal_default Decimal(30, 20)
    fdecimal_bigdecimal Decimal(30, 20)
    with__original__name String
    finitialized Array(String)
    fwidth String
    flob String
    fcommented String
    fboolean UInt8
}
"""


class DbmlTest(unittest.TestCase):

    def test_schema2dbml(self):
        table = python2schema.ConvertDataclass(schema_test_data.TestProto)
        dbml = schema2dbml.TableConverter(table).to_dbml()
        self.assertEqual(dbml, DBML_SNIPPET)

    def test_example(self):
        table = python2schema.ConvertDataclass(schema_example.Example)
        dbml = schema2dbml.TableConverter(table).to_dbml()
        print(f'Example:\n{dbml}')


if __name__ == '__main__':
    unittest.main()
