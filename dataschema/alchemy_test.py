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
from dataschema import (python2schema, schema2sqlalchemy, schema_example,
                        schema_test_data)
from sqlalchemy.schema import CreateTable

EXPECTED_CREATE_TABLE = """
CREATE TABLE "TestProto" (
	id INTEGER NOT NULL,
	fint32 INTEGER NOT NULL,
	fsint32 INTEGER NOT NULL,
	fint64 INTEGER NOT NULL,
	fsint64 INTEGER,
	fdouble REAL,
	ffloat FLOAT,
	fstring VARCHAR,
	fbytes VARBINARY,
	fdate DATE,
	ftimestamp DATETIME,
	fdqannotated VARCHAR,
	frep_seq ARRAY,
	frep_array ARRAY,
	frep_set ARRAY,
	fdecimal_bigint DECIMAL(30, 0),
	fdecimal_default DECIMAL(30, 20),
	fdecimal_bigdecimal DECIMAL(30, 20),
	with__original__name VARCHAR,
	finitialized ARRAY,
	fwidth VARCHAR,
	flob VARCHAR,
	fcommented VARCHAR,
	fboolean BOOLEAN,
	PRIMARY KEY (id)
)

"""


class AlchemyTest(unittest.TestCase):

    def test_schema2alchemy(self):
        table = python2schema.ConvertDataclass(schema_test_data.TestProto)
        alchemy = schema2sqlalchemy.ConvertTable(table)
        self.assertEqual(
            str(CreateTable(alchemy)).replace(', \n', ',\n'),
            EXPECTED_CREATE_TABLE)

    def test_example(self):
        table = python2schema.ConvertDataclass(schema_example.Example)
        alchemy = schema2sqlalchemy.ConvertTable(table)
        print(f'Example:\n{CreateTable(alchemy)}')


if __name__ == '__main__':
    unittest.main()
