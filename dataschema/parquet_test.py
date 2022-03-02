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
"""Tests the conversion to and from parquet."""

import unittest
from dataclasses import dataclass
from dataschema import (Schema, parquet2schema, python2schema, schema2parquet,
                        schema_example, schema_test_data)
from typing import Dict, List, Optional, Set


@dataclass
class Element:
    foo: str
    bar: Optional[int]


@dataclass
class SuperElement:
    baz: int
    element: Element
    optelement: Optional[Element]


@dataclass
class TestStruct:
    element: Element
    olist: List[Optional[int]]
    elist: List[Element]
    eset: Set[Element]
    emap: Dict[int, Optional[Element]]
    super_element: Optional[SuperElement]


PARQUET_SCHEMA = """id: int64 not null
fint32: uint32 not null
fsint32: int32 not null
fint64: uint64 not null
fsint64: int64
fdouble: double
ffloat: float
fstring: string
fbytes: binary
fdate: date32[day]
ftimestamp: timestamp[ms, tz=Etc/UTC]
fdqannotated: string
frep_seq: list<element: string not null>
  child 0, element: string not null
frep_array: list<element: string not null>
  child 0, element: string not null
frep_set: list<element: string not null>
  child 0, element: string not null
fdecimal_bigint: decimal128(30, 0)
fdecimal_default: decimal128(30, 20)
fdecimal_bigdecimal: decimal128(30, 20)
with__original__name: string
finitialized: list<element: string not null>
  child 0, element: string not null
fwidth: string
flob: large_string
fcommented: string
fboolean: bool"""

ELEMENT_PARQUET_SCHEMA = """element: struct<foo: string not null, bar: int64> not null
  child 0, foo: string not null
  child 1, bar: int64
olist: list<element: int64>
  child 0, element: int64
elist: list<element: struct<foo: string not null, bar: int64> not null>
  child 0, element: struct<foo: string not null, bar: int64> not null
      child 0, foo: string not null
      child 1, bar: int64
eset: list<element: struct<foo: string not null, bar: int64> not null>
  child 0, element: struct<foo: string not null, bar: int64> not null
      child 0, foo: string not null
      child 1, bar: int64
emap: map<int64, struct<foo: string not null, bar: int64>>
  child 0, entries: struct<key: int64 not null, value: struct<foo: string not null, bar: int64>> not null
      child 0, key: int64 not null
      child 1, value: struct<foo: string not null, bar: int64>
          child 0, foo: string not null
          child 1, bar: int64
super_element: struct<baz: int64 not null, element: struct<foo: string not null, bar: int64> not null, optelement: struct<foo: string not null, bar: int64>>
  child 0, baz: int64 not null
  child 1, element: struct<foo: string not null, bar: int64> not null
      child 0, foo: string not null
      child 1, bar: int64
  child 2, optelement: struct<foo: string not null, bar: int64>
      child 0, foo: string not null
      child 1, bar: int64"""


class ParquetTest(unittest.TestCase):

    def test_schema2parquet(self):
        table = python2schema.ConvertDataclass(schema_test_data.TestProto)
        pq_schema = schema2parquet.ConvertTable(table)
        self.assertEqual(f'{pq_schema}', PARQUET_SCHEMA)
        rtable = parquet2schema.ConvertSchema(pq_schema,
                                              'pyschema.test.TestProto')
        diffs = table.compare(rtable)
        self.assertEqual(len(diffs), 2)
        # Internal per no sets in parquet.
        self.assertEqual(diffs[0].code, Schema.SchemaDiffCode.TABLE_NAMES)
        self.assertEqual(diffs[1].code,
                         Schema.SchemaDiffCode.REPEATED_STRUCT_MISMATCH)
        self.assertEqual(diffs[1].column, 'frep_set')

    def test_example(self):
        table = python2schema.ConvertDataclass(schema_example.Example)
        pq_schema = schema2parquet.ConvertTable(table)
        print(f'Example:\n{pq_schema}')

    def test_nested(self):
        table = python2schema.ConvertDataclass(TestStruct)
        pq_schema = schema2parquet.ConvertTable(table)
        self.assertEqual(f'{pq_schema}', ELEMENT_PARQUET_SCHEMA)
        rtable = parquet2schema.ConvertSchema(pq_schema,
                                              'pyschema.test.TestStruct')
        diffs = table.compare(rtable)
        # Internal structure differences per repeated / array & no sets in parquet.
        self.assertEqual(len(diffs), 3)
        self.assertEqual(diffs[0].code, Schema.SchemaDiffCode.TABLE_NAMES)
        self.assertEqual(diffs[1].code,
                         Schema.SchemaDiffCode.REPEATED_STRUCT_MISMATCH)
        self.assertEqual(diffs[1].column, 'elist')
        self.assertEqual(diffs[2].code,
                         Schema.SchemaDiffCode.ARRAY_SET_MISMATCH)
        self.assertEqual(diffs[2].column, 'eset')


if __name__ == '__main__':
    unittest.main()
