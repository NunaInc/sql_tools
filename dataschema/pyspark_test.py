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
"""Tests the conversion to and from PySpark structures."""

import unittest
from dataclasses import dataclass

from dataschema import (Schema, pyspark2schema, python2schema, schema2pyspark,
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


def _rm_spaces(s):
    return s.replace(' ', '').replace('\n', '')


PYSPARK_SCHEMA = """
StructType(List(
    StructField(id,LongType,false),
    StructField(fint32,LongType,false),
    StructField(fsint32,IntegerType,false),
    StructField(fint64,DecimalType(20,0),false),
    StructField(fsint64,LongType,true),
    StructField(fdouble,DoubleType,true),
    StructField(ffloat,FloatType,true),
    StructField(fstring,StringType,true),
    StructField(fbytes,BinaryType,true),
    StructField(fdate,DateType,true),
    StructField(ftimestamp,TimestampType,true),
    StructField(fdqannotated,StringType,true),
    StructField(frep_seq,ArrayType(StringType,false),false),
    StructField(frep_array,ArrayType(StringType,false),false),
    StructField(frep_set,ArrayType(StringType,false),true),
    StructField(fdecimal_bigint,DecimalType(30,0),true),
    StructField(fdecimal_default,DecimalType(30,20),true),
    StructField(fdecimal_bigdecimal,DecimalType(30,20),true),
    StructField(with__original__name,StringType,true),
    StructField(finitialized,ArrayType(StringType,false),false),
    StructField(fwidth,StringType,true),
    StructField(flob,StringType,true),
    StructField(fcommented,StringType,true),
    StructField(fboolean,BooleanType,true)
))
"""

ELEMENT_PYSPARK_SCHEMA = """
StructType(List(
    StructField(element,StructType(List(
        StructField(foo,StringType,false),
        StructField(bar,LongType,true))),false),
    StructField(olist,ArrayType(LongType,true),true),
    StructField(elist,ArrayType(StructType(List(
        StructField(foo,StringType,false),
        StructField(bar,LongType,true))),false),false),
    StructField(eset,ArrayType(StructType(List(
        StructField(foo,StringType,false),
        StructField(bar,LongType,true))),false),true),
    StructField(emap,MapType(
        LongType,
        StructType(List(
            StructField(foo,StringType,false),
            StructField(bar,LongType,true))),true),true),
    StructField(super_element,StructType(List(
        StructField(baz,LongType,false),
        StructField(element,
                    StructType(List(
                        StructField(foo,StringType,false),
                        StructField(bar,LongType,true))),false),
        StructField(optelement,
                    StructType(List(
                        StructField(foo,StringType,false),
                        StructField(bar,LongType,true))),true))),true)))
"""


class PysparkTest(unittest.TestCase):

    def test_schema2pyspark(self):
        table = python2schema.ConvertDataclass(schema_test_data.TestProto)
        struct_type = schema2pyspark.ConvertTable(table)
        # print(f'PySpark schema: {struct_type}')
        self.assertEqual(f'{struct_type}', _rm_spaces(PYSPARK_SCHEMA))
        rtable = pyspark2schema.ConvertStructType(struct_type,
                                                  'pyschema.test.TestProto')
        diffs = table.compare(rtable)
        # Internal per no sets & signs in pyspark.
        self.assertEqual([diff.code for diff in diffs], [
            Schema.SchemaDiffCode.TABLE_NAMES,
            Schema.SchemaDiffCode.TYPES_DIFFERENT_CONVERTIBLE,
            Schema.SchemaDiffCode.TYPES_DIFFERENT_CONVERTIBLE,
            Schema.SchemaDiffCode.DATETIME, Schema.SchemaDiffCode.DATETIME,
            Schema.SchemaDiffCode.REPEATED_STRUCT_MISMATCH
        ])
        self.assertEqual(
            [diff.column for diff in diffs],
            [None, 'fint32', 'fint64', 'ftimestamp', 'ftimestamp', 'frep_set'])

    def test_example(self):
        table = python2schema.ConvertDataclass(schema_example.Example)
        struct_type = schema2pyspark.ConvertTable(table)
        print(f'Example:\n{struct_type}')

    def test_nested(self):
        table = python2schema.ConvertDataclass(TestStruct)
        struct_type = schema2pyspark.ConvertTable(table)
        # print(f'TestStruct:\n{struct_type}')
        self.assertEqual(f'{struct_type}', _rm_spaces(ELEMENT_PYSPARK_SCHEMA))
        rtable = pyspark2schema.ConvertStructType(struct_type,
                                                  'pyschema.test.TestStruct')
        diffs = table.compare(rtable)
        # Internal structure differences per repeated / array & no sets in pyspark.
        self.assertEqual([diff.code for diff in diffs], [
            Schema.SchemaDiffCode.TABLE_NAMES,
            Schema.SchemaDiffCode.REPEATED_STRUCT_MISMATCH
        ])
        self.assertEqual([diff.column for diff in diffs], [None, 'eset'])


if __name__ == '__main__':
    unittest.main()
