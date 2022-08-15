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
"""Tests various kinds of type composition for fields."""

import dataclasses
import unittest

from dataschema import (python2schema, schema2dbml, schema2expectations,
                        schema2parquet, schema2pyspark, schema2python,
                        schema2scala, schema2sql, schema2sqlalchemy,
                        schema_types)
from sqlalchemy.schema import CreateTable
from typing import Dict, List, Optional, Set


@dataclasses.dataclass
class BarNested:
    bar: str


@dataclasses.dataclass
class FooClass:
    """Small schema class for testing."""
    simple_list: List[int]
    list_opt_int: List[Optional[int]]
    opt_list_int: Optional[List[int]]
    list_opt_nested: List[Optional[BarNested]]
    opt_list_nested: Optional[List[BarNested]]
    opt_list_opt_int: Optional[List[Optional[int]]]
    list_opt_set: Set[Optional[int]]
    opt_set_int: Optional[Set[int]]
    opt_list_map: Optional[Dict[int, Optional[List[str]]]]
    list_opt_decimal: List[Optional[schema_types.Decimal(10, 4)]]
    opt_list_opt_decimal: List[Optional[schema_types.Decimal(10, 4)]]
    opt_zstd_str: Optional[schema_types.Zstd(str)]
    opt_list_zstd_str: Optional[List[Optional[schema_types.Zstd(str)]]]


EXPECTED_PARQUET = """
StructType(List(
    StructField(simple_list,
                ArrayType(LongType,false),false),
    StructField(list_opt_int,
                ArrayType(LongType,true),true),
    StructField(opt_list_int,
                ArrayType(LongType,false),true),
    StructField(list_opt_nested,
                ArrayType(StructType(List(
                    StructField(bar,StringType,false))),true),true),
    StructField(opt_list_nested,
                ArrayType(StructType(List(
                    StructField(bar,StringType,false))),false),true),
    StructField(opt_list_opt_int,
                ArrayType(LongType,true),true),
    StructField(list_opt_set,
                ArrayType(LongType,true),true),
    StructField(opt_set_int,
                ArrayType(LongType,false),true),
    StructField(opt_list_map,
                MapType(LongType,
                        ArrayType(StringType,false),true),true),
    StructField(list_opt_decimal,
                ArrayType(DecimalType(10,4),true),true),
    StructField(opt_list_opt_decimal,
                ArrayType(DecimalType(10,4),true),true),
    StructField(opt_zstd_str,StringType,true),
    StructField(opt_list_zstd_str,
                ArrayType(StringType,true),true)))
"""

EXPECTED_EXPECTATIONS = [{
    'kwargs': {
        'column': 'simple_list',
        'column_index': 0,
    },
    'expectation_type': 'expect_column_to_exist',
}, {
    'kwargs': {
        'column': 'simple_list',
    },
    'expectation_type': 'expect_column_values_to_not_be_null',
},{
    'kwargs': {
        'column': 'simple_list',
        'type_': 'ArrayType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}, {
    'kwargs': {
        'column': 'simple_list',
        'min_value': -9223372036854775808,
        'max_value': 9223372036854775807,
    },
    'expectation_type': 'expect_column_values_to_be_between',
}, {
    'kwargs': {
        'column': 'list_opt_int',
        'column_index': 1,
    },
    'expectation_type': 'expect_column_to_exist',
},{
    'kwargs': {
        'column': 'list_opt_int',
    },
    'expectation_type': 'expect_column_values_to_not_be_null',
}, {
    'kwargs': {
        'column': 'list_opt_int',
        'type_': 'ArrayType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}, {
    'kwargs': {
        'column': 'opt_list_int',
        'column_index': 2,
    },
    'expectation_type': 'expect_column_to_exist',
}, {
    'kwargs': {
        'column': 'opt_list_int',
        'type_': 'ArrayType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}, {
    'kwargs': {
        'column': 'list_opt_nested',
        'column_index': 3,
    },
    'expectation_type': 'expect_column_to_exist',
}, {
    'kwargs': {
        'column': 'list_opt_nested',
    },
    'expectation_type': 'expect_column_values_to_not_be_null',
}, {
    'kwargs': {
        'column': 'list_opt_nested',
        'type_': 'ArrayType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}, {
    'kwargs': {
        'column': 'opt_list_nested',
        'column_index': 4,
    },
    'expectation_type': 'expect_column_to_exist',
}, {
    'kwargs': {
        'column': 'opt_list_nested',
        'type_': 'ArrayType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}, {
    'kwargs': {
        'column': 'opt_list_opt_int',
        'column_index': 5,
    },
    'expectation_type': 'expect_column_to_exist',
}, {
    'kwargs': {
        'column': 'opt_list_opt_int',
        'type_': 'ArrayType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}, {
    'kwargs': {
        'column': 'list_opt_set',
        'column_index': 6,
    },
    'expectation_type': 'expect_column_to_exist',
},{
    'kwargs': {
        'column': 'list_opt_set',
    },
    'expectation_type': 'expect_column_values_to_not_be_null',
}, {
    'kwargs': {
        'column': 'list_opt_set',
        'type_': 'ArrayType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}, {
    'kwargs': {
        'column': 'opt_set_int',
        'column_index': 7,
    },
    'expectation_type': 'expect_column_to_exist',
}, {
    'kwargs': {
        'column': 'opt_set_int',
        'type_': 'ArrayType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}, {
    'kwargs': {
        'column': 'opt_list_map',
        'column_index': 8,
    },
    'expectation_type': 'expect_column_to_exist',
}, {
    'kwargs': {
        'column': 'opt_list_map',
        'type_': 'MapType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}, {
    'kwargs': {
        'column': 'list_opt_decimal',
        'column_index': 9,
    },
    'expectation_type': 'expect_column_to_exist',
}, {
    'kwargs': {
        'column': 'list_opt_decimal',
    },
    'expectation_type': 'expect_column_values_to_not_be_null',
}, {
    'kwargs': {
        'column': 'list_opt_decimal',
        'type_': 'ArrayType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}, {
    'kwargs': {
        'column': 'opt_list_opt_decimal',
        'column_index': 10,
    },
    'expectation_type': 'expect_column_to_exist',
}, {
    'kwargs': {
        'column': 'opt_list_opt_decimal',
    },
    'expectation_type': 'expect_column_values_to_not_be_null',
},{
    'kwargs': {
        'column': 'opt_list_opt_decimal',
        'type_': 'ArrayType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}, {
    'kwargs': {
        'column': 'opt_zstd_str',
        'column_index': 11,
    },
    'expectation_type': 'expect_column_to_exist',
}, {
    'kwargs': {
        'column': 'opt_zstd_str',
        'type_': 'StringType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}, {
    'kwargs': {
        'column': 'opt_list_zstd_str',
        'column_index': 12,
    },
    'expectation_type': 'expect_column_to_exist',
}, {
    'kwargs': {
        'column': 'opt_list_zstd_str',
        'type_': 'ArrayType',
    },
    'expectation_type': 'expect_column_values_to_be_of_type',
}]


class TypeCompositionTest(unittest.TestCase):

    def test_generate_scala(self):
        table = python2schema.ConvertDataclass(FooClass)
        exports = schema2scala.TypeExports()
        exports.add_export('__main__.BarNested', 'BarNested', 'com.foo.schema')
        scala_code = schema2scala.ConvertTable(table,
                                               'com.foo.schema',
                                               type_exports=exports)
        # print(f'Scala code:\n{scala_code}')
        self.assertEqual(
            scala_code, """package com.foo.schema
import org.apache.spark.sql.types.Decimal


case class FooClass(
  simple_list: Seq[Long],
  list_opt_int: Array[Option[Long]],
  opt_list_int: Option[Array[Long]],
  list_opt_nested: Array[Option[BarNested]],
  opt_list_nested: Option[Array[BarNested]],
  opt_list_opt_int: Option[Array[Option[Long]]],
  list_opt_set: Set[Option[Long]],
  opt_set_int: Option[Set[Long]],
  opt_list_map: Option[Map[Long, Option[Array[String]]]],
  list_opt_decimal: Array[Option[Decimal]],
  opt_list_opt_decimal: Array[Option[Decimal]],
  opt_zstd_str: Option[String],
  opt_list_zstd_str: Option[Array[Option[String]]]
)""")

    def test_generate_sql(self):
        table = python2schema.ConvertDataclass(FooClass)
        sql_code = schema2sql.ConvertTable(table)
        # print(f'Sql code:\n{sql_code}')
        # Note: Cannot have Nullable(<nested type>(..)) in Clickhouse,
        # where <nested type> in Array, Map, Nested:
        self.assertEqual(
            sql_code, """CREATE TABLE ${database}.${table} (
  simple_list Array(Int64),
  list_opt_int Array(Nullable(Int64)),
  opt_list_int Array(Int64),
  list_opt_nested Array(Nested(
    bar String
  )),
  opt_list_nested Array(Nested(
    bar String
  )),
  opt_list_opt_int Array(Nullable(Int64)),
  list_opt_set Array(Nullable(Int64)),
  opt_set_int Array(Int64),
  opt_list_map Map(Int64, Array(String)),
  list_opt_decimal Array(Nullable(Decimal64(4))),
  opt_list_opt_decimal Array(Nullable(Decimal64(4))),
  opt_zstd_str Nullable(String) CODEC(ZSTD),
  opt_list_zstd_str Array(Nullable(String))
)
""")

    def test_generate_parquet(self):
        table = python2schema.ConvertDataclass(FooClass)
        pq_schema = schema2parquet.ConvertTable(table)
        # print(f'Parquet code:\n{pq_schema}')
        self.assertEqual(
            f'{pq_schema}', """simple_list: list<element: int64 not null>
  child 0, element: int64 not null
list_opt_int: list<element: int64>
  child 0, element: int64
opt_list_int: list<element: int64 not null>
  child 0, element: int64 not null
list_opt_nested: list<element: struct<bar: string not null>>
  child 0, element: struct<bar: string not null>
      child 0, bar: string not null
opt_list_nested: list<element: struct<bar: string not null> not null>
  child 0, element: struct<bar: string not null> not null
      child 0, bar: string not null
opt_list_opt_int: list<element: int64>
  child 0, element: int64
list_opt_set: list<element: int64>
  child 0, element: int64
opt_set_int: list<element: int64 not null>
  child 0, element: int64 not null
opt_list_map: map<int64, list<element: string not null>>
  child 0, entries: struct<key: int64 not null, value: list<element: string not null>> not null
      child 0, key: int64 not null
      child 1, value: list<element: string not null>
          child 0, element: string not null
list_opt_decimal: list<element: decimal128(10, 4)>
  child 0, element: decimal128(10, 4)
opt_list_opt_decimal: list<element: decimal128(10, 4)>
  child 0, element: decimal128(10, 4)
opt_zstd_str: string
opt_list_zstd_str: list<element: string>
  child 0, element: string""")

    def test_generate_python(self):
        table = python2schema.ConvertDataclass(FooClass)
        py_schema = schema2python.ConvertTable(table)
        # print(f'Python code:\n{py_schema}')
        self.assertEqual(
            py_schema, """\"\"\"Generated by schema2python utility.\"\"\"
import dataclasses
import decimal
import typing
from dataschema import annotations
from dataschema.entity import Annotate

JAVA_PACKAGE = "FooClass"

@dataclasses.dataclass
class FooClass:
    simple_list: typing.List[int]
    list_opt_int: typing.List[typing.Optional[int]]
    opt_list_int: typing.Optional[typing.List[int]]
    list_opt_nested: typing.List[typing.Optional[__main__.BarNested]]
    opt_list_nested: typing.Optional[typing.List[__main__.BarNested]]
    opt_list_opt_int: typing.Optional[typing.List[typing.Optional[int]]]
    list_opt_set: typing.Set[typing.Optional[int]]
    opt_set_int: typing.Optional[typing.Set[int]]
    opt_list_map: typing.Optional[typing.Dict[int, typing.Optional[typing.List[str]]]]
    list_opt_decimal: typing.List[Annotate(typing.Optional[decimal.Decimal], [annotations.Decimal(10, 4)])]
    opt_list_opt_decimal: typing.List[Annotate(typing.Optional[decimal.Decimal], [annotations.Decimal(10, 4)])]
    opt_zstd_str: Annotate(typing.Optional[str], [annotations.Compression("ZSTD", None)])
    opt_list_zstd_str: typing.Optional[typing.List["""
            """Annotate(typing.Optional[str], """
            """[annotations.Compression("ZSTD", None)])]]""")

    def test_generate_pyspark(self):
        table = python2schema.ConvertDataclass(FooClass)
        spark_schema = schema2pyspark.ConvertTable(table)
        # print(f'Spark code:\n{spark_schema}')
        expected = EXPECTED_PARQUET.strip().replace(' ', '').replace('\n', '')
        self.assertEqual(f'{spark_schema}', expected)

    def test_generate_dbml(self):
        table = python2schema.ConvertDataclass(FooClass)
        dbml_schema = schema2dbml.ConvertTable(table)
        # print(f'DBML:\n{dbml_schema}')
        self.assertEqual(
            dbml_schema, """Table FooClass {
    simple_list Array(Int64)
    list_opt_int Array(Int64)
    opt_list_int Array(Int64 [not null])
    list_opt_nested Array(Nested(
  bar String [not null]
))
    opt_list_nested Array(Nested(
  bar String [not null]
) [not null])
    opt_list_opt_int Array(Int64)
    list_opt_set Array(Int64)
    opt_set_int Array(Int64 [not null])
    opt_list_map Map(Int64 [not null], Array(String [not null]))
    list_opt_decimal Array(Decimal(10, 4))
    opt_list_opt_decimal Array(Decimal(10, 4))
    opt_zstd_str String
    opt_list_zstd_str Array(String)
}
""")

    def test_generate_alchemy(self):
        table = python2schema.ConvertDataclass(FooClass)
        alchemy_schema = schema2sqlalchemy.ConvertTable(table)
        alchemy_str = str(CreateTable(alchemy_schema)).replace(', \n', ',\n')
        # print(f'Alchemy:\n{alchemy_str}')
        self.assertEqual(
            alchemy_str.strip(), """CREATE TABLE "FooClass" (
	simple_list ARRAY,
	list_opt_int ARRAY,
	opt_list_int ARRAY,
	list_opt_nested ARRAY,
	opt_list_nested ARRAY,
	opt_list_opt_int ARRAY,
	list_opt_set ARRAY,
	opt_set_int ARRAY,
	opt_list_map JSON,
	list_opt_decimal ARRAY,
	opt_list_opt_decimal ARRAY,
	opt_zstd_str VARCHAR,
	opt_list_zstd_str ARRAY,
	PRIMARY KEY (simple_list, list_opt_int, opt_list_int, list_opt_nested, opt_list_nested, opt_list_opt_int, list_opt_set, opt_set_int, opt_list_map, list_opt_decimal, opt_list_opt_decimal, opt_zstd_str, opt_list_zstd_str)
)""")

    def test_generate_expectations(self):
        table = python2schema.ConvertDataclass(FooClass)

        def strip_expectation(ex):
            del ex['meta']
            del ex['kwargs']['result_format']
            return ex

        expectations = [
            strip_expectation(ex.to_json_dict())
            for ex in schema2expectations.ConvertTable(
                table, schema2expectations.EngineType.SPARK)
        ]
        # print(f'Expectations:\n{expectations}')
        self.assertEqual(expectations, EXPECTED_EXPECTATIONS)


if __name__ == '__main__':
    unittest.main()
