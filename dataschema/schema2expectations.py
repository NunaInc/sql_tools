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
"""From a Schema creates a set of expectation configurations."""

from enum import Enum
from typing import Any, Dict, List, Optional, Union
from great_expectations.core import ExpectationConfiguration
from dataschema import (
    Schema,
    Schema_pb2,
    schema2pandas,
    schema2pyspark,
    # schema2sqlalchemy,
)

ResultFormat = Union[str, Dict[str, Any]]


class EngineType(Enum):
    PANDAS = 1
    SQL_ALCHEMY = 2
    SPARK = 3


def GetResultFormat(result_format: Optional[ResultFormat]):
    if result_format:
        return result_format
    return 'BASIC'


# Issues with Great Expectations:
#   - peculiar way of specifying arguments (come on - dicts ??)
#   - engine dependent type specification
#   - no support for nested
#       (may add that though:
#        https://github.com/great-expectations/great_expectations/issues/3975)
#   - nothing covering arrays except that type is "list"

_PANDAS_PY_TYPE_NAMES = {
    Schema_pb2.ColumnInfo.TYPE_ARRAY: 'list',
    Schema_pb2.ColumnInfo.TYPE_SET: 'list',
    Schema_pb2.ColumnInfo.TYPE_MAP: 'dict',
    Schema_pb2.ColumnInfo.TYPE_STRING: 'str',
    Schema_pb2.ColumnInfo.TYPE_BYTES: 'bytes',
    Schema_pb2.ColumnInfo.TYPE_DATE: 'datetime64[d]',
    Schema_pb2.ColumnInfo.TYPE_DATETIME_64: 'datetime64',
}
_INT_TYPES = {
    Schema_pb2.ColumnInfo.TYPE_INT_8,
    Schema_pb2.ColumnInfo.TYPE_INT_16,
    Schema_pb2.ColumnInfo.TYPE_INT_32,
    Schema_pb2.ColumnInfo.TYPE_INT_64,
    Schema_pb2.ColumnInfo.TYPE_UINT_8,
    Schema_pb2.ColumnInfo.TYPE_UINT_16,
    Schema_pb2.ColumnInfo.TYPE_UINT_32,
    Schema_pb2.ColumnInfo.TYPE_UINT_64,
}


def _GetPandasTypeExpectation(
        column: Schema.Column,
        result_format: Optional[ResultFormat] = None
) -> ExpectationConfiguration:
    # There is a discussion here: the type of a nullable column can
    # be either the normal pandas or an object..
    # In general the Pandas type system is quite restricted and targeted
    # for columnar, basic types things.
    # Oh, and is totally confused by bytes and a bunch of other types.
    if column.info.column_type in _PANDAS_PY_TYPE_NAMES:
        type_name = _PANDAS_PY_TYPE_NAMES[column.info.column_type]
    else:
        pd_type = schema2pandas.ConvertColumnUnderlyingType(column)
        if isinstance(pd_type, str):
            type_name = pd_type
        else:
            type_name = pd_type.numpy_dtype.name
    alternate_type = None
    if (column.info.label == Schema_pb2.ColumnInfo.LABEL_OPTIONAL and
            type_name != 'O'):
        alternate_type = 'O'
    elif column.info.column_type in _INT_TYPES:
        alternate_type = 'int'
    elif column.info.column_type in (Schema_pb2.ColumnInfo.TYPE_FLOAT_32,
                                     Schema_pb2.ColumnInfo.TYPE_DECIMAL):
        alternate_type = 'float64'
    elif column.info.column_type == Schema_pb2.ColumnInfo.TYPE_BYTES:
        alternate_type = 'str'
    elif column.info.column_type in (Schema_pb2.ColumnInfo.TYPE_DATE,
                                     Schema_pb2.ColumnInfo.TYPE_DATETIME_64):
        alternate_type = 'str'
    if alternate_type:
        return ExpectationConfiguration(
            expectation_type='expect_column_values_to_be_in_type_list',
            kwargs={
                'column': column.name(),
                'type_list': [type_name, alternate_type],
                'result_format': GetResultFormat(result_format)
            })
    else:
        return ExpectationConfiguration(
            expectation_type='expect_column_values_to_be_of_type',
            kwargs={
                'column': column.name(),
                'type_': type_name,
                'result_format': GetResultFormat(result_format),
            })


def _GetPySparkTypeExpectation(
        column: Schema.Column,
        result_format: Optional[ResultFormat] = None
) -> ExpectationConfiguration:
    spark_field = schema2pyspark.ConvertColumn(column)
    # Another limitation of GreatExpectations: should be able
    #  to specify the type as spark_field.dataType.jsonValue()
    #  and GE should be able to easily reconstruct the type with
    #  nullable condition, subtypes and such, but no..
    # E.g. for an Array(Integer) it would only verify it is an array
    return ExpectationConfiguration(
        expectation_type='expect_column_values_to_be_of_type',
        kwargs={
            'column': column.name(),
            'type_': type(spark_field.dataType).__name__,
            'result_format': GetResultFormat(result_format),
        })


def _GetSqlAlchemyTypeExpectation(
        column: Schema.Column,
        result_format: Optional[ResultFormat] = None
) -> ExpectationConfiguration:
    # This basically is unreliable, because the great expectations
    # checks on the exact type specification, instead of using a
    # compatibility map or such.
    # In particular a SMALLINT can be created as NUMERIC in the actual
    # database, and a DATE can be an integer or such.
    # One would think that
    #     sql_alchemy_engine.dialect.type_compiler.process(
    #                column.type, type_expression=column)
    # can help us get the underlying type of a column, but that is not
    # the case. As such we have no particular expectation on the
    # actual type for sql.
    #
    # alchemy_column = schema2sqlalchemy.ConvertColumn(column)
    # Again, a lot of things are lost in here: arrays, nullable, structs.
    # return ExpectationConfiguration(
    #     expectation_type='expect_column_values_to_be_of_type',
    #     kwargs={
    #         'column': column.name(),
    #         'type_': type(alchemy_column.type).__name__,
    #         'result_format': GetResultFormat(result_format),
    #    })
    del column
    del result_format


def GetTypeExpectation(
        column: Schema.Column,
        engine: EngineType,
        result_format: Optional[ResultFormat] = None
) -> ExpectationConfiguration:
    if engine == EngineType.PANDAS:
        return _GetPandasTypeExpectation(column, result_format)
    if engine == EngineType.SQL_ALCHEMY:
        return _GetSqlAlchemyTypeExpectation(column, result_format)
    if engine == EngineType.SPARK:
        return _GetPySparkTypeExpectation(column, result_format)
    return None


_RANGE_VALUES = {
    Schema_pb2.ColumnInfo.TYPE_INT_8: (-0x80, 0x7f),
    Schema_pb2.ColumnInfo.TYPE_INT_16: (-0x8000, 0x7fff),
    Schema_pb2.ColumnInfo.TYPE_INT_32: (-0x80000000, 0x7fffffff),
    Schema_pb2.ColumnInfo.TYPE_INT_64:
        (-0x8000000000000000, 0x7fffffffffffffff),
    Schema_pb2.ColumnInfo.TYPE_UINT_8: (0, 0xff),
    Schema_pb2.ColumnInfo.TYPE_UINT_16: (0, 0xffff),
    Schema_pb2.ColumnInfo.TYPE_UINT_32: (0, 0xffffffff),
    Schema_pb2.ColumnInfo.TYPE_UINT_64: (0, 0xffffffffffffffff)
}


def _GetColumnName(column: Schema.Column, engine: EngineType):
    if engine == EngineType.SQL_ALCHEMY:
        return column.sql_name()
    return column.name()


def GetRangeExpectation(
        column: Schema.Column,
        engine: EngineType,
        result_format: Optional[ResultFormat] = None
) -> ExpectationConfiguration:
    # Again: arrays - no way to check them.
    if column.info.column_type not in _RANGE_VALUES:
        return None
    column_range = _RANGE_VALUES[column.info.column_type]
    return ExpectationConfiguration(
        expectation_type='expect_column_values_to_be_between',
        kwargs={
            'column': _GetColumnName(column, engine),
            'min_value': column_range[0],
            'max_value': column_range[1],
            'result_format': GetResultFormat(result_format),
        })


def GetDataExpectations(
    column: Schema.Column,
    engine: EngineType,
    result_format: Optional[ResultFormat] = None
) -> List[ExpectationConfiguration]:
    expectations = []
    column_name = _GetColumnName(column, engine)
    if column.data_annotation.width:
        expectations.append(
            ExpectationConfiguration(
                expectation_type='expect_column_value_lengths_to_be_between',
                kwargs={
                    'column': column_name,
                    'min_value': 0,
                    'max_value': column.data_annotation.width,
                    'result_format': GetResultFormat(result_format),
                }))
    if column.data_annotation.dq_field.enum_value:
        expectations.append(
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column':
                        column_name,
                    'value_set':
                        list(column.data_annotation.dq_field.enum_value),
                    'result_format':
                        GetResultFormat(result_format),
                }))
    if column.data_annotation.dq_field.format:
        # Unfortunately no way to specify the datetime format per-se
        expectations.append(
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_dateutil_parseable',
                kwargs={
                    'column': column_name,
                    'result_format': GetResultFormat(result_format),
                }))
    if column.data_annotation.dq_field.regexp:
        expectations.append(
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_match_regex',
                kwargs={
                    'column': column_name,
                    'regex': column.data_annotation.dq_field.regexp(),
                    'result_format': GetResultFormat(result_format),
                }))
    return expectations


def ConvertColumn(
    column: Schema.Column,
    engine: EngineType,
    result_format: Optional[ResultFormat] = None
) -> List[ExpectationConfiguration]:
    if column.data_annotation.dq_field.is_ignored:
        return []
    expectations = []
    texpectation = GetTypeExpectation(column, engine, result_format)
    if texpectation is not None:
        expectations.append(texpectation)
    expectations.extend(GetDataExpectations(column, engine, result_format))
    range_exp = GetRangeExpectation(column, engine, result_format)
    if range_exp:
        expectations.append(range_exp)
    return expectations


def ConvertTable(
    table: Schema.Table,
    engine: EngineType,
    result_format: Optional[ResultFormat] = None
) -> List[ExpectationConfiguration]:
    expectations = []
    index = 0
    for column in table.columns:
        expectations.append(
            ExpectationConfiguration(
                expectation_type='expect_column_to_exist',
                kwargs={
                    'column': _GetColumnName(column, engine),
                    'column_index': index,
                    'result_format': GetResultFormat(result_format)
                }))
        index += 1
        expectations.extend(ConvertColumn(column, engine, result_format))
    return expectations
