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
"""Converts Schema to PySpark types.

Note: unsigned not supported by Spark, so we use signed w/ superior width
to hold the unsigned range.
ie: uint8 => int16, uint16 -> int32, uint32 -> int64, uint64 -> decimal(20)"""

from pyspark.sql import types
from dataschema import Schema_pb2, Schema


def _GetDecimalType(column: Schema.Column):
    if not column.info.decimal_info.scale:
        return types.DecimalType(column.info.decimal_info.precision)
    return types.DecimalType(column.info.decimal_info.precision,
                             column.info.decimal_info.scale)


def _GetArrayType(column: Schema.Column):
    if len(column.fields) != 1:
        raise ValueError(
            f'Array column expected to have one element column: `{column}`')
    nullable_elem = (
        column.fields[0].info.label == Schema_pb2.ColumnInfo.LABEL_OPTIONAL)
    return types.ArrayType(_GetColumnType(column.fields[0]), nullable_elem)


def _GetMapType(column: Schema.Column):
    if len(column.fields) != 2:
        raise ValueError(
            f'Map column expected to have two elements column: `{column}`')
    return types.MapType(
        _GetColumnType(column.fields[0]), _GetColumnType(column.fields[1]),
        column.fields[1] != Schema_pb2.ColumnInfo.LABEL_REQUIRED)


def _GetNestedType(column: Schema.Column):
    return types.StructType(
        [ConvertColumn(sub_column) for sub_column in column.fields])


_TYPE_MAPPING = {
    Schema_pb2.ColumnInfo.TYPE_BOOLEAN: lambda _: types.BooleanType(),
    Schema_pb2.ColumnInfo.TYPE_INT_8: lambda _: types.ByteType(),
    Schema_pb2.ColumnInfo.TYPE_INT_16: lambda _: types.ShortType(),
    Schema_pb2.ColumnInfo.TYPE_INT_32: lambda _: types.IntegerType(),
    Schema_pb2.ColumnInfo.TYPE_INT_64: lambda _: types.LongType(),
    Schema_pb2.ColumnInfo.TYPE_UINT_8: lambda _: types.ShortType(),
    Schema_pb2.ColumnInfo.TYPE_UINT_16: lambda _: types.IntegerType(),
    Schema_pb2.ColumnInfo.TYPE_UINT_32: lambda _: types.LongType(),
    Schema_pb2.ColumnInfo.TYPE_UINT_64: lambda _: types.DecimalType(20),
    Schema_pb2.ColumnInfo.TYPE_FLOAT_32: lambda _: types.FloatType(),
    Schema_pb2.ColumnInfo.TYPE_FLOAT_64: lambda _: types.DoubleType(),
    Schema_pb2.ColumnInfo.TYPE_DATE: lambda _: types.DateType(),
    Schema_pb2.ColumnInfo.TYPE_DATETIME_64: lambda _: types.TimestampType(),
    Schema_pb2.ColumnInfo.TYPE_STRING: lambda _: types.StringType(),
    Schema_pb2.ColumnInfo.TYPE_BYTES: lambda _: types.BinaryType(),
    Schema_pb2.ColumnInfo.TYPE_DECIMAL: _GetDecimalType,
    Schema_pb2.ColumnInfo.TYPE_NESTED: _GetNestedType,
    Schema_pb2.ColumnInfo.TYPE_ARRAY: _GetArrayType,
    Schema_pb2.ColumnInfo.TYPE_SET: _GetArrayType,
    Schema_pb2.ColumnInfo.TYPE_MAP: _GetMapType,
}

_STRUCT_TYPES = {
    Schema_pb2.ColumnInfo.TYPE_ARRAY,
    Schema_pb2.ColumnInfo.TYPE_SET,
    Schema_pb2.ColumnInfo.TYPE_MAP,
}


def _GetColumnType(column: Schema.Column):
    if column.info.column_type not in _TYPE_MAPPING:
        return ValueError(f'Cannot convert type for column: {column}')
    return _TYPE_MAPPING[column.info.column_type](column)


def ConvertColumn(column: Schema.Column) -> types.StructField:
    """Converts a Schema column to a PySpark StructField of corresponding type."""
    non_nullable = column.info.label == Schema_pb2.ColumnInfo.LABEL_REQUIRED
    if (column.info.label == Schema_pb2.ColumnInfo.LABEL_REPEATED and
            column.info.column_type not in _STRUCT_TYPES):
        return types.StructField(column.info.name,
                                 types.ArrayType(_GetColumnType(column), False),
                                 False)
    return types.StructField(column.info.name, _GetColumnType(column),
                             not non_nullable)


def ConvertTable(table: Schema.Table):
    """Converts a python data Schema table to a PySpark StructType."""
    return types.StructType([ConvertColumn(column) for column in table.columns])
