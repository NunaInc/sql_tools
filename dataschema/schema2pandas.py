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
"""Converts python data schema to Pandas Schema.

This works differently only for basic data types. Arrays, repeated, etc default to the
normal 'object' data type from Pandas.
"""
import pandas
from dataschema import Schema_pb2, Schema
from typing import Any, Dict, List


def _GetDateTimeType(column: Schema.Column):
    tz = 'Etc/UTC'
    if column.info.timestamp_info.timezone:
        tz = column.info.timestamp_info.timezone
    return pandas.DatetimeTZDtype(tz=tz)


_TYPE_MAPPING = {
    Schema_pb2.ColumnInfo.TYPE_BOOLEAN: lambda _: pandas.BooleanDtype(),
    Schema_pb2.ColumnInfo.TYPE_INT_8: lambda _: pandas.Int8Dtype(),
    Schema_pb2.ColumnInfo.TYPE_INT_16: lambda _: pandas.Int16Dtype(),
    Schema_pb2.ColumnInfo.TYPE_INT_32: lambda _: pandas.Int32Dtype(),
    Schema_pb2.ColumnInfo.TYPE_INT_64: lambda _: pandas.Int64Dtype(),
    Schema_pb2.ColumnInfo.TYPE_UINT_8: lambda _: pandas.UInt8Dtype(),
    Schema_pb2.ColumnInfo.TYPE_UINT_16: lambda _: pandas.UInt16Dtype(),
    Schema_pb2.ColumnInfo.TYPE_UINT_32: lambda _: pandas.UInt32Dtype(),
    Schema_pb2.ColumnInfo.TYPE_UINT_64: lambda _: pandas.UInt64Dtype(),
    Schema_pb2.ColumnInfo.TYPE_FLOAT_32: lambda _: pandas.Float32Dtype(),
    Schema_pb2.ColumnInfo.TYPE_FLOAT_64: lambda _: pandas.Float64Dtype(),
    Schema_pb2.ColumnInfo.TYPE_DATETIME_64: _GetDateTimeType,
    Schema_pb2.ColumnInfo.TYPE_STRING: lambda _: pandas.StringDtype(),
    Schema_pb2.ColumnInfo.TYPE_BYTES: lambda _: 'V',
}


def ConvertColumn(column: Schema.Column):
    """Returns the pandas datatype specification for a column."""
    if column.info.label == Schema_pb2.ColumnInfo.LABEL_REPEATED:
        return 'O'
    if column.info.column_type not in _TYPE_MAPPING:
        # Anything but basic types is an 'object':
        return 'O'
    return _TYPE_MAPPING[column.info.column_type](column)


def ConvertColumnUnderlyingType(column: Schema.Column):
    if column.info.column_type in _TYPE_MAPPING:
        return _TYPE_MAPPING[column.info.column_type](column)
    if column.info.column_type in (Schema_pb2.ColumnInfo.TYPE_ARRAY,
                                   Schema_pb2.ColumnInfo.TYPE_SET):
        return ConvertColumnUnderlyingType(column.fields[0])
    return 'O'


def ConvertTable(table: Schema.Table):
    """Converts a python data Schema table to an arrow schema."""
    return {column.name(): ConvertColumn(column) for column in table.columns}


def ToDataFrame(data: Dict[str, List[Any]], table: Schema.Table):
    """For a dataframe-like map, this converts data to a pandas DataFrame
    keeping the original column types specified in table, as much as possible."""
    pandas_schema = ConvertTable(table)
    return pandas.DataFrame(
        {k: pandas.array(v, dtype=pandas_schema[k]) for k, v in data.items()})
