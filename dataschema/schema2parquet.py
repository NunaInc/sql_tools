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
"""Converts python data schema to Arrow Schema."""
import pyarrow
from dataschema import Schema_pb2, Schema


def _GetStringType(column: Schema.Column):
    if column.data_annotation.is_lob:
        return pyarrow.large_string()
    return pyarrow.string()


def _GetBytesType(column: Schema.Column):
    if column.data_annotation.is_lob:
        return pyarrow.large_binary()
    return pyarrow.binary()


def _GetDateTimeType(column: Schema.Column):
    unit = 's'
    if not column.info.timestamp_info.precision:
        unit = 's'
    elif column.info.timestamp_info.precision == 3:
        unit = 'ms'
    elif column.info.timestamp_info.precision == 6:
        unit = 'us'
    elif column.info.timestamp_info.precision == 9:
        unit = 'ns'
    else:
        raise ValueError(
            f'Invalid time precision {column.info.timestamp_info.precision}'
            f'for column `{column.info.name}`')
    tz = None
    if column.info.timestamp_info.timezone:
        tz = column.info.timestamp_info.timezone
    return pyarrow.timestamp(unit, tz)


def _GetDecimalType(column: Schema.Column):
    if not column.info.decimal_info.scale:
        return pyarrow.decimal128(column.info.decimal_info.precision)
    return pyarrow.decimal128(column.info.decimal_info.precision,
                              column.info.decimal_info.scale)


def _GetArrayType(column: Schema.Column):
    if len(column.fields) != 1:
        raise ValueError(
            f'Array column expected to have one element column: `{column}`')
    nullable_elem = (
        column.fields[0].info.label == Schema_pb2.ColumnInfo.LABEL_OPTIONAL)
    return pyarrow.list_(
        pyarrow.field('element',
                      _GetColumnType(column.fields[0]),
                      nullable=nullable_elem))


def _GetSetType(column: Schema.Column):
    if len(column.fields) != 1:
        raise ValueError(
            f'Set column expected to have one element column: `{column}`')
    nullable_elem = (
        column.fields[0].info.label == Schema_pb2.ColumnInfo.LABEL_OPTIONAL)
    return pyarrow.list_(
        pyarrow.field('element',
                      _GetColumnType(column.fields[0]),
                      nullable=nullable_elem))


def _GetMapType(column: Schema.Column):
    if len(column.fields) != 2:
        raise ValueError(
            f'Map column expected to have two elements column: `{column}`')
    return pyarrow.map_(_GetColumnType(column.fields[0]),
                        _GetColumnType(column.fields[1]))


def _GetNestedType(column: Schema.Column):
    fields = [ConvertColumn(sub_column) for sub_column in column.fields]
    return pyarrow.struct(fields)


_TYPE_MAPPING = {
    Schema_pb2.ColumnInfo.TYPE_BOOLEAN: lambda _: pyarrow.bool_(),
    Schema_pb2.ColumnInfo.TYPE_INT_8: lambda _: pyarrow.int8(),
    Schema_pb2.ColumnInfo.TYPE_INT_16: lambda _: pyarrow.int16(),
    Schema_pb2.ColumnInfo.TYPE_INT_32: lambda _: pyarrow.int32(),
    Schema_pb2.ColumnInfo.TYPE_INT_64: lambda _: pyarrow.int64(),
    Schema_pb2.ColumnInfo.TYPE_UINT_8: lambda _: pyarrow.uint8(),
    Schema_pb2.ColumnInfo.TYPE_UINT_16: lambda _: pyarrow.uint16(),
    Schema_pb2.ColumnInfo.TYPE_UINT_32: lambda _: pyarrow.uint32(),
    Schema_pb2.ColumnInfo.TYPE_UINT_64: lambda _: pyarrow.uint64(),
    Schema_pb2.ColumnInfo.TYPE_FLOAT_32: lambda _: pyarrow.float32(),
    Schema_pb2.ColumnInfo.TYPE_FLOAT_64: lambda _: pyarrow.float64(),
    Schema_pb2.ColumnInfo.TYPE_DATE: lambda _: pyarrow.date32(),
    Schema_pb2.ColumnInfo.TYPE_DATETIME_64: _GetDateTimeType,
    Schema_pb2.ColumnInfo.TYPE_STRING: _GetStringType,
    Schema_pb2.ColumnInfo.TYPE_BYTES: _GetBytesType,
    Schema_pb2.ColumnInfo.TYPE_DECIMAL: _GetDecimalType,
    Schema_pb2.ColumnInfo.TYPE_NESTED: _GetNestedType,
    Schema_pb2.ColumnInfo.TYPE_ARRAY: _GetArrayType,
    Schema_pb2.ColumnInfo.TYPE_SET: _GetSetType,
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


def ConvertColumn(column: Schema.Column):
    """Converts a python data Schema column to an arrow field."""
    non_nullable = column.info.label == Schema_pb2.ColumnInfo.LABEL_REQUIRED
    if (column.info.label == Schema_pb2.ColumnInfo.LABEL_REPEATED and
            column.info.column_type not in _STRUCT_TYPES):
        return pyarrow.field(column.info.name,
                             pyarrow.list_(
                                 pyarrow.field('element',
                                               _GetColumnType(column),
                                               nullable=False)),
                             nullable=not non_nullable)
    return pyarrow.field(column.info.name,
                         _GetColumnType(column),
                         nullable=not non_nullable)


def ConvertTable(table: Schema.Table):
    """Converts a python data Schema table to an arrow schema."""
    return pyarrow.schema([ConvertColumn(column) for column in table.columns])


def ConvertSchema(table: Schema.Table):
    """Backward compatible function - deprecated - do not use."""
    return ConvertTable(table)
