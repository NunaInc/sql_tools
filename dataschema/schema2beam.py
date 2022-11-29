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
"""Utility to generate Apache Beam schema proto from Schema tables.


The Beam schema proto is here:
model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/schema.proto

https://github.com/apache/beam/blob/master/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/schema.proto
"""

import hashlib
import uuid

from apache_beam.portability.api import schema_pb2
from apache_beam.portability import common_urns
from dataschema import Schema_pb2, Schema

_ATOMIC_TYPES = {
    Schema_pb2.ColumnInfo.TYPE_BOOLEAN: schema_pb2.AtomicType.BOOLEAN,
    Schema_pb2.ColumnInfo.TYPE_INT_8: schema_pb2.AtomicType.BYTE,
    Schema_pb2.ColumnInfo.TYPE_INT_16: schema_pb2.AtomicType.INT16,
    Schema_pb2.ColumnInfo.TYPE_INT_32: schema_pb2.AtomicType.INT32,
    Schema_pb2.ColumnInfo.TYPE_INT_64: schema_pb2.AtomicType.INT64,
    Schema_pb2.ColumnInfo.TYPE_UINT_8: schema_pb2.AtomicType.BYTE,
    Schema_pb2.ColumnInfo.TYPE_UINT_16: schema_pb2.AtomicType.INT16,
    Schema_pb2.ColumnInfo.TYPE_UINT_32: schema_pb2.AtomicType.INT32,
    Schema_pb2.ColumnInfo.TYPE_UINT_64: schema_pb2.AtomicType.INT64,
    Schema_pb2.ColumnInfo.TYPE_FLOAT_32: schema_pb2.AtomicType.FLOAT,
    Schema_pb2.ColumnInfo.TYPE_FLOAT_64: schema_pb2.AtomicType.DOUBLE,
    Schema_pb2.ColumnInfo.TYPE_STRING: schema_pb2.AtomicType.STRING,
    Schema_pb2.ColumnInfo.TYPE_BYTES: schema_pb2.AtomicType.BYTES,
}
_STRUCT_TYPES = {
    Schema_pb2.ColumnInfo.TYPE_ARRAY,
    Schema_pb2.ColumnInfo.TYPE_SET,
    Schema_pb2.ColumnInfo.TYPE_MAP,
}


def _full_name_uuid(s: str):
    h = hashlib.new("sha256")
    h.update(s.encode("utf-8"))
    return str(uuid.UUID(bytes=h.digest()[:16]))


def _GetDecimalType(column: Schema.Column):
    field_type = schema_pb2.FieldType()
    field_type.logical_type.urn = common_urns.decimal.urn
    field_type.logical_type.representation.atomic_type = schema_pb2.AtomicType.BYTES
    # TODO(catalin): there is probably a way to encode precision and scale
    # here, like FixedPrecisionDecimalArgumentRepresentation
    # but we skip for now.
    return field_type


def _GetArrayType(column: Schema.Column):
    if len(column.fields) != 1:
        raise ValueError(
            f'Array column expected to have one element column: `{column}`')
    field_type = schema_pb2.FieldType()
    field_type.array_type.element_type.CopyFrom(_GetColumnType(
        column.fields[0]))
    return field_type


def _GetMapType(column: Schema.Column):
    if len(column.fields) != 2:
        raise ValueError(
            f'Map column expected to have two elements column: `{column}`')
    field_type = schema_pb2.FieldType()
    field_type.map_type.key_type.CopyFrom(_GetColumnType(column.fields[0]))
    field_type.map_type.value_type.CopyFrom(_GetColumnType(column.fields[1]))
    return field_type


def _GetNestedType(column: Schema.Column):
    field_type = schema_pb2.FieldType()
    field_type.row_type.schema.id = _full_name_uuid(
        f'{column.table_name}.{column.name()}')
    field_type.row_type.schema.fields.extend(
        [ConvertColumn(sub_column) for sub_column in column.fields])
    return field_type


BEAN_DATE_URN = "dataschema:logical_type:date:v1"


def _GetDateType(column: Schema.Column):
    # This is not quite there - there is no proper 'Date' type in beam
    # Using a string - not perfect, and generally the date type is pretty
    # screwed up and generally should not be used.
    # You would have to deal with it in the bean part, using the urn above.
    field_type = schema_pb2.FieldType()
    field_type.logical_type.urn = BEAN_DATE_URN
    field_type.logical_type.representation.atomic_type = schema_pb2.AtomicType.STRING
    return field_type


BEAN_DATETYPE_URN = "dataschema:logical_type:datetime:v1"


def _GetDatetime64Type(column: Schema.Column):
    # Same thing here: we leave it at the latitude of the consumer to
    # figure out what to do with the logical type.
    field_type = schema_pb2.FieldType()
    field_type.logical_type.urn = BEAN_DATETYPE_URN
    field_type.logical_type.representation.atomic_type = schema_pb2.AtomicType.STRING
    return field_type


def _GetColumnType(column: Schema.Column) -> schema_pb2.FieldType:
    if column.info.column_type in _ATOMIC_TYPES:
        field_type = schema_pb2.FieldType()
        field_type.atomic_type = _ATOMIC_TYPES[column.info.column_type]
    elif (column.info.column_type == Schema_pb2.ColumnInfo.TYPE_ARRAY or
          column.info.column_type == Schema_pb2.ColumnInfo.TYPE_SET):
        field_type = _GetArrayType(column)
    elif column.info.column_type == Schema_pb2.ColumnInfo.TYPE_MAP:
        field_type = _GetMapType(column)
    elif column.info.column_type == Schema_pb2.ColumnInfo.TYPE_DECIMAL:
        field_type = _GetDecimalType(column)
    elif column.info.column_type == Schema_pb2.ColumnInfo.TYPE_DATE:
        field_type = _GetDateType(column)
    elif column.info.column_type == Schema_pb2.ColumnInfo.TYPE_DATETIME_64:
        field_type = _GetDatetime64Type(column)
    elif column.info.column_type == Schema_pb2.ColumnInfo.TYPE_NESTED:
        field_type = _GetNestedType(column)
    else:
        raise ValueError(f"Cannot convert column: {column}")
    field_type.nullable = (column.info.label !=
                           Schema_pb2.ColumnInfo.LABEL_REQUIRED)
    return field_type


def ConvertColumn(column: Schema.Column) -> schema_pb2.Field:
    field = schema_pb2.Field(name=column.info.name)
    if column.data_annotation.comment:
        field.description = column.data_annotation.comment
    if (column.info.label == Schema_pb2.ColumnInfo.LABEL_REPEATED and
            column.info.column_type not in _STRUCT_TYPES):
        field.type.iterable_type.element_type.CopyFrom(_GetColumnType(column))
    else:
        field.type.CopyFrom(_GetColumnType(column))
    return field


def ConvertTable(table: Schema.Table) -> schema_pb2.Schema:
    """Converts a python data Schema table to a Beam - Schema"""
    schema = schema_pb2.Schema(id=_full_name_uuid(table.full_name()))
    # TODO(catalin): may want to add the table options to schema.options
    schema.fields.extend([ConvertColumn(column) for column in table.columns])
    return schema
