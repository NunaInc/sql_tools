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
"""Converts a schema represented as Protocol Buffer to Schema structures."""

import json
from dataschema import Schema_pb2, Schema, strutil
from google.protobuf import descriptor
from typing import Optional

_ACCEPTABLE_TYPES = {
    descriptor.FieldDescriptor.CPPTYPE_BOOL:
        (Schema_pb2.ColumnInfo.TYPE_BOOLEAN, []),
    descriptor.FieldDescriptor.CPPTYPE_DOUBLE:
        (Schema_pb2.ColumnInfo.TYPE_FLOAT_64, []),
    descriptor.FieldDescriptor.CPPTYPE_FLOAT:
        (Schema_pb2.ColumnInfo.TYPE_FLOAT_32, []),
    descriptor.FieldDescriptor.CPPTYPE_ENUM:
        (Schema_pb2.ColumnInfo.TYPE_STRING, [Schema_pb2.ColumnInfo.TYPE_INT_32]
        ),
    descriptor.FieldDescriptor.CPPTYPE_INT32:
        (Schema_pb2.ColumnInfo.TYPE_INT_32, [
            Schema_pb2.ColumnInfo.TYPE_DATE, Schema_pb2.ColumnInfo.TYPE_INT_8,
            Schema_pb2.ColumnInfo.TYPE_INT_16
        ]),
    descriptor.FieldDescriptor.CPPTYPE_INT64:
        (Schema_pb2.ColumnInfo.TYPE_INT_64,
         [Schema_pb2.ColumnInfo.TYPE_DATETIME_64]),
    descriptor.FieldDescriptor.CPPTYPE_UINT32:
        (Schema_pb2.ColumnInfo.TYPE_UINT_32, [
            Schema_pb2.ColumnInfo.TYPE_UINT_8,
            Schema_pb2.ColumnInfo.TYPE_UINT_16
        ]),
    descriptor.FieldDescriptor.CPPTYPE_UINT64:
        (Schema_pb2.ColumnInfo.TYPE_UINT_64, []),
    descriptor.FieldDescriptor.CPPTYPE_STRING:
        (Schema_pb2.ColumnInfo.TYPE_STRING, []),
    descriptor.FieldDescriptor.CPPTYPE_MESSAGE:
        (Schema_pb2.ColumnInfo.TYPE_NESTED, [
            Schema_pb2.ColumnInfo.TYPE_ARRAY, Schema_pb2.ColumnInfo.TYPE_SET,
            Schema_pb2.ColumnInfo.TYPE_MAP
        ])
}
_MAX_DEPTH = 15


def _GetAnnotatedType(field: descriptor.FieldDescriptor) -> Optional[int]:
    """Returns the column type annotation of a field."""
    field_opt = field.GetOptions()
    if not field_opt.HasExtension(Schema_pb2.column_info):
        return None
    opt = field_opt.Extensions[Schema_pb2.column_info]
    if not opt.HasField('column_type'):
        return None
    return opt.column_type


def _IsNestedElement(field: descriptor.FieldDescriptor,
                     field_type: int) -> bool:
    if (field.label != descriptor.FieldDescriptor.LABEL_REPEATED or
            field.cpp_type != descriptor.FieldDescriptor.CPPTYPE_MESSAGE or
        (_GetAnnotatedType(field) != field_type) or
            field.message_type is None or len(field.message_type.fields) != 1 or
            field.message_type.fields[0].name != 'element'):
        return False
    return True


def _IsNestedArray(field: descriptor.FieldDescriptor) -> bool:
    """If this is a nested field that translates to an array."""
    return _IsNestedElement(field, Schema_pb2.ColumnInfo.TYPE_ARRAY)


def _IsNestedSet(field: descriptor.FieldDescriptor) -> bool:
    """If this is a nested field that translates to a set."""
    return _IsNestedElement(field, Schema_pb2.ColumnInfo.TYPE_SET)


def _IsNestedMap(field: descriptor.FieldDescriptor) -> bool:
    """If the field is a nested field that translates to a map."""
    if not field.message_type.GetOptions().map_entry:
        return False
    if (len(field.message_type.fields) != 2 or
            field.message_type.fields[0].name != 'key' or
            field.message_type.fields[1].name != 'value'):
        raise ValueError(
            f'Badly formatted map message type for field: `{field.name}`')
    return True


def _GetColumnType(field: descriptor.FieldDescriptor) -> Optional[int]:
    if field.cpp_type not in _ACCEPTABLE_TYPES:
        raise ValueError(f'Unknown field cpp type {field.cpp_type} '
                         f'for {field.name}')
    (default_type, acceptable_types) = _ACCEPTABLE_TYPES[field.cpp_type]
    if (field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_STRING and
            field.type == descriptor.FieldDescriptor.TYPE_BYTES):
        default_type = Schema_pb2.ColumnInfo.TYPE_BYTES
        acceptable_types = [Schema_pb2.ColumnInfo.TYPE_DECIMAL]
    result_type = _GetAnnotatedType(field)
    if result_type is None:
        result_type = default_type
    if result_type != default_type and result_type not in acceptable_types:
        raise ValueError(f'Annotated type {result_type} is not acceptable '
                         f'for proto type {field.type}')
    return result_type


def _GetNestedType(field: descriptor.FieldDescriptor) -> Optional[int]:
    if field.message_type is None:
        raise ValueError(f'Message field `{field.name}` does not have '
                         'a `message_type` set.')
    if _IsNestedArray(field):
        return Schema_pb2.ColumnInfo.TYPE_ARRAY
    if _IsNestedSet(field):
        return Schema_pb2.ColumnInfo.TYPE_SET
    if _IsNestedMap(field):
        return Schema_pb2.ColumnInfo.TYPE_MAP
    return Schema_pb2.ColumnInfo.TYPE_NESTED


def _GetType(field: descriptor.FieldDescriptor) -> Optional[int]:
    column_type = _GetColumnType(field)
    if field.cpp_type != descriptor.FieldDescriptor.CPPTYPE_MESSAGE:
        return column_type
    nested_type = _GetNestedType(field)
    if column_type not in [nested_type, Schema_pb2.ColumnInfo.TYPE_NESTED]:
        raise ValueError(f'Message field `{field.name}` has annotations that '
                         'contradict its structure.')
    return nested_type


def _GetLabel(field: descriptor.FieldDescriptor) -> Optional[int]:
    if field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
        return Schema_pb2.ColumnInfo.LABEL_REPEATED
    if field.label == descriptor.FieldDescriptor.LABEL_REQUIRED:
        return Schema_pb2.ColumnInfo.LABEL_REQUIRED
    if field.label == descriptor.FieldDescriptor.LABEL_OPTIONAL:
        return Schema_pb2.ColumnInfo.LABEL_OPTIONAL
    raise ValueError(f'Message field `{field.name}` has no label.')


def _CheckDefaultColumnInfo(field: descriptor.FieldDescriptor):
    field_opt = field.GetOptions()
    if not field_opt.HasExtension(Schema_pb2.column_info):
        return True
    info = field_opt.Extensions[Schema_pb2.column_info]
    for fn in ['label', 'name', 'message_name', 'field_id']:
        if info.HasField(fn):
            raise ValueError(f'Unexpected column_info annotation `{fn}` '
                             f'for field `{field.name}`')
    if info.field:
        raise ValueError('Unexpected column_info annotation `field` for field '
                         f'`{fn}` - {info.field}')
    return True


def _MessageFullJavaName(msg: descriptor.Descriptor):
    crt_msg = msg
    names = []
    while crt_msg is not None:
        names.append(crt_msg.name)
        crt_msg = crt_msg.containing_type
    names.reverse()
    return Schema.full_name(msg.file.GetOptions().java_package, '.'.join(names))


def ConvertField(field: descriptor.FieldDescriptor,
                 convert_sub_fields: bool = True,
                 depth: int = 0) -> Schema.Column:
    """Converts a proto field descriptor to a schema Column."""
    column = Schema.Column()
    column.field = field
    column.source_type = Schema.SourceType.PROTO
    column.table_name = field.containing_type.full_name
    column.java_class_name = _MessageFullJavaName(field.containing_type)
    column.info.column_type = _GetType(field)
    column.info.name = field.name
    column.info.label = _GetLabel(field)
    column.info.field_id = field.number
    field_opt = field.GetOptions()
    if field_opt.HasExtension(Schema_pb2.column_info):
        _CheckDefaultColumnInfo(field)
        column.info.CopyFrom(field_opt.Extensions[Schema_pb2.column_info])
    column.has_data_annotation = field_opt.HasExtension(
        Schema_pb2.column_data_annotation)
    if column.has_data_annotation:
        column.data_annotation.CopyFrom(
            field_opt.Extensions[Schema_pb2.column_data_annotation])
    column.has_clickhouse_annotation = field_opt.HasExtension(
        Schema_pb2.column_clickhouse_annotation)
    if column.has_clickhouse_annotation:
        column.clickhouse_annotation.CopyFrom(
            field_opt.Extensions[Schema_pb2.column_clickhouse_annotation])
    column.has_scala_annotation = field_opt.HasExtension(
        Schema_pb2.column_scala_annotation)
    if column.has_scala_annotation:
        column.scala_annotation.CopyFrom(
            field_opt.Extensions[Schema_pb2.column_scala_annotation])
    if (column.field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_ENUM and
            column.info.column_type == Schema_pb2.ColumnInfo.TYPE_STRING):
        column.has_clickhouse_annotation = True
        column.clickhouse_annotation.is_low_cardinality = True
    column.info.column_type = _GetType(field)
    column.info.name = field.name
    column.info.label = _GetLabel(field)
    column.info.field_id = field.number
    column.info.deprecated = field_opt.deprecated
    if field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_MESSAGE:
        column.info.message_name = field.message_type.full_name
    if field.message_type and (
            convert_sub_fields or column.info.column_type in [
                Schema_pb2.ColumnInfo.TYPE_SET,
                Schema_pb2.ColumnInfo.TYPE_ARRAY, Schema_pb2.ColumnInfo.TYPE_MAP
            ]):
        if depth + 1 > _MAX_DEPTH:
            raise ValueError(
                'Max depth of fields encountered - make sure your structure does '
                'not include recursive structures.')
        for sub_field in field.message_type.fields:
            column.add_sub_column(
                ConvertField(sub_field,
                             convert_sub_fields=convert_sub_fields,
                             depth=depth + 1))
    if column.info.synthgen_spec:
        column.synthgen = json.loads(column.info.synthgen_spec)
    return column


def ConvertMessage(msg: descriptor.Descriptor,
                   convert_sub_fields: bool = True) -> Schema.Table:
    """Converts a proto message descriptor to a schema Table."""
    table = Schema.Table()
    table.msg = msg
    table.source_type = Schema.SourceType.PROTO
    table.info.package = strutil.StripSuffix(msg.full_name, f'.{msg.name}')
    table.info.java_package = strutil.StripSuffix(_MessageFullJavaName(msg),
                                                  f'.{msg.name}')
    msg_opt = msg.GetOptions()
    table.has_data_annotation = msg_opt.HasExtension(
        Schema_pb2.table_data_annotation)
    if table.has_data_annotation:
        table.data_annotation.CopyFrom(
            msg_opt.Extensions[Schema_pb2.table_data_annotation])
    table.has_clickhouse_annotation = msg_opt.HasExtension(
        Schema_pb2.table_clickhouse_annotation)
    if table.has_clickhouse_annotation:
        table.clickhouse_annotation.CopyFrom(
            msg_opt.Extensions[Schema_pb2.table_clickhouse_annotation])
    table.has_scala_annotation = msg_opt.HasExtension(
        Schema_pb2.table_scala_annotation)
    if table.has_scala_annotation:
        table.scala_annotation.CopyFrom(
            msg_opt.Extensions[Schema_pb2.table_scala_annotation])
    table.info.name = msg.name
    table.info.full_name = msg.full_name
    for field in msg.fields:
        table.add_column(ConvertField(field, convert_sub_fields))
    for nested_msg in msg.nested_types:
        if not nested_msg.GetOptions().map_entry:
            table.add_nested(ConvertMessage(nested_msg, convert_sub_fields))
    return table
