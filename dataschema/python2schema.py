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
"""Converts a schema represented as Python dataclasses to Schema structures."""
import dataclasses
import datetime
import decimal
import sys

from dataschema import Schema, Schema_pb2, entity, strutil
from typing import Optional


def _GetLabel(field: dataclasses.Field) -> Optional[int]:
    field_type = entity.GetAnnotatedType(field.type)
    if not hasattr(field_type, '__origin__'):
        return Schema_pb2.ColumnInfo.LABEL_REQUIRED
    if entity.GetOptionalType(field_type) is not None:
        return Schema_pb2.ColumnInfo.LABEL_OPTIONAL
    if entity.GetStructuredTypeName(field_type) is not None:
        return Schema_pb2.ColumnInfo.LABEL_REPEATED
    raise ValueError(f'Cannot determine label for datafieldd `{field.name}` '
                     f'of type {field_type}.')


_BASE_TYPES = {
    int: Schema_pb2.ColumnInfo.TYPE_INT_64,
    bytes: Schema_pb2.ColumnInfo.TYPE_BYTES,
    str: Schema_pb2.ColumnInfo.TYPE_STRING,
    float: Schema_pb2.ColumnInfo.TYPE_FLOAT_64,
    bool: Schema_pb2.ColumnInfo.TYPE_BOOLEAN,
    datetime.date: Schema_pb2.ColumnInfo.TYPE_DATE,
    datetime.datetime: Schema_pb2.ColumnInfo.TYPE_DATETIME_64,
    decimal.Decimal: Schema_pb2.ColumnInfo.TYPE_DECIMAL
}


def _GetColumnType(field_cls: type) -> Optional[int]:
    opt_type = entity.GetOptionalType(field_cls)
    if opt_type is not None:
        field_cls = opt_type
    if entity.IsBasicType(field_cls):
        return _BASE_TYPES[entity.GetOriginalType(field_cls)]
    field_cls = entity.GetAnnotatedType(field_cls)
    if dataclasses.is_dataclass(field_cls):
        return Schema_pb2.ColumnInfo.TYPE_NESTED
    struct_name = entity.GetStructuredTypeName(field_cls)
    if struct_name == 'dict':
        return Schema_pb2.ColumnInfo.TYPE_MAP
    if struct_name == 'set':
        return Schema_pb2.ColumnInfo.TYPE_SET
    if struct_name == 'list':
        if (entity.GetOptionalType(field_cls.__args__[0]) or
                entity.GetStructuredTypeName(field_cls.__args__[0])):
            return Schema_pb2.ColumnInfo.TYPE_ARRAY
        return _GetColumnType(field_cls.__args__[0])


def _BuildField(name, cls):
    sub_field = dataclasses.field()
    sub_field.name = name
    sub_field.type = cls
    return sub_field


_MAX_DEPTH = 15


def ConvertField(field: dataclasses.Field,
                 table_name: str,
                 java_class_name: str,
                 convert_sub_fields: bool = True,
                 depth: int = 0) -> Schema.Column:
    column = Schema.Column()
    column.table_name = table_name
    column.java_class_name = java_class_name
    column.field = field
    column.source_type = Schema.SourceType.DATACLASS
    entity.FieldTypeChecker(field.name, field.type).check()
    column.info.column_type = _GetColumnType(field.type)
    column.info.name = field.name
    column.info.label = _GetLabel(field)
    if column.info.column_type == Schema_pb2.ColumnInfo.TYPE_SET:
        column.info.repeated_semantics = Schema_pb2.ColumnInfo.REPEATED_SET
    for annot in entity.SchemaAnnotations(field.type):
        annot.annotate_column(column)
    if column.info.column_type == Schema_pb2.ColumnInfo.TYPE_NESTED:
        msg_type = entity.GetOriginalType(field.type)
        if entity.GetStructuredTypeName(msg_type):
            msg_type = msg_type.__args__[0]
        column.info.message_name = Schema.full_name(msg_type.__module__,
                                                    msg_type.__qualname__)
    if (convert_sub_fields and
            column.info.column_type == Schema_pb2.ColumnInfo.TYPE_NESTED):
        sub_type = entity.GetOriginalType(field.type)
        if entity.GetStructuredTypeName(sub_type):
            sub_type = sub_type.__args__[0]
        sub_table = ConvertDataclass(sub_type, convert_sub_fields)
        for sub_column in sub_table.columns:
            column.add_sub_column(sub_column)
    elif column.info.column_type in [
            Schema_pb2.ColumnInfo.TYPE_SET, Schema_pb2.ColumnInfo.TYPE_ARRAY
    ]:
        column.add_sub_column(
            ConvertField(
                _BuildField('element',
                            entity.GetAnnotatedType(field.type).__args__[0]),
                table_name, java_class_name, convert_sub_fields, depth + 1))
    elif column.info.column_type == Schema_pb2.ColumnInfo.TYPE_MAP:
        column.add_sub_column(
            ConvertField(
                _BuildField('key',
                            entity.GetAnnotatedType(field.type).__args__[0]),
                table_name, java_class_name, convert_sub_fields, depth + 1))
        column.add_sub_column(
            ConvertField(
                _BuildField('value',
                            entity.GetAnnotatedType(field.type).__args__[1]),
                table_name, java_class_name, convert_sub_fields, depth + 1))
    return column


def ConvertDataclass(msg: type,
                     convert_sub_fields: bool = True) -> Schema.Table:
    table = Schema.Table()
    table.msg = msg
    table.source_type = Schema.SourceType.DATACLASS
    errors = entity.DataclassChecker(msg).check()
    if errors:
        errors_str = '; '.join(errors)
        raise ValueError(f'Invalid dataclass for table creation: {errors_str}')
    table.info.name = msg.__name__
    table.info.full_name = Schema.full_name(msg.__module__, msg.__qualname__)
    table.info.package = strutil.StripSuffix(table.info.full_name,
                                             f'.{msg.__name__}')
    table.info.java_package = strutil.StripSuffix(
        Schema.full_name(entity.GetJavaPackage(sys.modules[msg.__module__]),
                         msg.__qualname__), f'.{msg.__name__}')
    for annot in entity.SchemaAnnotations(msg):
        annot.annotate_table(table)
    for field in dataclasses.fields(msg):
        table.add_column(
            ConvertField(
                field, table.info.full_name,
                Schema.full_name(table.info.java_package, table.info.name),
                convert_sub_fields))
    for key in msg.__dict__:
        if dataclasses.is_dataclass(msg.__dict__[key]):
            table.add_nested(
                ConvertDataclass(msg.__dict__[key], convert_sub_fields))
    return table
