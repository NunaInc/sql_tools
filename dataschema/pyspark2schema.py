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
"""Converts PySpark types to Schema."""

from pyspark.sql import types
from dataschema import Schema_pb2, Schema
from typing import Optional


def _SetType(field: types.StructField, column: Schema.Column):
    if isinstance(field.dataType, types.BooleanType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_BOOLEAN
    elif isinstance(field.dataType, types.ByteType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_INT_8
    elif isinstance(field.dataType, types.ShortType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_INT_16
    elif isinstance(field.dataType, types.IntegerType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_INT_32
    elif isinstance(field.dataType, types.LongType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_INT_64
    elif isinstance(field.dataType, types.FloatType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_FLOAT_32
    elif isinstance(field.dataType, types.DoubleType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_FLOAT_64
    elif isinstance(field.dataType, types.DateType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_DATE
    elif isinstance(field.dataType, types.TimestampType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_DATETIME_64
        # PySpark operates at microsecond precision by default:
        column.info.timestamp_info.CopyFrom(
            Schema_pb2.ColumnInfo.TimestampInfo(precision=6))
    elif isinstance(field.dataType, types.DecimalType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_DECIMAL
        column.info.decimal_info.CopyFrom(
            Schema_pb2.ColumnInfo.DecimalInfo(
                precision=field.dataType.precision, scale=field.dataType.scale))
    elif isinstance(field.dataType, types.StringType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_STRING
    elif isinstance(field.dataType, types.BinaryType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_BYTES
    elif isinstance(field.dataType, types.ArrayType):
        if (isinstance(field.dataType.elementType,
                       (types.ArrayType, types.MapType)) or
                field.dataType.containsNull):
            column.info.column_type = Schema_pb2.ColumnInfo.TYPE_ARRAY
        else:
            _SetType(
                types.StructField('element', field.dataType.elementType,
                                  field.dataType.containsNull), column)
    elif isinstance(field.dataType, types.MapType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_MAP
    elif isinstance(field.dataType, types.StructType):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_NESTED
    else:
        raise ValueError(f'Cannot convert parquet type for field `{field}`')


def _GetNullLabel(is_nullable: bool) -> int:
    if is_nullable:
        return Schema_pb2.ColumnInfo.LABEL_OPTIONAL
    return Schema_pb2.ColumnInfo.LABEL_REQUIRED


def _SetLabel(field: types.StructField, column: Schema.Column):
    if isinstance(field.dataType, (types.MapType, types.ArrayType)):
        column.info.label = Schema_pb2.ColumnInfo.LABEL_REPEATED
    else:
        column.info.label = _GetNullLabel(field.nullable)


def _FieldStructTable(field: types.StructField, table_full_name: str,
                      table_java_class: str) -> Schema.Table:
    field_type = None
    type_name = None
    if isinstance(field.dataType, types.StructType):
        type_name = f'Nested_{field.name}'
        field_type = field.dataType
    elif isinstance(field.dataType, types.ArrayType):
        if (isinstance(field.dataType.elementType,
                       (types.ArrayType, types.MapType, types.StructType)) or
                field.dataType.containsNull):
            type_name = f'Array_{field.name}'
            if isinstance(field.dataType.elementType, types.StructType):
                field_type = field.dataType.elementType
            else:
                field_type = types.StructType([
                    types.StructField('element', field.dataType.elementType,
                                      field.dataType.containsNull)
                ])
    elif isinstance(field.dataType, types.MapType):
        type_name = f'Map_{field.name}'
        field_type = types.StructType([
            types.StructField('key', field.dataType.keyType, False),
            types.StructField('value', field.dataType.valueType,
                              field.dataType.valueContainsNull)
        ])
    if type_name is None or field_type is None:
        return None
    return ConvertStructType(field_type,
                             Schema.full_name(table_full_name, type_name),
                             Schema.full_name(table_java_class, type_name))


def ConvertField(field: types.StructField, table_full_name: str,
                 java_class_name: str) -> Schema.Column:
    """Converts a Spark field to a Schema column."""
    column = Schema.Column()
    column.field = field
    column.source_type = Schema.SourceType.PYSPARK
    column.table_name = table_full_name
    column.java_class_name = java_class_name
    column.info.name = field.name
    _SetType(field, column)
    _SetLabel(field, column)
    field_sub_table = _FieldStructTable(field, table_full_name, java_class_name)
    if field_sub_table is not None:
        for sub_column in field_sub_table.columns:
            column.add_sub_column(sub_column)
    return (column, field_sub_table)


def ConvertStructType(struct: types.StructType,
                      full_name: Optional[str] = 'adhoc.PySparkStruct',
                      java_package: Optional[str] = '') -> Schema.Table:
    """Converts a Spark structured type to a Schema table."""
    table = Schema.Table()
    table.msg = struct
    table.source_type = Schema.SourceType.PYSPARK
    components = full_name.split('.')
    table.info.package = '.'.join(
        components[:-1]) if len(components) > 1 else ''
    if java_package:
        table.info.java_package = java_package
    else:
        table.info.java_package = Schema.java_package_from_python(
            table.info.package)
    table.info.name = components[-1]
    table.info.full_name = full_name
    java_class_name = Schema.full_name(table.info.java_package, table.info.name)
    for field in struct.fields:
        (column, field_sub_table) = ConvertField(field, full_name,
                                                 java_class_name)
        if field_sub_table is not None:
            table.add_nested(field_sub_table)
        table.add_column(column)
    return table
