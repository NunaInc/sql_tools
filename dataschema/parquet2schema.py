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
"""Converts Arrow (and Parquet) schema to python data schema."""
import pyarrow
import urllib.parse

from pyarrow import fs, parquet
from typing import Optional, Union
from dataschema import Schema_pb2, Schema


def _GetIntType(dt: pyarrow.DataType) -> int:
    if pyarrow.types.is_signed_integer(dt):
        if pyarrow.types.is_int8(dt):
            return Schema_pb2.ColumnInfo.TYPE_INT_8
        if pyarrow.types.is_int16(dt):
            return Schema_pb2.ColumnInfo.TYPE_INT_16
        if pyarrow.types.is_int32(dt):
            return Schema_pb2.ColumnInfo.TYPE_INT_32
        if pyarrow.types.is_int64(dt):
            return Schema_pb2.ColumnInfo.TYPE_INT_64
    else:
        if pyarrow.types.is_uint8(dt):
            return Schema_pb2.ColumnInfo.TYPE_UINT_8
        if pyarrow.types.is_uint16(dt):
            return Schema_pb2.ColumnInfo.TYPE_UINT_16
        if pyarrow.types.is_uint32(dt):
            return Schema_pb2.ColumnInfo.TYPE_UINT_32
        if pyarrow.types.is_uint64(dt):
            return Schema_pb2.ColumnInfo.TYPE_UINT_64
    raise ValueError(f'Unsupported arrow integer type: {dt}')


def _GetFloatType(dt: pyarrow.DataType) -> int:
    if pyarrow.types.is_float16(dt) or pyarrow.types.is_float32(dt):
        return Schema_pb2.ColumnInfo.TYPE_FLOAT_32
    if pyarrow.types.is_float64(dt):
        return Schema_pb2.ColumnInfo.TYPE_FLOAT_64
    raise ValueError(f'Unsupported arrow floating type: {dt}')


def _SetTemporalType(field: pyarrow.Field, column: Schema.Column):
    if pyarrow.types.is_date64(field.type):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_DATETIME_64
    elif pyarrow.types.is_date32(field.type):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_DATE
    elif pyarrow.types.is_timestamp(field.type):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_DATETIME_64
        if (field.type.tz is not None) or (field.type.unit is not None):
            ti = Schema_pb2.ColumnInfo.TimestampInfo()
            if field.type.tz is not None:
                ti.timezone = field.type.tz
            if field.type.unit == 's':
                ti.precision = 0
            elif field.type.unit == 'ms':
                ti.precision = 3
            elif field.type.unit == 'us':
                ti.precision = 6
            elif field.type.unit == 'ns':
                ti.precision = 9
            else:
                raise ValueError(f'Unknown timestamp unit {field.type.unit} '
                                 f'for field {field}')
            column.info.timestamp_info.CopyFrom(ti)
    else:
        raise ValueError(
            f'Unsupported arrow temporal type for field: `{field}`')


def _SetListType(field: pyarrow.Field, column: Schema.Column):
    if (pyarrow.types.is_nested(field.type.value_type) or
            field.type.value_field.nullable):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_ARRAY
    else:
        _SetType(field.type.value_field, column)


def _SetDictType(field: pyarrow.Field, column: Schema.Column):
    if field.type.value_type.is_nested() or field.type.value_field.nullable:
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_SET
    else:
        _SetType(field.type.value_field, column)


def _SetType(field: pyarrow.Field, column: Schema.Column):
    if pyarrow.types.is_integer(field.type):
        column.info.column_type = _GetIntType(field.type)
    elif pyarrow.types.is_floating(field.type):
        column.info.column_type = _GetFloatType(field.type)
    elif pyarrow.types.is_temporal(field.type):
        _SetTemporalType(field, column)
    elif pyarrow.types.is_decimal(field.type):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_DECIMAL
        column.info.decimal_info.CopyFrom(
            Schema_pb2.ColumnInfo.DecimalInfo(precision=field.type.precision,
                                              scale=field.type.scale))
    elif pyarrow.types.is_boolean(field.type):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_BOOLEAN
    elif pyarrow.types.is_string(field.type):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_STRING
    elif pyarrow.types.is_large_string(field.type):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_STRING
        column.has_data_annotation = True
        column.data_annotation.is_lob = True
    elif (pyarrow.types.is_binary(field.type) or
          pyarrow.types.is_fixed_size_binary(field.type)):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_BYTES
    elif pyarrow.types.is_large_binary(field.type):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_BYTES
        column.has_data_annotation = True
        column.data_annotation.is_lob = True
    elif pyarrow.types.is_list(field.type):
        _SetListType(field, column)
    elif (pyarrow.types.is_dictionary(field.type) or
          pyarrow.types.is_map(field.type)):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_MAP
    elif pyarrow.types.is_nested(field.type):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_NESTED
    else:
        raise ValueError(f'Cannot convert parquet type for field `{field}`')


def _GetNullLabel(is_nullable: bool) -> int:
    if is_nullable:
        return Schema_pb2.ColumnInfo.LABEL_OPTIONAL
    return Schema_pb2.ColumnInfo.LABEL_REQUIRED


def _SetLabel(field: pyarrow.Field, column: Schema.Column):
    if (pyarrow.types.is_list(field.type) or
            pyarrow.types.is_dictionary(field.type) or
            pyarrow.types.is_map(field.type)):
        column.info.label = Schema_pb2.ColumnInfo.LABEL_REPEATED
    else:
        column.info.label = _GetNullLabel(field.nullable)


def _FieldStructTable(field: pyarrow.Field, table_full_name: str,
                      table_java_class: str) -> Schema.Table:
    field_type = None
    type_name = None
    if pyarrow.types.is_struct(field.type):
        type_name = f'Nested_{field.name}'
        field_type = field.type
    elif pyarrow.types.is_list(field.type):
        if (pyarrow.types.is_nested(field.type.value_type) or
                field.type.value_field.nullable):
            type_name = f'Array_{field.name}'
            field_type = pyarrow.struct([
                pyarrow.field('element',
                              field.type.value_type,
                              nullable=field.type.value_field.nullable)
            ])
    elif pyarrow.types.is_dictionary(field.type):
        type_name = f'Dict_{field.name}'
        field_type = pyarrow.struct([
            pyarrow.field('key', field.type.index_type, nullable=False),
            pyarrow.field('value', field.type.value_type, nullable=True)
        ])
    elif pyarrow.types.is_map(field.type):
        type_name = f'Map_{field.name}'
        field_type = pyarrow.struct([
            pyarrow.field('key', field.type.key_type, nullable=False),
            pyarrow.field('value', field.type.item_type, nullable=True)
        ])
    if type_name is None or field_type is None:
        return None
    return ConvertSchema(field_type, Schema.full_name(table_full_name,
                                                      type_name),
                         Schema.full_name(table_java_class, type_name))


def ConvertField(field: pyarrow.Field, table_full_name: str,
                 java_class_name: str) -> Schema.Column:
    """Converts an arrow table schema field to a schema column."""
    column = Schema.Column()
    column.field = field
    column.source_type = Schema.SourceType.ARROW
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


def ConvertSchema(schema: Union[pyarrow.Schema, pyarrow.StructType],
                  full_name: Optional[str] = 'adhoc.Parquet',
                  java_package: Optional[str] = '') -> Schema.Table:
    """Converts an Arrow schema, or a struct type to a table."""
    table = Schema.Table()
    table.msg = schema
    if isinstance(schema, pyarrow.Schema):
        table.source_type = Schema.SourceType.ARROW
        fields = [schema.field(i) for i in range(len(schema))]
    elif pyarrow.types.is_struct(schema):
        table.source_type = Schema.SourceType.ARROW_STRUCT
        fields = [schema[i] for i in range(len(schema))]
    else:
        raise ValueError(
            f'Cannot convert python schema from this object: {schema}')
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
    for field in fields:
        (column, field_sub_table) = ConvertField(field, full_name,
                                                 java_class_name)
        if field_sub_table is not None:
            table.add_nested(field_sub_table)
        table.add_column(column)
    return table


def ConvertParquetSchema(pfile: parquet.ParquetFile,
                         full_name: Optional[str] = 'adhoc.Parquet',
                         java_package: Optional[str] = '') -> Schema.Table:
    """Extracts schema from a parquet file."""
    return ConvertSchema(pfile.schema_arrow, full_name, java_package)


def OpenParquetFile(file_url: str) -> parquet.ParquetFile:
    """Opens a parquet file from an url/path. Use `s3` or `hdfs` as schemes."""
    info = urllib.parse.urlparse(file_url)
    if not info.scheme or info.scheme == 'file':
        return parquet.ParquetFile(info.path)
    if info.scheme == 's3':
        query_params = urllib.parse.parse_qs(info.query)
        fs_s3 = fs.S3FileSystem(access_key=query_params.get('access_key', None),
                                secret_key=query_params.get('secret_key', None),
                                session_token=query_params.get(
                                    'session_token', None))
        return parquet.ParquetFile(fs_s3.open_input_file(info.path))
    if info.scheme == 'hdfs':
        port = info.port if info.port is not None else 8020
        fs_hdfs = fs.HadoopFileSystem(host=info.host, port=port)
        return parquet.ParquetFile(fs_hdfs.open_input_file(info.path))
    raise ValueError(f'Unknown url scheme `{info.scheme}` for parquet file')
