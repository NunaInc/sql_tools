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
"""Converts Schema to Clickhouse specific SQL create table statement."""

import dataclasses
import os
from google.protobuf import descriptor
from dataschema import Schema, Schema_pb2, proto2schema, python2schema, strutil
from types import ModuleType
from typing import Dict, List, Optional


def GetIndent(indent: int) -> str:
    return ' ' * indent


def GetTimestampStr(column: Schema.Column) -> str:
    info = column.timestamp_info()
    if info is None:
        return ''
    s = f'({info.precision}'
    if info.timezone:
        s += f', "{info.timezone}"'
    s += ')'
    return s


CLICKHOUSE_TYPE_NAME = {
    Schema_pb2.ColumnInfo.TYPE_STRING: 'String',
    Schema_pb2.ColumnInfo.TYPE_BYTES: 'String',
    Schema_pb2.ColumnInfo.TYPE_BOOLEAN: 'UInt8',
    Schema_pb2.ColumnInfo.TYPE_INT_8: 'Int8',
    Schema_pb2.ColumnInfo.TYPE_INT_16: 'Int16',
    Schema_pb2.ColumnInfo.TYPE_INT_32: 'Int32',
    Schema_pb2.ColumnInfo.TYPE_INT_64: 'Int64',
    Schema_pb2.ColumnInfo.TYPE_UINT_8: 'UInt8',
    Schema_pb2.ColumnInfo.TYPE_UINT_16: 'UInt16',
    Schema_pb2.ColumnInfo.TYPE_UINT_32: 'UInt32',
    Schema_pb2.ColumnInfo.TYPE_UINT_64: 'UInt64',
    Schema_pb2.ColumnInfo.TYPE_DECIMAL: 'Decimal',
    Schema_pb2.ColumnInfo.TYPE_FLOAT_32: 'Float32',
    Schema_pb2.ColumnInfo.TYPE_FLOAT_64: 'Float64',
    Schema_pb2.ColumnInfo.TYPE_DATE: 'Date',
    Schema_pb2.ColumnInfo.TYPE_DATETIME_64: 'DateTime64',
    Schema_pb2.ColumnInfo.TYPE_NESTED: 'Nested',
    Schema_pb2.ColumnInfo.TYPE_ARRAY: 'Array',
    Schema_pb2.ColumnInfo.TYPE_SET: 'Set',
}


class TableConverter:
    """Converts a schema Table to a SQL create table statement."""

    def __init__(self, table: Schema.Table):
        self.table = table

    def _get_decimal_str(self, column: Schema.Column) -> str:
        info = column.decimal_info()
        if info is None:
            raise ValueError(
                f'No decimal info for decimal column `{column.name()}`.')
        if info.precision <= 9:
            size = 32
        elif info.precision <= 18:
            size = 64
        elif info.precision <= 38:
            size = 128
        elif info.precision <= 76:
            size = 256
        else:
            raise ValueError('Decimal precision out of range for '
                             f'`{column.name()}`: {info.precision}')
        return f'Decimal{size}({info.scale})'

    def _get_timestamp_str(self, column: Schema.Column) -> str:
        return GetTimestampStr(column)

    def _get_compression_name(self, value: int) -> Optional[str]:
        if value == Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_LZ4:
            return 'LZ4'
        if value == Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_LZ4HC:
            return 'LZ4HC'
        elif value == Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_ZSTD:
            return 'ZSTD'
        elif value == Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_UNCOMPRESSED:
            return 'UNCOMPRESSED'
        return None

    def _get_codec(self, column: Schema.Column,
                   is_nested: bool) -> Optional[str]:
        """Extracts the Clickhouse encoding string for `column`."""
        if column.is_low_cardinality():
            # No compression for low cardinality allowed in clickhouse.
            return None
        if is_nested:
            # No compression for descendants of nested columns.
            return None
        # TODO: Support compression for de-sugared nested columns, e.g.:
        # `field.sub_field` String CODEC(ZSTD)
        codecs = []
        delta = column.clickhouse_annotation.delta_compression_width
        if delta:
            codecs.append(f'Delta({delta})')
        compression = self._get_compression_name(
            column.clickhouse_annotation.compression_type)
        if compression is None:
            # TODO: Support different default compression for nested tables,
            # currently uses default compression from parent table.
            compression = self._get_compression_name(
                self.table.clickhouse_annotation.default_compression)

        if compression is not None and compression != 'UNCOMPRESSED':
            level = column.clickhouse_annotation.compression_level
            if level:
                compression += f'({level})'
            codecs.append(compression)
        if codecs:
            return ', '.join(codecs)
        return None

    def _column_to_sql(self, column: Schema.Column, indent: int,
                       type_only: bool, is_nested: bool) -> str:
        """Returns a Clickhouse SQL column specification for `column`.

        Parameters:
        column: Column specification.
        indent: Number of indentations at previous level.
        type_only: Whether or not to return only the column type.
        is_nested: Whether or not the column is a descendant of a nested column.

        Returns:
        str: Clickhouse SQL column specification for `column`.
        """
        s = ''
        if not type_only:
            s += f'{GetIndent(indent)}{column.sql_name()} '
        end = ''
        column_type = column.info.column_type
        if (column.info.label == Schema_pb2.ColumnInfo.LABEL_REPEATED and
                column_type != Schema_pb2.ColumnInfo.TYPE_MAP):
            s += 'Array('
            end += ')'
        if column.is_low_cardinality():
            s += 'LowCardinality('
            end += ')'
        # ClickHouse nested types (Nested, Tuple) cannot be inside a Nullable.
        if (column.info.label == Schema_pb2.ColumnInfo.LABEL_OPTIONAL and
                column_type != Schema_pb2.ColumnInfo.TYPE_NESTED):
            s += 'Nullable('
            end += ')'
        if column_type == Schema_pb2.ColumnInfo.TYPE_MAP:
            ktype = self._column_to_sql(
                column.fields[0], 0, type_only=True, is_nested=is_nested)
            vtype = self._column_to_sql(
                column.fields[1], 0, type_only=True, is_nested=is_nested)
            s += f'Map({ktype}, {vtype})'
        elif column_type in [
                Schema_pb2.ColumnInfo.TYPE_ARRAY, Schema_pb2.ColumnInfo.TYPE_SET
        ]:
            s += self._column_to_sql(
                column.fields[0], 0, type_only=True, is_nested=is_nested)
        elif column.clickhouse_annotation.type_name:
            s += column.clickhouse_annotation.type_name
        elif column_type == Schema_pb2.ColumnInfo.TYPE_DECIMAL:
            s += self._get_decimal_str(column)
        else:
            if column_type not in CLICKHOUSE_TYPE_NAME:
                raise KeyError(
                    f'Unknown type to convert to clickhouse: {column_type}')
            if (column_type == Schema_pb2.ColumnInfo.TYPE_NESTED and
                    column.clickhouse_annotation.nested_type_name):
                s += column.clickhouse_annotation.nested_type_name
            else:
                s += CLICKHOUSE_TYPE_NAME[column_type]
            if column_type == Schema_pb2.ColumnInfo.TYPE_DECIMAL:
                s += self._get_decimal_str(column)
            elif column_type == Schema_pb2.ColumnInfo.TYPE_DATETIME_64:
                s += self._get_timestamp_str(column)
            elif column_type == Schema_pb2.ColumnInfo.TYPE_NESTED:
                sub_columns = []
                for sub_column in column.fields:
                    sub_columns.append(
                        self._column_to_sql(sub_column,
                                            indent + 2,
                                            type_only=False,
                                            is_nested=True))
                sub_columns_str = ',\n'.join(sub_columns)
                s += f'(\n{sub_columns_str}\n{GetIndent(indent)})'
        s += end
        if not type_only:
            codec = self._get_codec(column, is_nested=is_nested)
            if codec is not None:
                s += f' CODEC({codec})'
        return s

    def columns_sql(self, indent: int) -> List[str]:
        """Returns a list of Clickhouse SQL column specifications."""
        columns = []
        for column in self.table.columns:
            columns.append(self._column_to_sql(
                column, indent, type_only=False, is_nested=False))
        return columns

    def table_options(self, replication_params: str) -> str:
        """Extracts Clickhouse CREATE TABLE options for this message."""
        copt = []
        force_order_by = False
        if self.table.clickhouse_annotation.HasField('engine'):
            if (self.table.clickhouse_annotation.engine ==
                    Schema_pb2.TableClickhouseAnnotation.ENGINE_MERGE_TREE):
                force_order_by = True
                copt.append('ENGINE = MergeTree()')
            elif (self.table.clickhouse_annotation.engine ==
                  Schema_pb2.TableClickhouseAnnotation.ENGINE_LOG):
                copt.append('ENGINE = Log()')
            elif (self.table.clickhouse_annotation.engine ==
                  Schema_pb2.TableClickhouseAnnotation.ENGINE_TINY_LOG):
                copt.append('ENGINE = TinyLog()')
            elif (self.table.clickhouse_annotation.engine == Schema_pb2.
                  TableClickhouseAnnotation.ENGINE_REPLICATED_MERGE_TREE):
                force_order_by = True
                copt.append(
                    f'ENGINE = ReplicatedMergeTree({replication_params})')
        if self.table.clickhouse_annotation.order_by_fields:
            order_by = ', '.join(
                self.table.clickhouse_annotation.order_by_fields)
            copt.append(f'ORDER BY ({order_by})')
        elif force_order_by:
            copt.append('ORDER BY tuple()')
        if self.table.clickhouse_annotation.partition_by_sql_expression:
            partition_by = ', '.join(
                self.table.clickhouse_annotation.partition_by_sql_expression)
            copt.append(f'PARTITION BY ({partition_by})')
        if self.table.clickhouse_annotation.sample_by_sql_expression:
            sample_by = ', '.join(
                self.table.clickhouse_annotation.sample_by_sql_expression)
            copt.append(f'SAMPLE BY ({sample_by})')
        if self.table.clickhouse_annotation.index_granularity > 0:
            ig = self.table.clickhouse_annotation.index_granularity
            copt.append(f'SETTINGS index_granularity = {ig}')
        if self.table.data_annotation.comment:
            comment = "'" + repr('"' + self.table.data_annotation.comment)[2:]
            copt.append(f'COMMENT {comment}')
        return copt

    def to_sql(self,
               table_name: Optional[str] = '${database}.${table}',
               replication_params: str = '${replicationParams}',
               if_not_exists: Optional[bool] = False) -> str:
        """Returns a CREATE TABLE SQL statement for this message."""
        s = 'CREATE TABLE '
        if if_not_exists:
            s += 'IF NOT EXISTS '
        tname_str = table_name if table_name else self.table.name()
        columns_str = ',\n'.join(self.columns_sql(2))
        s += f'{tname_str} (\n{columns_str}\n)\n'
        copts = self.table_options(replication_params)
        if copts:
            copts_str = '\n'.join(copts)
            s += f'\n{copts_str}'
        return s

    def validate(self) -> bool:
        """Validates the message as a SQL table. Raises exceptions on errors."""
        return self.table.validate()


class FileConverter:
    """Converts a proto FileDescriptor to corresponding SQL table statement."""

    def __init__(self):
        self.name = None
        self.basename = None
        self.package = None
        self.converters = None

    def from_proto_file(
            self,
            file_descriptor: descriptor.FileDescriptor) -> 'FileConverter':
        self.name = file_descriptor.name
        self.basename = strutil.StripSuffix(
            os.path.basename(file_descriptor.name), '.proto')
        self.package = file_descriptor.package
        self.java_package = file_descriptor.GetOptions().java_package
        self.converters = [
            TableConverter(proto2schema.ConvertMessage(msg))
            for msg in file_descriptor.message_types_by_name.values()
        ]
        return self

    def from_module(self, py_module: ModuleType) -> 'FileConverter':
        self.name = py_module.__name__
        self.basename = strutil.StripSuffix(
            os.path.basename(py_module.__file__), '.py')
        self.package = py_module.__name__
        self.converters = [
            TableConverter(python2schema.ConvertDataclass(datacls))
            for datacls in py_module.__dict__.values()
            if dataclasses.is_dataclass(datacls)
        ]
        return self

    def get_path(self, dir_map, basename) -> str:
        """Returns directory path for saving SQL file `basename`."""
        end_path = os.path.join('/'.join(self.package.split('.')), basename)
        for k, v in dir_map.items():
            if self.name.startswith(k):
                return os.path.join(v, end_path)
        return end_path

    def to_sql(self,
               table_name: str = '${database}.${table}',
               replication_params: str = '${replicationParams}',
               if_not_exists: Optional[bool] = False):
        """Converts the messages in this file to several SQL CREATE TABLE."""
        result = {}
        for conv in self.converters:
            result[conv.table.name()] = conv.to_sql(table_name,
                                                    replication_params,
                                                    if_not_exists)
        return result

    def validate(self) -> bool:
        """Validates the messages and fields in this file for SQL correctness."""
        for conv in self.converters:
            conv.validate()
        return True


def ConvertTable(table: Schema.Table,
                 table_name: str = '${database}.${table}',
                 replication_params: str = '${replicationParams}',
                 if_not_exists: Optional[bool] = False) -> str:
    return TableConverter(table).to_sql(table_name, replication_params,
                                        if_not_exists)


class SchemaConverter:
    """Converts a list of file descriptors to SQL create statements."""

    def __init__(self):
        self.file_converters = []

    def add_descriptors(self,
                        file_descriptors: List[descriptor.FileDescriptor],
                        export_only: Optional[bool] = False):
        for fd in file_descriptors:
            try:
                fc = FileConverter().from_proto_file(fd)
                if not export_only:
                    self.file_converters.append(fc)
            except ValueError as e:
                raise ValueError(f'Processing proto file: {fd.name}') from e

    def add_modules(self,
                    py_modules: List[ModuleType],
                    export_only: Optional[bool] = False):
        for pym in py_modules:
            try:
                fc = FileConverter().from_module(pym)
                if not export_only:
                    self.file_converters.append(fc)
            except ValueError as e:
                raise ValueError(
                    f'Processing input pyton module: {pym.__name__}'
                    f' / {pym.__file__}') from e

    def to_sql_files(self,
                     dir_map: Dict[str, str],
                     table_name: Optional[str] = '${database}.${table}',
                     replication_params: str = '${replicationParams}',
                     if_not_exists: Optional[bool] = False) -> Dict[str, str]:
        files = {}
        for fc in self.file_converters:
            contents_map = fc.to_sql(table_name, replication_params,
                                     if_not_exists)
            for (crt_table_name, content) in contents_map.items():
                basename = f'{fc.basename}_{crt_table_name}.sql'
                path = fc.get_path(dir_map, basename)
                full_contents = f"""
--------------------------------------------------------------------------------
--
-- {path}
-- Generated from: {fc.name} / {crt_table_name}
--
{content}
"""
                files[path] = full_contents
        return files

    def validate(self) -> List[str]:
        """Validates the files in for SQL correctness. Returns a list of errors."""
        errors = []
        for fc in self.file_converters:
            try:
                fc.validate()
            except ValueError as e:
                errors.extend([
                    f'{fc.file_descriptor.name}: ERROR: {arg}' for arg in e.args
                ])
        if errors:
            return errors
        return None
