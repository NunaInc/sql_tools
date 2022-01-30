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
"""Utility to generate python code snippets for Schema tables / set of tables."""

from dataschema import Schema, Schema_pb2, strutil
from typing import List, Optional, Set


def _GetCompressionName(compression: Schema_pb2.ColumnClickhouseAnnotation):
    if compression == Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_LZ4:
        return 'LZ4'
    elif compression == Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_ZSTD:
        return 'ZSTD'
    elif compression == Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_UNCOMPRESSED:
        return 'UNCOMPRESSED'
    raise ValueError(f'Untreated compression value: {compression}')


def _GetTableAnnotation(table: Schema.Table):
    """Returns annotations needed for a dataclass to match the provided table."""
    annotations = []
    if table.data_annotation.comment:
        annotations.append(
            '@annotations.comment(value = '
            '"{strutil.EscapeString(table.data_annotation.comment)}")')
    if table.scala_annotation.is_entity:
        annotations.append('@annotations.entity_class()')
    if table.clickhouse_annotation.engine:
        engine = None
        if (table.clickhouse_annotation.engine ==
                Schema_pb2.TableClickhouseAnnotation.ENGINE_MERGE_TREE):
            engine = 'MERGE_TREE'
        elif (table.clickhouse_annotation.engine ==
              Schema_pb2.TableClickhouseAnnotation.ENGINE_LOG):
            engine = 'LOG'
        elif (table.clickhouse_annotation.engine ==
              Schema_pb2.TableClickhouseAnnotation.ENGINE_TINY_LOG):
            engine = 'TINY_LOG'
        elif (table.clickhouse_annotation.engine == Schema_pb2.
              TableClickhouseAnnotation.ENGINE_REPLICATED_MERGE_TREE):
            engine = 'REPLICATED_MERGE_TREE'
        if engine:
            annotations.append(
                f'@annotations.clickhouse_engine(engine="{engine}")')
    if table.clickhouse_annotation.order_by_fields:
        arg = ', '.join(
            f'"{field}"'
            for field in table.clickhouse_annotation.order_by_fields)
        annotations.append(f'@annotations.order_by(values=[{arg}])')
    if table.clickhouse_annotation.partition_by_sql_expression:
        arg = ', '.join(
            f'"{exp}"'
            for exp in table.clickhouse_annotation.partition_by_sql_expression)
        annotations.append(f'@annotations.partition_by(values=[{arg}])')
    if table.clickhouse_annotation.sample_by_sql_expression:
        arg = ', '.join(
            f'"{exp}"'
            for exp in table.clickhouse_annotation.sample_by_sql_expression)
        annotations.append(f'@annotations.sample_by(values=[{arg}])')
    if table.clickhouse_annotation.index_granularity:
        annotations.append('@annotations.index_granularity(value='
                           f'{table.clickhouse_annotation.index_granularity})')
    if table.clickhouse_annotation.default_compression:
        compression = _GetCompressionName(
            table.clickhouse_annotation.default_compression)
        annotations.append(
            f'@annotations.default_compression(value="{compression}")')

    return annotations


def _GetColumnAnnotations(column: Schema.Column):
    """Returns annotations needed for a field to match the provided column."""
    annotations = []
    if column.data_annotation.is_id:
        annotations.append('annotations.Id()')
    if column.data_annotation.comment:
        annotations.append(
            'annotations.Comment("' +
            strutil.EscapeString(column.data_annotation.comment) + '")')
    if column.info.HasField('decimal_info'):
        annotations.append(
            f'annotations.Decimal({column.info.decimal_info.precision}, '
            f'{column.info.decimal_info.scale})')
    if column.info.HasField('timestamp_info'):
        annotations.append(
            f'annotations.Timestamp({column.info.timestamp_info.precision}, '
            f'"{column.info.timestamp_info.timezone}")')
    if column.data_annotation.join_reference:
        annotations.append(
            'annotations.JoinReference("' +
            strutil.EscapeString(column.data_annotation.join_reference) + '")')
    if column.scala_annotation.original_name:
        annotations.append(
            'annotations.ScalaOriginalName("' +
            strutil.EscapeString(column.scala_annotation.original_name) + '")')
    if column.data_annotation.width:
        annotations.append(f'annotations.Width({column.data_annotation.width})')
    if column.scala_annotation.initializer:
        annotations.append(
            'annotations.ScalaInitializer("' +
            strutil.EscapeString(column.scala_annotation.initializer) + '")')
    if column.scala_annotation.java_type_name:
        annotations.append(
            'annotations.JavaTypeName("' +
            strutil.EscapeString(column.scala_annotation.java_type_name) + '")')
    if column.data_annotation.is_lob:
        annotations.append('annotations.Lob()')
    if column.clickhouse_annotation.compression_type:
        compression = _GetCompressionName(
            column.clickhouse_annotation.compression_type)
        level = None
        if column.clickhouse_annotation.HasField('compression_level'):
            level = column.clickhouse_annotation.compression_level
        annotations.append(f'annotations.Compression("{compression}", {level})')
    if column.clickhouse_annotation.delta_compression_width:
        width = column.clickhouse_annotation.delta_compression_width
        annotations.append(f'annotations.DeltaCompression({width})')
    if column.clickhouse_annotation.is_low_cardinality:
        annotations.append('annotations.LowCardinality()')
    if column.clickhouse_annotation.type_name:
        annotations.append(
            'annotations.ClickhouseType("' +
            strutil.EscapeString(column.clickhouse_annotation.type_name) + '")')
    if column.info.deprecated:
        annotations.append('annotations.Deprecated()')
    if column.info.repeated_semantics == Schema_pb2.ColumnInfo.REPEATED_ARRAY:
        annotations.append('annotations.Array()')
    # TODO(cp): add dq_filed and synthgen if desired at some point.

    return annotations


class TypeInfo:
    """Represents the type of a field."""

    def __init__(self,
                 name: str,
                 annotations: Optional[List[str]] = None,
                 imports: Optional[Set[str]] = None,
                 sub_types: Optional[List['TypeInfo']] = None):
        """Creates a type.

        Args:
            name: name of this type - can be a 'template' or a full type name.
            annotations: list of annotations to use for this type.
            imports: set of modules to import for this type definition.
            sub_type: if this is not empty the `name` is a template-like type, and
                      this specifies the arguments of that template.
        """
        self.name = name
        if annotations:
            self.annotations = annotations
        else:
            self.annotations = []
        if imports:
            self.imports = imports
        else:
            self.imports = set()
        if sub_types:
            self.sub_types = sub_types
        else:
            self.sub_types = []

    def copy(self):
        """Returns a copy of this type info."""
        return TypeInfo(self.name, self.annotations.copy(), self.imports.copy(),
                        self.sub_types.copy())

    def add_imports(self, imports: Set[str]):
        """Adds a set of imports to this type, returns self."""
        self.imports.update(imports)
        return self

    def get_imports(self) -> Set[str]:
        """Returns the union of imports needed by this type and subtypes."""
        imports = self.imports.copy()
        for sub_type in self.sub_types:
            imports.update(sub_type.get_imports())
        return imports

    def add_annotations(self, annotations: List[str]):
        """Adds a list of annotations to this type."""
        self.annotations.extend(annotations)
        return self

    def has_annotations(self) -> bool:
        """If this type of its subtypes contain any annotations."""
        if self.annotations:
            return True
        for sub_type in self.sub_types:
            if sub_type.has_annotations():
                return True
        return False

    def apply_template(self, template_name):
        """Wraps the type name in provided 'template'."""
        self.name = f'{template_name}[{self.name}]'
        return self

    def py_code(self):
        """Returns a python code snippet, describing this type."""
        if self.sub_types:
            base_type = f'{self.name}[{", ".join(t.py_code() for t in self.sub_types)}]'
        else:
            base_type = self.name
        if not self.annotations:
            return base_type
        return f'Annotate({base_type}, [{", ".join(self.annotations)}])'


class NestedType:
    """Represents a nested type needed for a field."""

    def __init__(self, name: str):
        self.name = name
        self.fields: List['FieldInfo'] = []

    def get_imports(self) -> Set[str]:
        imports = set()
        imports.add('dataclasses')
        for field in self.fields:
            imports.update(field.get_imports())
        return imports

    def has_annotations(self) -> bool:
        for field in self.fields:
            if field.has_annotations():
                return True
        return False

    def py_code_lines(self, indent, delta_indent):
        """Returns a list of line, corresponding to a python snippet for this type."""
        lines = []
        lines.append(f'{indent}@dataclasses.dataclass')
        lines.append(f'{indent}class {self.name}:')
        for field in self.fields:
            lines.extend(
                field.py_code_lines(indent + delta_indent, delta_indent))
        return lines


class FieldInfo:
    """Data necessary for representing a filed of a dataclass."""

    def __init__(self, name: str, type_info: Optional[TypeInfo] = None):
        self.name = name
        self.type_info = type_info
        self.nested: List[NestedType] = []

    def get_imports(self) -> Set[str]:
        imports = set()
        if self.type_info:
            imports.update(self.type_info.get_imports())
        for nested in self.nested:
            imports.update(nested.get_imports())
        return imports

    def has_annotations(self) -> bool:
        if self.type_info and self.type_info.has_annotations():
            return True
        for nested in self.nested:
            if nested.has_annotations():
                return True
        return False

    def py_code_lines(self, indent, delta_indent):
        """Returns a list of line, corresponding to a python snippet for this field."""
        lines = []
        for nested in self.nested:
            lines.extend(nested.py_code_lines(indent, delta_indent))
        if not self.type_info:
            lines.append(f'{indent}{self.name}')
        else:
            lines.append(f'{indent}{self.name}: {self.type_info.py_code()}')
        return lines


_TYPE_INFO = {
    Schema_pb2.ColumnInfo.TYPE_STRING:
        TypeInfo('str'),
    Schema_pb2.ColumnInfo.TYPE_BYTES:
        TypeInfo('bytes'),
    Schema_pb2.ColumnInfo.TYPE_BOOLEAN:
        TypeInfo('bool'),
    Schema_pb2.ColumnInfo.TYPE_INT_8:
        TypeInfo('int', ['annotations.TypeInt8()']),
    Schema_pb2.ColumnInfo.TYPE_INT_16:
        TypeInfo('int', ['annotations.TypeInt16()']),
    Schema_pb2.ColumnInfo.TYPE_INT_32:
        TypeInfo('int', ['annotations.TypeInt32()']),
    Schema_pb2.ColumnInfo.TYPE_INT_64:
        TypeInfo('int'),
    Schema_pb2.ColumnInfo.TYPE_UINT_8:
        TypeInfo('int', ['annotations.TypeUInt8()']),
    Schema_pb2.ColumnInfo.TYPE_UINT_16:
        TypeInfo('int', ['annotations.TypeUInt16()']),
    Schema_pb2.ColumnInfo.TYPE_UINT_32:
        TypeInfo('int', ['annotations.TypeUInt32()']),
    Schema_pb2.ColumnInfo.TYPE_UINT_64:
        TypeInfo('int', ['annotations.TypeUInt64()']),
    Schema_pb2.ColumnInfo.TYPE_DECIMAL:
        TypeInfo('decimal.Decimal', [], {'decimal'}),
    Schema_pb2.ColumnInfo.TYPE_FLOAT_32:
        TypeInfo('float', ['annotations.TypeFloat32()']),
    Schema_pb2.ColumnInfo.TYPE_FLOAT_64:
        TypeInfo('float'),
    Schema_pb2.ColumnInfo.TYPE_DATE:
        TypeInfo('datetime.date', [], {'datetime'}),
    Schema_pb2.ColumnInfo.TYPE_DATETIME_64:
        TypeInfo('datetime.datetime', [], {'datetime'}),
}


def _ApplyLabel(column: Schema.Column, info: FieldInfo):
    if column.is_repeated():
        info.type_info.add_imports({'typing'}).apply_template('typing.List')
    elif column.is_optional():
        info.type_info.add_imports({'typing'}).apply_template('typing.Optional')


def GetFieldInfo(column: Schema.Column,
                 force_nested_types: bool = False,
                 nested_prefix: str = 'Nested_') -> FieldInfo:
    """Returns the corresponding information for provided column.

    Args:
        column: the column for which to generate the dataclass FieldInfo.
        force_nested_types: when True, a nested subclass is generated always,
            even for known dataclass types.
        nested_prefix: name prefix for nested dataclasses.

    Returns:
        The corresponding `FieldInfo` class for column.
    """
    column.validate()
    info = FieldInfo(column.info.name)
    nested_name = f'{nested_prefix}{column.info.name}'
    sub_nested_name = f'{nested_name}_'
    if column.info.column_type in _TYPE_INFO:
        info.type_info = _TYPE_INFO[column.info.column_type].copy()
        _ApplyLabel(column, info)
    elif column.info.column_type == Schema_pb2.ColumnInfo.TYPE_NESTED:
        if column.info.message_name and not force_nested_types:
            info.type_info = TypeInfo(column.info.message_name)
        else:
            info.type_info = TypeInfo(nested_name)
            nested = NestedType(info.type_info.name)
            nested.fields = [
                GetFieldInfo(sub_column, force_nested_types, sub_nested_name)
                for sub_column in column.fields
            ]
            info.nested.append(nested)
        _ApplyLabel(column, info)
    elif column.info.column_type in (Schema_pb2.ColumnInfo.TYPE_ARRAY,
                                     Schema_pb2.ColumnInfo.TYPE_SET):
        element_info = GetFieldInfo(column.fields[0], force_nested_types,
                                    sub_nested_name)
        if column.info.column_type == Schema_pb2.ColumnInfo.TYPE_ARRAY:
            name = 'typing.List'
        else:
            name = 'typing.Set'
        info.type_info = TypeInfo(name, None, {'typing'},
                                  [element_info.type_info])
        info.nested.extend(element_info.nested)
    elif column.info.column_type == Schema_pb2.ColumnInfo.TYPE_MAP:
        key_info = GetFieldInfo(column.fields[0], force_nested_types,
                                sub_nested_name)
        value_info = GetFieldInfo(column.fields[1], force_nested_types,
                                  sub_nested_name)
        info.type_info = TypeInfo('typing.Dict', None, {'typing'},
                                  [key_info.type_info, value_info.type_info])
        info.nested.extend(key_info.nested)
        info.nested.extend(value_info.nested)
    else:
        raise ValueError(f'Unsupported type `{column.info.column_type}` '
                         f'for field `{column.name()}`')
    info.type_info.add_annotations(_GetColumnAnnotations(column))

    return info


class DataclassInfo:
    """Holding corresponding dataclass information of a schema table."""

    def __init__(self, name: str, fields: List[FieldInfo],
                 annotations: List[str]):
        self.name = name
        self.fields = fields
        self.annotations = annotations
        self.imports = {'dataclasses'}

    def get_imports(self) -> Set[str]:
        imports = self.imports.copy()
        for field in self.fields:
            imports.update(field.get_imports())
        return imports

    def has_annotations(self) -> bool:
        if self.annotations:
            return True
        for field in self.fields:
            if field.has_annotations():
                return True
        return False

    def py_code_lines(self, indent='    '):
        """Returns a python code snippet, corresponding to this dataclass."""
        lines = self.annotations.copy()
        lines.append('@dataclasses.dataclass')
        lines.append(f'class {self.name}:')
        for field in self.fields:
            lines.extend(field.py_code_lines(indent, indent))
        return lines


def GetDataclassInfo(table: Schema.Table,
                     force_nested_types: bool = False) -> DataclassInfo:
    """Generates a schema to python conversion class for a schema table.

    If `force_nested_type` is on, it generates nested classes for all nested
    fields, even ones marked with a known name."""

    return DataclassInfo(
        table.name(),
        [GetFieldInfo(column, force_nested_types) for column in table.columns],
        _GetTableAnnotation(table))


class ModuleInfo:
    """Holds corresponding information for a set of a schema table,
    generates python modules."""

    def __init__(self, name: str, dclasses: List[DataclassInfo]):
        self.name = name
        self.dataclasses = dclasses

    def add_dataclass(self, dclass: DataclassInfo):
        self.dataclasses.append(dclass)

    def has_annotations(self):
        for dclass in self.dataclasses:
            if dclass.has_annotations():
                return True
        return False

    def get_imports(self):
        imports = set()
        for dclass in self.dataclasses:
            imports.update(dclass.get_imports())
        return imports

    def py_module_lines(self,
                        java_package: Optional[str] = None,
                        indent: str = '    '):
        """Generates lines of code, including imports and optional decorations."""
        lines = []
        lines.append('"""Generated by schema2python utility."""')
        imports = [f'import {imp}' for imp in self.get_imports()]
        imports.sort()
        if self.has_annotations():
            imports.append('from dataschema import annotations')
            imports.append('from dataschema.entity import Annotate')
        lines.extend(imports)
        lines.append('')
        if java_package:
            lines.append(f'JAVA_PACKAGE = "{java_package}"')
        else:
            lines.append(f'JAVA_PACKAGE = "{self.name}"')
        for dclass in self.dataclasses:
            lines.append('')
            lines.extend(dclass.py_code_lines(indent))
        return lines


def GetModuleInfo(name: str,
                  tables: List[Schema.Table],
                  force_nested_types: bool = False) -> ModuleInfo:
    """Generates a module information for a set of tables.

    Args:
        name: name of the module.
        table: list of schema tables.
        force_nested_types: forces the generation of nested classes.
    """
    return ModuleInfo(
        name, [GetDataclassInfo(table, force_nested_types) for table in tables])


def ConvertTable(table: Schema.Table,
                 java_package: Optional[str] = None,
                 force_nested_types: bool = False) -> str:
    """Converts the table schema to python code snippetdclass."""
    return '\n'.join(
        ModuleInfo(table.name(), [GetDataclassInfo(table, force_nested_types)
                                 ]).py_module_lines(java_package))
