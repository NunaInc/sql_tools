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
"""Converts python dataclass schema to Scala case classes."""

import dataclasses
import os
from dataschema import Schema, Schema_pb2, entity, python2schema, strutil
from types import ModuleType
from typing import Dict, List, Optional, Tuple, Union


class ScalaImports:
    """Accumulates a set of imports for a Scala file."""

    def __init__(self):
        self.imports = set()

    def add_import(self, import_name: str):
        self.imports.add(import_name)

    def add_imports(self, imports: 'ScalaImports'):
        self.imports = self.imports.union(imports.imports)

    def to_scala(self, java_package):
        """Generates a set of import lines for a Scala file from our list."""
        package_map = {}
        for i in self.imports:
            comp = i.split('.')
            package = '.'.join(comp[:-1])
            if package == java_package:
                continue  # skip imports from same package
            if package not in package_map:
                package_map[package] = []
            package_map[package].append(comp[-1])
        lines = []
        for k, v in package_map.items():
            if len(v) == 1:
                lines.append(f'import {k}.{v[0]}')
            else:
                v.sort()
                lines.append(f'import {k}.{{{", ".join(v)}}}')
        lines.sort()
        return '\n'.join(lines)


class ScalaAnnotations:
    """Accumulates a set of class or field annotations, and necessary imports."""

    def __init__(self):
        self.annotations = []
        self.imports = ScalaImports()

    def add_annotation(self, annotation: str, import_name: str):
        self.annotations.append(annotation)
        if import_name is not None:
            self.imports.add_import(import_name)


class ScalaTypeInfo:
    """Represents a type for scala export: name, imports and templates."""

    def __init__(self,
                 type_name: Union[str, Tuple['ScalaTypeInfo']],
                 import_name: Optional[str] = None,
                 template_type: Optional[List[str]] = None):
        self.type_name = type_name
        self.import_name = import_name
        self.template_type = template_type
        if self.is_map():
            if (not isinstance(type_name, Tuple) or len(type_name) != 2 or
                    not isinstance(type_name[0], ScalaTypeInfo) or
                    not isinstance(type_name[1], ScalaTypeInfo)):
                raise ValueError('Invalid Map type initialized')
        elif not isinstance(type_name, str):
            raise ValueError('Invalid non-Map type initialized')

    def is_map(self):
        return (self.template_type is not None and
                len(self.template_type) == 1 and self.template_type[0] == 'Map')

    def __str__(self) -> str:
        return f'{self.type_name} / {self.import_name} / {self.template_type}'

    def add_template_type(self, template_type: str) -> 'ScalaTypeInfo':
        if template_type is None:
            return self
        if self.template_type is None:
            self.template_type = [template_type]
        else:
            self.template_type.append(template_type)
        return self

    def scala_type_str(self) -> str:
        s = ''
        if self.template_type is not None:
            for t in reversed(self.template_type):
                s += t + '['
        if self.is_map():
            s += (self.type_name[0].scala_type_str() + ', ' +
                  self.type_name[1].scala_type_str())
        else:
            s += self.type_name
        if self.template_type is not None:
            s += ']' * len(self.template_type)
        return s

    def clone(self) -> 'ScalaTypeInfo':
        return ScalaTypeInfo(self.type_name, self.import_name,
                             self.template_type)


class TypeExports:
    """Keeps track of types exported by a set of module files."""

    def __init__(self):
        # Maps from type name to ScalaTypeInfo.
        self._type_maps = {}

    def add_export(self, type_name: str, class_name: str, java_package: str):
        self._type_maps[type_name] = ScalaTypeInfo(class_name,
                                                   import_name=Schema.full_name(
                                                       java_package,
                                                       class_name))

    def find_import(self, type_name: str) -> ScalaTypeInfo:
        if type_name not in self._type_maps:
            raise KeyError(f'Cannot find exported type name `{type_name}`')
        return self._type_maps[type_name]

    def find_import_unqualified(self, class_name: str) -> str:
        for type_info in self._type_maps.values():
            if type_info.type_name == class_name:
                return type_info.import_name
        raise KeyError(f'Cannot find unqualified class name `{class_name}`')


TYPE_MAPPING = {
    Schema_pb2.ColumnInfo.TYPE_STRING:
        ScalaTypeInfo('String'),
    Schema_pb2.ColumnInfo.TYPE_BOOLEAN:
        ScalaTypeInfo('Boolean'),
    Schema_pb2.ColumnInfo.TYPE_INT_8:
        ScalaTypeInfo('Byte'),
    Schema_pb2.ColumnInfo.TYPE_INT_16:
        ScalaTypeInfo('Int'),
    Schema_pb2.ColumnInfo.TYPE_INT_32:
        ScalaTypeInfo('Int'),
    Schema_pb2.ColumnInfo.TYPE_INT_64:
        ScalaTypeInfo('Long'),
    Schema_pb2.ColumnInfo.TYPE_UINT_8:
        ScalaTypeInfo('Int'),
    Schema_pb2.ColumnInfo.TYPE_UINT_16:
        ScalaTypeInfo('Int'),
    Schema_pb2.ColumnInfo.TYPE_UINT_32:
        ScalaTypeInfo('Long'),
    Schema_pb2.ColumnInfo.TYPE_UINT_64:
        ScalaTypeInfo('BigInt', 'scala.math.BigInt'),
    Schema_pb2.ColumnInfo.TYPE_FLOAT_32:
        ScalaTypeInfo('Float'),
    Schema_pb2.ColumnInfo.TYPE_FLOAT_64:
        ScalaTypeInfo('Double'),
    Schema_pb2.ColumnInfo.TYPE_DATE:
        ScalaTypeInfo('Date', 'java.sql.Date'),
    Schema_pb2.ColumnInfo.TYPE_DATETIME_64:
        ScalaTypeInfo('Timestamp', 'java.sql.Timestamp'),
    Schema_pb2.ColumnInfo.TYPE_BYTES:
        ScalaTypeInfo('ByteBuffer', 'java.nio.ByteBuffer'),
    Schema_pb2.ColumnInfo.TYPE_DECIMAL:
        ScalaTypeInfo('Decimal', 'org.apache.spark.sql.types.Decimal'),
}


@dataclasses.dataclass
class ScalaAnnotationClasses:
    """Scala class names to use for various annotations in the schema.

    Each member must contain a full class name.
    Specifying something like `com.mypackage.ClassName` would
    result in an import on `com.mypackage.ClassName` in the scala code,
    and an annotation `@ClassName([<argument>])` for the class member.

    This contains only classes for annotations which do not have
    an obvious Scala or Spark correspondent.

    Attributes:
       comment: class to use for comments.
       dq_field: class to use for data quality annotations.
       join_reference: class to use for annotating join references.
       width: class to use for annotating field width.
    """
    comment: Optional[str] = None
    dq_field: Optional[str] = None
    join_reference: Optional[str] = None
    width: Optional[str] = None


def _BoolStr(value: bool):
    return 'true' if value else 'false'


class TableConverter:
    """Converts a schema table to a Scala case class."""

    def __init__(self,
                 table: Schema.Table,
                 scala_annotations: Optional[ScalaAnnotationClasses] = None):
        self.table = table
        self.nested = [
            TableConverter(nested_table) for nested_table in table.nested
        ]
        if scala_annotations is None:
            scala_annotations = ScalaAnnotationClasses()
        self.scala_annotations = scala_annotations

    def _has_annotation(self, name):
        return getattr(self.scala_annotations, name, None) is not None

    def _annotation(self, name):
        return getattr(self.scala_annotations, name, None).split('.')[-1]

    def _annotation_class(self, name):
        return getattr(self.scala_annotations, name, None)

    def _get_class_annotations(self) -> ScalaAnnotations:
        annotations = ScalaAnnotations()
        if self.table.scala_annotation.is_entity:
            annotations.add_annotation('@Entity', 'javax.persistence.Entity')
        if (self.table.data_annotation.comment and
                self._has_annotation('comment')):
            annot = self._annotation('comment')
            comment = strutil.EscapeString(self.table.data_annotation.comment)
            annotations.add_annotation(f'@{annot}("{comment}")',
                                       self._annotation_class('comment'))
        return annotations

    def _get_dqfield_annotation(
            self, dq_field: Schema_pb2.ColumnDataAnnotation.DqField) -> str:
        args = []
        if dq_field.HasField('is_nullable'):
            args.append(f'nullable = {_BoolStr(dq_field.is_nullable)}')
        if dq_field.HasField('is_ignored'):
            args.append(f'ignore = {_BoolStr(dq_field.is_ignored)}')
        if dq_field.HasField('format'):
            value = strutil.EscapeString(dq_field.format)
            args.append(f'format = "{value}"')
        if dq_field.enum_value:
            value = ', '.join(f'"{strutil.EscapeString(value)}"'
                              for value in dq_field.enum_value)
            args.append(f'enumValues = Array({value})')
        if dq_field.HasField('regexp'):
            value = strutil.EscapeString(dq_field.regexp)
            args.append(f'regexp = "{value}"')
        annot = self._annotation('dq_field')
        value = ', '.join(args)
        return f'@{annot}({value})'

    def _get_decimal_type(self, column: Schema.Column,
                          original_type: ScalaTypeInfo) -> ScalaTypeInfo:
        info = column.decimal_info()
        if not info:
            return original_type
        if info.scale == 0:
            return ScalaTypeInfo('BigInt', 'scala.math.BigInt')
        return ScalaTypeInfo('Decimal', 'org.apache.spark.sql.types.Decimal')

    def _get_column_annotations(self, column: Schema.Column,
                                exports: TypeExports) -> ScalaAnnotations:
        annotations = ScalaAnnotations()
        if column.info.deprecated:
            annotations.add_annotation('@Deprecated', None)
        if column.data_annotation.is_id:
            annotations.add_annotation('@Id', 'javax.persistence.Id')
        if column.data_annotation.comment and self._has_annotation('comment'):
            annot = self._annotation('comment')
            comment = strutil.EscapeString(column.data_annotation.comment)
            annotations.add_annotation(f'@{annot}("{comment}")',
                                       self._annotation_class('comment'))
        if (column.data_annotation.HasField('dq_field') and
                self._has_annotation('dq_field')):
            annotations.add_annotation(
                self._get_dqfield_annotation(column.data_annotation.dq_field),
                self._annotation_class('dq_field'))
        if (column.data_annotation.join_reference and
                self._has_annotation('join_reference')):
            annot = self._annotation('join_reference')
            ref = column.data_annotation.join_reference
            annotations.add_annotation(f'@{annot}(classOf[{ref}])',
                                       self._annotation_class('join_reference'))
            annotations.imports.add_import(
                exports.find_import_unqualified(
                    column.data_annotation.join_reference))
        if (column.data_annotation.width and self._has_annotation('width')):
            annot = self._annotation('width')
            annotations.add_annotation(
                f'@{annot}({column.data_annotation.width})',
                self._annotation_class('width'))
        if column.data_annotation.is_lob:
            annotations.add_annotation('@Lob', 'javax.persistence.Lob')
        return annotations

    def _get_column_struct_template(self,
                                    column: Schema.Column) -> Optional[str]:
        if column.info.label == Schema_pb2.ColumnInfo.LABEL_OPTIONAL:
            return 'Option'
        if column.info.label == Schema_pb2.ColumnInfo.LABEL_REQUIRED:
            return None
        if column.info.label != Schema_pb2.ColumnInfo.LABEL_REPEATED:
            raise ValueError(
                f'Label not specified for column `{column.name()}`')
        if column.info.repeated_semantics == Schema_pb2.ColumnInfo.REPEATED_ARRAY:
            return 'Array'
        if column.info.repeated_semantics == Schema_pb2.ColumnInfo.REPEATED_SET:
            return 'Set'
        return 'Seq'

    def _get_map_type_info(self, column: Schema.Column,
                           exports: TypeExports) -> ScalaTypeInfo:
        key_type = self._get_column_type(column.fields[0], exports)
        if key_type.template_type is not None:
            key_type.template_type = [
                t for t in key_type.template_type if t != 'Option'
            ]
        val_type = self._get_column_type(column.fields[1], exports)
        return ScalaTypeInfo((key_type, val_type), template_type=['Map'])

    def _get_column_type(self, column: Schema.Column,
                         exports: TypeExports) -> ScalaTypeInfo:
        if column.info.column_type == Schema_pb2.ColumnInfo.TYPE_NESTED:
            return exports.find_import(
                column.info.message_name).clone().add_template_type(
                    self._get_column_struct_template(column))
        elif column.info.column_type == Schema_pb2.ColumnInfo.TYPE_ARRAY:
            return self._get_column_type(
                column.fields[0], exports).add_template_type(
                    self._get_column_struct_template(column))
        elif column.info.column_type == Schema_pb2.ColumnInfo.TYPE_SET:
            return self._get_column_type(column.fields[0],
                                         exports).add_template_type('Set')
        elif column.info.column_type == Schema_pb2.ColumnInfo.TYPE_MAP:
            return self._get_map_type_info(column, exports)
        else:
            mapped_type = TYPE_MAPPING[column.info.column_type].clone()
            if column.info.column_type == Schema_pb2.ColumnInfo.TYPE_DECIMAL:
                mapped_type = self._get_decimal_type(column, mapped_type)
            if column.scala_annotation.java_type_name:
                mapped_type = ScalaTypeInfo(
                    column.scala_annotation.java_type_name)
            return mapped_type.add_template_type(
                self._get_column_struct_template(column))

    def _get_column_name(self, column: Schema.Column) -> str:
        if not column.scala_annotation.original_name:
            return column.name()
        return f'`{column.scala_annotation.original_name}`'

    def to_scala(self, exports: Optional[TypeExports] = None):
        if not exports:
            exports = TypeExports()
        class_annotations = self._get_class_annotations()
        imports = ScalaImports()
        imports.add_imports(class_annotations.imports)
        column_lines = []
        for column in self.table.columns:
            column_annotations = self._get_column_annotations(column, exports)
            imports.add_imports(column_annotations.imports)
            column_type = self._get_column_type(column, exports)
            if column_type.import_name is not None:
                imports.add_import(column_type.import_name)
            prefix = ''
            if column_annotations.annotations:
                prefix = '\n'.join([
                    f'  {annot}' for annot in column_annotations.annotations
                ]) + '\n'
            column_lines.append(f'{prefix}  {self._get_column_name(column)}: ' +
                                column_type.scala_type_str() +
                                column.scala_annotation.initializer)
        nested_classes = []
        nested_sub_imports = []
        for nested in self.nested:
            (nested_content, nested_imports) = nested.to_scala(exports)
            nested_classes.append('\n'.join(
                [f'  {line}' for line in nested_content.split('\n')]))
            nested_sub_imports.append(f'import {nested.table.info.java_package}'
                                      f'.{nested.table.name()}')
            imports.add_imports(nested_imports)
        nested_classes_content = ''
        nested_sub_imports_content = ''
        if nested_classes:
            value = '\n'.join(nested_classes)
            nested_classes_content = f'\nobject {self.table.name()} {{{value}\n}}'
        if nested_sub_imports:
            nested_sub_imports_content = '\n' + '\n'.join(
                nested_sub_imports) + '\n'
        column_value = ',\n'.join(column_lines)
        return (nested_sub_imports_content + '\n'.join([
            '\n'.join(class_annotations.annotations),
            f'case class {self.table.name()}(\n{column_value}\n)'
        ]) + nested_classes_content, imports)


def ConvertTable(
        table: Schema.Table,
        java_package: Optional[str] = None,
        scala_annotations: Optional[ScalaAnnotationClasses] = None) -> str:
    """Converts a Schema table to a scala snippet."""
    converter = TableConverter(table, scala_annotations)
    (classes, imports) = converter.to_scala()
    if java_package is None:
        java_package = table.info.java_package
    return (f'package {java_package}\n'
            f'{imports.to_scala(java_package)}\n\n{classes}')


class _NestedNames:

    def __init__(self, name: str, nested: List['_NestedNames']):
        self.name = name
        self.nested = nested


class FileConverter:
    """Converts a python dataclass file to corresponding Scala case class file."""

    def __init__(self,
                 scala_annotations: Optional[ScalaAnnotationClasses] = None):
        self.name = None
        self.basename = None
        self.package = None
        self.java_package = None
        self.nested_names = []
        self.scala_annotations = scala_annotations

    def from_module(self, py_module: ModuleType) -> 'FileConverter':
        self.name = py_module.__name__
        self.basename = strutil.StripSuffix(
            os.path.basename(py_module.__file__), '.py')
        self.package = py_module.__name__
        self.java_package = entity.GetJavaPackage(py_module)
        self.converters = [
            TableConverter(python2schema.ConvertDataclass(datacls),
                           self.scala_annotations)
            for datacls in py_module.__dict__.values()
            if dataclasses.is_dataclass(datacls)
        ]
        self.nested_names = [
            self._build_dataclass_nested_names(datacls)
            for datacls in py_module.__dict__.values()
            if dataclasses.is_dataclass(datacls)
        ]
        return self

    def _build_dataclass_nested_names(self, datacls: type) -> _NestedNames:
        nested_name = _NestedNames(datacls.__name__, [])
        for key in datacls.__dict__:
            if not dataclasses.is_dataclass(datacls.__dict__[key]):
                continue
            nested_name.nested.append(
                self._build_dataclass_nested_names(datacls.__dict__[key]))
        return nested_name

    def _fill_export(self, exports: TypeExports, names: _NestedNames,
                     package: str, java_package: str):
        type_name = Schema.full_name(package, names.name)
        exports.add_export(type_name, names.name, java_package)
        sub_java_package = Schema.full_name(java_package, names.name)
        for nested in names.nested:
            self._fill_export(exports, nested, type_name, sub_java_package)

    def fill_exports(self, exports: TypeExports):
        for names in self.nested_names:
            self._fill_export(exports, names, self.package, self.java_package)

    def to_scala(self, exports: TypeExports) -> str:
        imports = ScalaImports()
        classes = []
        for conv in self.converters:
            (crt_class, crt_imports) = conv.to_scala(exports)
            classes.append(crt_class)
            imports.add_imports(crt_imports)
        sclasses = '\n\n'.join(classes)
        return (f'package {self.java_package}\n'
                f'{imports.to_scala(self.java_package)}\n\n{sclasses}')

    def get_path(self, dir_map) -> str:
        end_path = os.path.join('/'.join(self.java_package.split('.')),
                                f'{self.basename}.scala')
        for k, v in dir_map.items():
            if self.name.startswith(k):
                return os.path.join(v, end_path)
        return end_path


class SchemaConverter:
    """Converts a list of dataclass files to Scala case class files."""

    def __init__(self,
                 scala_annotations: Optional[ScalaAnnotationClasses] = None):
        self.scala_annotations = scala_annotations
        self.file_converters = []
        self.exports = TypeExports()

    def add_modules(self,
                    py_modules: List[ModuleType],
                    export_only: Optional[bool] = False):
        for pym in py_modules:
            try:
                fc = FileConverter(self.scala_annotations).from_module(pym)
                if not export_only:
                    self.file_converters.append(fc)
                fc.fill_exports(self.exports)
            except ValueError as e:
                raise ValueError(
                    f'Processing input pyton module: {pym.__name__}'
                    f' / {pym.__file__}') from e

    def to_scala_files(self, dir_map: Dict[str, str]) -> Dict[str, str]:
        files = {}
        for fc in self.file_converters:
            contents = fc.to_scala(self.exports)
            path = fc.get_path(dir_map)
            full_contents = f"""
////////////////////////////////////////////////////////////////////////////////
//
// {path}
// Generated from: {fc.name} - DO NOT EDIT
//
{contents}
"""
            files[path] = full_contents
        return files
