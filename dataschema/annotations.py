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
"""Includes schema-specific annotations for classes and fields."""

import json
from dataschema import Schema, Schema_pb2, synthgen
from typing import List, Optional, Union

_SCHEMA_ANNOTATIONS = '__schema_annotations__'

CLICKHOUSE_SUPPORTED_NESTED_TYPES = { 'Tuple' }


def _class_annotate(cls, annotation):
    """Annotates a class."""

    def wrap(cls):
        schema_annotations = []
        if hasattr(cls, _SCHEMA_ANNOTATIONS):
            schema_annotations.extend(getattr(cls, _SCHEMA_ANNOTATIONS))
        if isinstance(annotation, list):
            schema_annotations.extend(annotation)
        else:
            schema_annotations.append(annotation)
        setattr(cls, _SCHEMA_ANNOTATIONS, schema_annotations)
        return cls

    if cls is None:
        return wrap
    return wrap(cls)


class Annotation:

    def annotate_column(self, column: Schema.Column):
        raise NotImplementedError()

    def annotate_table(self, table: Schema.Table):
        raise NotImplementedError()


class ColumnAnnotation(Annotation):
    """Annotations that can be applied only for columns/fields."""

    def annotate_table(self, table: Schema.Table):
        pass


class TableAnnotation(Annotation):
    """Annotations that can be applied only for tables/classes."""

    def annotate_column(self, column: Schema.Column):
        pass


class Decimal(ColumnAnnotation):
    """Precision / scale annotation for decimal columns."""

    def __init__(self, precision: int, scale: int):
        if precision < 0 or precision > Schema.MAX_DECIMAL_PRECISION:
            raise ValueError(f'Invalid decimal precision: {precision}')
        if scale < 0 or scale > precision:
            raise ValueError(f'Invalid decimal scale: {scale}')
        self.precision = precision
        self.scale = scale

    def annotate_column(self, column: Schema.Column):
        if column.info.column_type != Schema_pb2.ColumnInfo.TYPE_DECIMAL:
            raise ValueError(
                'Decimal annotation supported only for Decimal types.')
        column.info.decimal_info.CopyFrom(
            Schema_pb2.ColumnInfo.DecimalInfo(precision=self.precision,
                                              scale=self.scale))


class Timestamp(ColumnAnnotation):
    """Precision and timezone annotation for timestamp columns."""

    def __init__(self, precision: int, timezone: str):
        self.precision = precision
        self.timezone = timezone

    def annotate_column(self, column: Schema.Column):
        if column.info.column_type != Schema_pb2.ColumnInfo.TYPE_DATETIME_64:
            raise ValueError(
                'Timestamp annotation supported only for Datetime types.')
        column.info.timestamp_info.CopyFrom(
            Schema_pb2.ColumnInfo.TimestampInfo(precision=self.precision,
                                                timezone=self.timezone))


def _col_type_name(val):
    return Schema_pb2.ColumnInfo.ColumnType.Name(val)


class _TypeRedefine(ColumnAnnotation):
    """Base for exact type specification for numerics."""

    def __init__(self, column_type: int, expected_column_type: int):
        self.column_type = column_type
        self.expected_column_type = expected_column_type

    def annotate_column(self, column: Schema.Column):
        if column.info.column_type != self.expected_column_type:
            raise ValueError(
                f'{_col_type_name(self.column_type)} annotation supported '
                f'only for {_col_type_name(self.expected_column_type)} types. '
                f'Got {_col_type_name(column.info.column_type)}')
        column.info.column_type = self.column_type


class TypeInt8(_TypeRedefine):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_INT_8,
                         Schema_pb2.ColumnInfo.TYPE_INT_64)


class TypeInt16(_TypeRedefine):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_INT_16,
                         Schema_pb2.ColumnInfo.TYPE_INT_64)


class TypeInt32(_TypeRedefine):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_INT_32,
                         Schema_pb2.ColumnInfo.TYPE_INT_64)


class TypeInt64(_TypeRedefine):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_INT_64,
                         Schema_pb2.ColumnInfo.TYPE_INT_64)


class TypeUInt8(_TypeRedefine):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_UINT_8,
                         Schema_pb2.ColumnInfo.TYPE_INT_64)


class TypeUInt16(_TypeRedefine):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_UINT_16,
                         Schema_pb2.ColumnInfo.TYPE_INT_64)


class TypeUInt32(_TypeRedefine):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_UINT_32,
                         Schema_pb2.ColumnInfo.TYPE_INT_64)


class TypeUInt64(_TypeRedefine):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_UINT_64,
                         Schema_pb2.ColumnInfo.TYPE_INT_64)


class TypeFloat32(_TypeRedefine):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_FLOAT_32,
                         Schema_pb2.ColumnInfo.TYPE_FLOAT_64)


class TypeFloat64(_TypeRedefine):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_FLOAT_64,
                         Schema_pb2.ColumnInfo.TYPE_FLOAT_64)


class Comment(Annotation):
    """Attaches a comment to a class or a field."""

    def __init__(self, comment_str: str):
        self.comment_str = comment_str

    def annotate_column(self, column: Schema.Column):
        column.has_data_annotation = True
        column.data_annotation.comment = self.comment_str

    def annotate_table(self, table: Schema.Table):
        table.has_data_annotation = True
        table.data_annotation.comment = self.comment_str


def comment(cls=None, value=None):
    return _class_annotate(cls, annotation=Comment(value))


class EntityClass(TableAnnotation):

    def annotate_table(self, table: Schema.Table):
        table.has_scala_annotation = True
        table.scala_annotation.is_entity = True


def entity_class(cls=None):
    return _class_annotate(cls, annotation=EntityClass())


class ClickhouseEngine(TableAnnotation):
    """Annotates a table with the clickhouse engine to use on creation."""

    def __init__(self, engine: str):
        if engine in ('MERGE_TREE', 'MergeTree'):
            self.engine = Schema_pb2.TableClickhouseAnnotation.ENGINE_MERGE_TREE
        elif engine in ('LOG', 'Log'):
            self.engine = Schema_pb2.TableClickhouseAnnotation.ENGINE_LOG
        elif engine in ('TINY_LOG', 'TinyLog'):
            self.engine = Schema_pb2.TableClickhouseAnnotation.ENGINE_TINY_LOG
        elif engine in ('REPLICATED_MERGE_TREE', 'ReplicatedMergeTree'):
            self.engine = (Schema_pb2.TableClickhouseAnnotation.
                           ENGINE_REPLICATED_MERGE_TREE)
        else:
            raise ValueError(f'Invalid ClickhouseEngine argument: {engine}')

    def annotate_table(self, table: Schema.Table):
        table.has_clickhouse_annotation = True
        table.clickhouse_annotation.engine = self.engine


def clickhouse_engine(cls=None, engine=None):
    return _class_annotate(cls, annotation=ClickhouseEngine(engine))


def _make_list(arg):
    return arg if isinstance(arg, list) else [arg]


class ClickhouseOrderBy(TableAnnotation):

    def __init__(self, order_by_arg: Union[str, List[str]]):
        self.order_by = _make_list(order_by_arg)

    def annotate_table(self, table: Schema.Table):
        table.has_clickhouse_annotation = True
        table.clickhouse_annotation.order_by_fields.extend(self.order_by)


def order_by(cls=None, values=None):
    return _class_annotate(cls, annotation=ClickhouseOrderBy(values))


class ClickhousePartitionBy(TableAnnotation):

    def __init__(self, partition_by_arg: Union[str, List[str]]):
        self.partition_by = _make_list(partition_by_arg)

    def annotate_table(self, table: Schema.Table):
        table.has_clickhouse_annotation = True
        table.clickhouse_annotation.partition_by_sql_expression.extend(
            self.partition_by)


def partition_by(cls=None, values=None):
    return _class_annotate(cls, annotation=ClickhousePartitionBy(values))


class ClickhouseSampleBy(TableAnnotation):

    def __init__(self, sample_by_arg: Union[str, List[str]]):
        self.sample_by = _make_list(sample_by_arg)

    def annotate_table(self, table: Schema.Table):
        table.has_clickhouse_annotation = True
        table.clickhouse_annotation.sample_by_sql_expression.extend(
            self.sample_by)


def sample_by(cls=None, values=None):
    return _class_annotate(cls, annotation=ClickhouseSampleBy(values))


class ClickhouseIndexGranularity(TableAnnotation):

    def __init__(self, value: int):
        self.value = value

    def annotate_table(self, table: Schema.Table):
        table.has_clickhouse_annotation = True
        table.clickhouse_annotation.index_granularity = self.value


def index_granularity(cls=None, value=None):
    return _class_annotate(cls, annotation=ClickhouseIndexGranularity(value))


class DefaultCompression(TableAnnotation):
    """Sets the default compression for all fields at table level."""

    def __init__(self, value: str):
        self.compression = None
        if value == 'LZ4':
            self.compression = Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_LZ4
        elif value == 'ZSTD':
            self.compression = Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_ZSTD
        elif value == 'LZ4HC':
            self.compression = Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_LZ4HC
        else:
            raise ValueError(f'Invalid compression argument: {value}')

    def annotate_table(self, table: Schema.Table):
        table.has_clickhouse_annotation = True
        table.clickhouse_annotation.default_compression = self.compression


def default_compression(cls=None, value=None):
    return _class_annotate(cls, annotation=DefaultCompression(value))


class Id(ColumnAnnotation):
    """Column is an type ID column."""

    def annotate_column(self, column: Schema.Column):
        column.has_data_annotation = True
        column.data_annotation.is_id = True


class DqField(ColumnAnnotation):
    """Dq field annotation for scala."""

    def __init__(self,
                 is_nullable: bool = None,
                 is_ignored: bool = None,
                 format_str: str = None,
                 enum_values: List[str] = None,
                 regexp: str = None):
        self.is_nullable = is_nullable
        self.is_ignored = is_ignored
        self.format_str = format_str
        self.enum_values = enum_values
        self.regexp = regexp

    def annotate_column(self, column: Schema.Column):
        column.has_data_annotation = True
        if self.is_nullable is not None:
            column.data_annotation.dq_field.is_nullable = self.is_nullable
        if self.is_ignored is not None:
            column.data_annotation.dq_field.is_ignored = self.is_ignored
        if self.format_str is not None:
            column.data_annotation.dq_field.format = self.format_str
        if self.enum_values is not None:
            column.data_annotation.dq_field.enum_value.extend(self.enum_values)
        if self.regexp is not None:
            column.data_annotation.dq_field.regexp = self.regexp


class JoinReference(ColumnAnnotation):

    def __init__(self, value: str):
        self.value = value

    def annotate_column(self, column: Schema.Column):
        column.has_data_annotation = True
        column.data_annotation.join_reference = self.value


class ScalaOriginalName(ColumnAnnotation):

    def __init__(self, value: str):
        self.value = value

    def annotate_column(self, column: Schema.Column):
        column.has_scala_annotation = True
        column.scala_annotation.original_name = self.value


class ClickhouseOriginalName(ColumnAnnotation):

    def __init__(self, value: str):
        self.value = value

    def annotate_column(self, column: Schema.Column):
        column.has_clickhouse_annotation = True
        column.clickhouse_annotation.original_name = self.value


class Width(ColumnAnnotation):

    def __init__(self, value: int):
        self.value = value

    def annotate_column(self, column: Schema.Column):
        column.has_data_annotation = True
        column.data_annotation.width = self.value


class ScalaInitializer(ColumnAnnotation):

    def __init__(self, snippet: int):
        self.snippet = snippet

    def annotate_column(self, column: Schema.Column):
        column.has_scala_annotation = True
        column.scala_annotation.initializer = self.snippet


class JavaTypeName(ColumnAnnotation):

    def __init__(self, type_name: int):
        self.type_name = type_name

    def annotate_column(self, column: Schema.Column):
        column.has_scala_annotation = True
        column.scala_annotation.java_type_name = self.type_name


class Lob(ColumnAnnotation):
    """Large object annotation for java."""

    def annotate_column(self, column: Schema.Column):
        column.has_data_annotation = True
        column.data_annotation.is_lob = True


class Compression(ColumnAnnotation):
    """How to compress column in clickhouse."""

    def __init__(self, compression: str, level=None):
        if compression == 'LZ4':
            if level is not None:
                raise ValueError(
                    'Compression level cannot be specified for LZ4')
            self.compression = Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_LZ4
        elif compression == 'ZSTD':
            self.compression = Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_ZSTD
        elif compression == 'LZ4HC':
            self.compression = Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_LZ4HC
        elif compression == 'UNCOMPRESSED':
            self.compression = Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_UNCOMPRESSED
        else:
            raise ValueError(f'Invalid compression argument: {compression}')
        self.level = level

    def annotate_column(self, column: Schema.Column):
        column.has_clickhouse_annotation = True
        column.clickhouse_annotation.compression_type = self.compression
        if self.level is not None:
            column.clickhouse_annotation.compression_level = self.level


_DEFAULT_DELTA = {
    Schema_pb2.ColumnInfo.TYPE_INT_8: 1,
    Schema_pb2.ColumnInfo.TYPE_INT_16: 2,
    Schema_pb2.ColumnInfo.TYPE_INT_32: 4,
    Schema_pb2.ColumnInfo.TYPE_INT_64: 8,
    Schema_pb2.ColumnInfo.TYPE_UINT_8: 1,
    Schema_pb2.ColumnInfo.TYPE_UINT_16: 2,
    Schema_pb2.ColumnInfo.TYPE_UINT_32: 4,
    Schema_pb2.ColumnInfo.TYPE_UINT_64: 8,
}


class DeltaCompression(ColumnAnnotation):
    """Compress integers with delta compression."""

    def __init__(self, delta: Optional[int] = None):
        self.delta = delta

    def annotate_column(self, column: Schema.Column):
        if self.delta is None:
            delta = _DEFAULT_DELTA.get(column.info.column_type, 1)
        else:
            delta = self.delta
        column.has_clickhouse_annotation = True
        column.clickhouse_annotation.delta_compression_width = delta


class LowCardinality(ColumnAnnotation):
    """Shows that a string type field can contain a short list of values."""

    def annotate_column(self, column: Schema.Column):
        column.has_clickhouse_annotation = True
        column.clickhouse_annotation.is_low_cardinality = True


class ClickhouseType(ColumnAnnotation):
    """Override the default clickhouse type for this column w/ argument."""

    def __init__(self, type_name: int):
        self.type_name = type_name

    def annotate_column(self, column: Schema.Column):
        column.has_clickhouse_annotation = True
        column.clickhouse_annotation.type_name = self.type_name


class ClickhouseNestedType(ColumnAnnotation):
    """Override the default ClickHouse type for a nested column w/ argument."""

    def __init__(self, nested_type_name: int):
        if nested_type_name not in CLICKHOUSE_SUPPORTED_NESTED_TYPES:
            supported_types = ', '.join(CLICKHOUSE_SUPPORTED_NESTED_TYPES)
            raise ValueError(
                f'`{nested_type_name}` is not a supported ClickHouse nested '
                f'type. Supported types: {supported_types}.'
            )
        self.nested_type_name = nested_type_name

    def annotate_column(self, column: Schema.Column):
        if column.info.column_type != Schema_pb2.ColumnInfo.TYPE_NESTED:
            raise ValueError(
                'Nested type override is only supported for nested types.')
        column.has_clickhouse_annotation = True
        column.clickhouse_annotation.nested_type_name = self.nested_type_name


class Deprecated(ColumnAnnotation):
    """Shows the deprecation of a field."""

    def annotate_column(self, column: Schema.Column):
        column.info.deprecated = True


class Array(ColumnAnnotation):
    """Marks the semantics of an list as a specific Array."""

    def annotate_column(self, column: Schema.Column):
        column.info.repeated_semantics = Schema_pb2.ColumnInfo.REPEATED_ARRAY


class SynthGen(ColumnAnnotation):
    """Annotates a column with its synthetic data generator."""

    def __init__(self, generator: synthgen.GeneratorSpec):
        self.generator = generator

    def annotate_column(self, column: Schema.Column):
        column.synthgen = self.generator
        if isinstance(self.generator, synthgen.Generator):
            column.info.synthgen_spec = json.dumps(self.generator.save())
        else:
            column.info.synthgen_spec = json.dumps(self.generator)
