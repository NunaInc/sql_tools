# pylint: disable=invalid-name
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
"""Helper class for schema representation in Python."""
from dataschema import Schema_pb2, strutil, synthgen
from enum import Enum
from typing import Dict, List, Optional


def full_name(package: str, name: str):
    if not package:
        return name
    return f'{package}.{name}'


def java_package_from_python(pypackage: str):
    jpackage = strutil.StripPrefix(pypackage, 'pyschema.')
    return f'com.nuna.schema.{jpackage}'


MAX_DECIMAL_PRECISION = 76
_DELTA_COMPRESSION_MAX_DELTA = {
    Schema_pb2.ColumnInfo.TYPE_INT_8: 1,
    Schema_pb2.ColumnInfo.TYPE_INT_16: 2,
    Schema_pb2.ColumnInfo.TYPE_INT_32: 4,
    Schema_pb2.ColumnInfo.TYPE_INT_64: 8,
    Schema_pb2.ColumnInfo.TYPE_UINT_8: 1,
    Schema_pb2.ColumnInfo.TYPE_UINT_16: 2,
    Schema_pb2.ColumnInfo.TYPE_UINT_32: 4,
    Schema_pb2.ColumnInfo.TYPE_UINT_64: 8,
    Schema_pb2.ColumnInfo.TYPE_DATE: 4,
    Schema_pb2.ColumnInfo.TYPE_DATETIME_64: 8,
}


class SourceType(Enum):
    NONE = 0
    PROTO = 1
    DATACLASS = 2
    ARROW = 3
    ARROW_STRUCT = 4
    SQL_CREATE_TABLE = 5
    PYSPARK = 6


class SchemaDiffCode(Enum):
    """Types of differences encountered when comparing schema tables / columns."""
    UNKNOWN = 0
    # Columns or sub-fields from source not found in destination.
    NOT_FOUND_DEST = 1
    # Columns or sub-fields from destination not found in source.
    NOT_FOUND_SRC = 2
    # Differences in column label (optional / required).
    LABEL = 4
    # Differences in column label, but w/o conversion problems.
    # These are:
    #   * required source -> optional or repeated destination
    #   * any source -> repeated destination
    LABEL_CONVERTIBLE = 5
    # Differences in Decimal precision or scale.
    DECIMAL = 6
    # Differences in Decimal precision or scale
    #   - with more precision in destination.
    DECIMAL_CONVERTIBLE = 7
    # Differences in Timestamp precision or scale.
    DATETIME = 8
    # Differences in column names.
    # Note that when comparing tables directly, cannot get a NAMES difference for
    # its columns - would get a NOT_FOUND_xxx error.
    NAMES = 9
    # A set in source or destination is represented as an array on the other side.
    ARRAY_SET_MISMATCH = 10
    # A set or array in source or destination is represented as repeated on the
    # other side.
    REPEATED_STRUCT_MISMATCH = 11
    # Totally incompatible types.
    TYPES_INCOMPATIBLE = 12
    # Different types, but convertible from source to destination.
    TYPES_DIFFERENT_CONVERTIBLE = 13
    # Differences between integer type - same precision but different signs
    TYPES_SIGNLOSS_CONVERTIBLE = 14
    # Differences between table names
    TABLE_NAMES = 15


class SchemaDiff:
    """A schema difference when comparing schema tables / columns."""

    def __init__(self,
                 code: SchemaDiffCode,
                 description: str,
                 column: Optional[str] = None):
        self.code = code
        self.description = description
        self.column = column

    def is_convertible(self):
        """If this difference does not prevent a direct conversion."""
        return self.code in {
            SchemaDiffCode.NOT_FOUND_SRC,
            SchemaDiffCode.LABEL_CONVERTIBLE,
            SchemaDiffCode.DECIMAL_CONVERTIBLE,
            SchemaDiffCode.NAMES,
            SchemaDiffCode.TABLE_NAMES,
            SchemaDiffCode.ARRAY_SET_MISMATCH,
            SchemaDiffCode.REPEATED_STRUCT_MISMATCH,
            SchemaDiffCode.TYPES_DIFFERENT_CONVERTIBLE,
            SchemaDiffCode.TYPES_SIGNLOSS_CONVERTIBLE,
        }

    def is_struct_mismatch(self):
        """Acceptable stuct and name differences."""
        return self.code in {
            SchemaDiffCode.NAMES, SchemaDiffCode.TABLE_NAMES,
            SchemaDiffCode.ARRAY_SET_MISMATCH,
            SchemaDiffCode.REPEATED_STRUCT_MISMATCH
        }

    def __repr__(self):
        column = f' column: {self.column}' if self.column else ''
        return f'SchemaDiff<code: {self.code}{column} [{self.description}]>'


def _compare_names(src: List['Column'], dest: List['Column'], message: str,
                   col_name: Optional[str]) -> List[SchemaDiff]:
    src_fields = {f.name() for f in src}
    dest_fields = {f.name() for f in dest}
    src_missing = [name for name in dest_fields if name not in src_fields]
    dest_missing = [name for name in src_fields if name not in dest_fields]
    diffs = []
    if dest_missing:
        diffs.append(
            SchemaDiff(
                SchemaDiffCode.NOT_FOUND_DEST,
                f'{message}. Source elements cannot be found in '
                f'destination: {dest_missing}', col_name))
    if src_missing:
        diffs.append(
            SchemaDiff(
                SchemaDiffCode.NOT_FOUND_SRC,
                f'{message}. Destination elements cannot be found in '
                f'source: {src_missing}', col_name))
    return diffs


# Value types can be converted to key.
_CONVERTIBLE_TYPES = {
    Schema_pb2.ColumnInfo.TYPE_INT_8: [Schema_pb2.ColumnInfo.TYPE_BOOLEAN],
    Schema_pb2.ColumnInfo.TYPE_INT_16: [
        Schema_pb2.ColumnInfo.TYPE_BOOLEAN,
        Schema_pb2.ColumnInfo.TYPE_INT_8,
        Schema_pb2.ColumnInfo.TYPE_UINT_8,
    ],
    Schema_pb2.ColumnInfo.TYPE_INT_32: [
        Schema_pb2.ColumnInfo.TYPE_BOOLEAN, Schema_pb2.ColumnInfo.TYPE_INT_8,
        Schema_pb2.ColumnInfo.TYPE_INT_16, Schema_pb2.ColumnInfo.TYPE_UINT_8,
        Schema_pb2.ColumnInfo.TYPE_UINT_16, Schema_pb2.ColumnInfo.TYPE_DATE
    ],
    Schema_pb2.ColumnInfo.TYPE_INT_64: [
        Schema_pb2.ColumnInfo.TYPE_BOOLEAN,
        Schema_pb2.ColumnInfo.TYPE_INT_8,
        Schema_pb2.ColumnInfo.TYPE_INT_16,
        Schema_pb2.ColumnInfo.TYPE_INT_32,
        Schema_pb2.ColumnInfo.TYPE_UINT_8,
        Schema_pb2.ColumnInfo.TYPE_UINT_16,
        Schema_pb2.ColumnInfo.TYPE_UINT_32,
        Schema_pb2.ColumnInfo.TYPE_DATE,
        Schema_pb2.ColumnInfo.TYPE_DATETIME_64,
    ],
    Schema_pb2.ColumnInfo.TYPE_UINT_8: [Schema_pb2.ColumnInfo.TYPE_BOOLEAN],
    Schema_pb2.ColumnInfo.TYPE_UINT_16: [
        Schema_pb2.ColumnInfo.TYPE_BOOLEAN, Schema_pb2.ColumnInfo.TYPE_UINT_8
    ],
    Schema_pb2.ColumnInfo.TYPE_UINT_32: [
        Schema_pb2.ColumnInfo.TYPE_BOOLEAN, Schema_pb2.ColumnInfo.TYPE_UINT_8,
        Schema_pb2.ColumnInfo.TYPE_UINT_16
    ],
    Schema_pb2.ColumnInfo.TYPE_UINT_64: [
        Schema_pb2.ColumnInfo.TYPE_BOOLEAN, Schema_pb2.ColumnInfo.TYPE_UINT_8,
        Schema_pb2.ColumnInfo.TYPE_UINT_16, Schema_pb2.ColumnInfo.TYPE_UINT_32
    ],
    Schema_pb2.ColumnInfo.TYPE_FLOAT_64: [Schema_pb2.ColumnInfo.TYPE_FLOAT_32]
}

_SIGNLOSS_CONVERTIBLE_TYPES = {
    Schema_pb2.ColumnInfo.TYPE_INT_8: Schema_pb2.ColumnInfo.TYPE_UINT_8,
    Schema_pb2.ColumnInfo.TYPE_INT_16: Schema_pb2.ColumnInfo.TYPE_UINT_16,
    Schema_pb2.ColumnInfo.TYPE_INT_32: Schema_pb2.ColumnInfo.TYPE_UINT_32,
    Schema_pb2.ColumnInfo.TYPE_INT_64: Schema_pb2.ColumnInfo.TYPE_UINT_64,
    Schema_pb2.ColumnInfo.TYPE_UINT_8: Schema_pb2.ColumnInfo.TYPE_INT_8,
    Schema_pb2.ColumnInfo.TYPE_UINT_16: Schema_pb2.ColumnInfo.TYPE_INT_16,
    Schema_pb2.ColumnInfo.TYPE_UINT_32: Schema_pb2.ColumnInfo.TYPE_INT_32,
    Schema_pb2.ColumnInfo.TYPE_UINT_64: Schema_pb2.ColumnInfo.TYPE_INT_64,
}


class Column:
    """Code representation of a schema Column, parallel to Schema_pb2.Column.

    Attributes:
        field: The original source of the column (proto field, dataclass field etc).
        source_type: From which source this column was created.
        table_name: The name of the table that contains this column.
        java_class_name: Corresponding java name of the class that contains this
            column.
        info: Basic information about the column: type, name etc.
        data_annotation: Data annotations about this column: dq_field etc.
        clickhouse_annotation: Clickhouse specific information about the column:
            coding, compression etc.
        scala_annotation: Scala case class annotations about this column.
        has_data_annotation: If we have data annotations set in the class.
        has_clickhouse_annotation: If we have clickhouse annotations set in the class.
        has_scala_annotation: If we have scala annotations set in the class.
        fields: Containing sub-fields for nested types or structures.
        synthgen: The synthetic data generator or specification for this column.
    """

    def __init__(self):
        self.field = None
        self.source_type = SourceType.NONE
        self.table_name = ''
        self.java_class_name = ''
        self.info = Schema_pb2.ColumnInfo()
        self.data_annotation = Schema_pb2.ColumnDataAnnotation()
        self.clickhouse_annotation = Schema_pb2.ColumnClickhouseAnnotation()
        self.scala_annotation = Schema_pb2.ColumnScalaAnnotation()
        self.has_data_annotation = False
        self.has_clickhouse_annotation = False
        self.has_scala_annotation = False
        self.fields: List[Column] = []
        self.synthgen: synthgen.GeneratorSpec = None

    def name(self) -> str:
        """Shortcut to the name of this column."""
        return self.info.name

    def sql_name(self) -> str:
        """Return the name of this column in SQL statements."""
        if self.clickhouse_annotation.original_name:
            return self.clickhouse_annotation.original_name
        return self.name()

    def type_name(self) -> str:
        """Shortcut to the Schema type name of this column."""
        return Schema_pb2.ColumnInfo.ColumnType.Name(self.info.column_type)

    def add_sub_column(self, sub_column: 'Column'):
        """Adds a sub-column for this one (for nested, structured ones)."""
        self.fields.append(sub_column)
        self.info.field.append(sub_column.to_proto())

    def find_field(self, name: str):
        """Finds a sub-field(column) with specified name."""
        for f in self.fields:
            if f.name() == name:
                return f
        return None

    def timestamp_info(self) -> Schema_pb2.ColumnInfo.TimestampInfo:
        """Shortcut to the timestamp format information of this column."""
        if self.info.HasField('timestamp_info'):
            return self.info.timestamp_info
        return None

    def decimal_info(self) -> Schema_pb2.ColumnInfo.DecimalInfo:
        """Shortcut to the decimal format information of this column."""
        if self.info.HasField('decimal_info'):
            return self.info.decimal_info
        return None

    def is_low_cardinality(self) -> bool:
        """Shortcut if this column has a low cardinality value set."""
        return self.clickhouse_annotation.is_low_cardinality

    def is_id(self) -> bool:
        """Shortcut if this column is (part of) an table identifier."""
        return self.data_annotation.is_id

    def is_repeated(self) -> bool:
        """If this column is repeated."""
        return self.info.label == Schema_pb2.ColumnInfo.LABEL_REPEATED

    def is_optional(self) -> bool:
        """If this column has optional value."""
        return self.info.label == Schema_pb2.ColumnInfo.LABEL_OPTIONAL

    def is_required(self) -> bool:
        """If this column is required to have a value."""
        return self.info.label == Schema_pb2.ColumnInfo.LABEL_REQUIRED

    def validate(self) -> bool:
        """Validates the column specification. Raises `ValueError` on invalid."""
        if self.info.label == Schema_pb2.ColumnInfo.LABEL_UNDEFINED:
            raise ValueError(f'Undefined label for column `{self.name()}`')
        if (self.fields and self.info.column_type not in {
                Schema_pb2.ColumnInfo.TYPE_ARRAY,
                Schema_pb2.ColumnInfo.TYPE_SET, Schema_pb2.ColumnInfo.TYPE_MAP,
                Schema_pb2.ColumnInfo.TYPE_NESTED
        }):
            raise ValueError('Fields specified for not nested column '
                             f'`{self.name()}` of {self.table_name}')
        decimal_info = self.decimal_info()
        if self.info.column_type == Schema_pb2.ColumnInfo.TYPE_DECIMAL:
            if decimal_info is None:
                raise ValueError(
                    'Expecting decimal info annotation for decimal field '
                    f'`{self.name()}` of `{self.table_name}`')
            if (decimal_info.precision < 0 or
                    decimal_info.precision > MAX_DECIMAL_PRECISION):
                raise ValueError(
                    'Decimal precision for field '
                    f'`{self.name()}` of `{self.table_name}` is too large '
                    f'({decimal_info.precision} > {MAX_DECIMAL_PRECISION})')
            if decimal_info.precision < decimal_info.scale:
                raise ValueError(
                    'Decimal precision for field '
                    f'`{self.name()}` of `{self.table_name}` is less than the '
                    f'scale ({decimal_info.precision} < {decimal_info.scale})')
        elif (decimal_info is not None and
              self.info.column_type != Schema_pb2.ColumnInfo.TYPE_DECIMAL):
            raise ValueError(
                'Decimal info annotation present for non decimal field '
                f'`{self.name()}` of `{self.table_name}`')
        timestamp_info = self.timestamp_info()
        if timestamp_info is not None:
            if self.info.column_type != Schema_pb2.ColumnInfo.TYPE_DATETIME_64:
                raise ValueError(
                    'Timestamp info annotation present for non timestamp '
                    f'field `{self.name()}` or {self.table_name}')
            if timestamp_info.precision not in [0, 3, 6, 9]:
                raise ValueError(
                    'Timestamp precision for field '
                    f'`{self.name()}` of `{self.table_name}` is invalid: '
                    f'{timestamp_info.precision} (expected 0, 3, 6, or 9)')
        opt = self.clickhouse_annotation  # shortcut
        if (opt.is_low_cardinality and
                self.info.column_type != Schema_pb2.ColumnInfo.TYPE_STRING):
            raise ValueError(
                'Low cardinality fields allowed only for string types.'
                f' Detected for `{self.name()}` of `{self.table_name}`')
        if opt.HasField('delta_compression_width'):
            if opt.delta_compression_width not in [1, 2, 4, 8]:
                raise ValueError('Invalid delta_compression value for field '
                                 f'`{self.name()}` of `{self.table_name}`: '
                                 f'{opt.delta_compression_width}.')
            if self.info.column_type not in _DELTA_COMPRESSION_MAX_DELTA:
                raise ValueError(
                    'Delta_compression cannot be used for type of field '
                    f'`{self.name()}` of `{self.table_name}`.')
            max_delta = _DELTA_COMPRESSION_MAX_DELTA[self.info.column_type]
            if opt.delta_compression_width > max_delta:
                raise ValueError(
                    'Delta compression too large for the type of field '
                    f'`{self.name()}` of `{self.table_name}` '
                    f'({opt.delta_compression_width} > {max_delta})')
        if (opt.compression_type !=
                Schema_pb2.ColumnClickhouseAnnotation.COMPRESS_ZSTD and
                opt.HasField('compression_level')):
            raise ValueError(
                'Compression level can be specified only for ZSTD compressed '
                'fields. Found it for field '
                f'`{self.name()}` of `{self.table_name}`.')
        return True

    def to_proto(self) -> Schema_pb2.Column:
        """Convert this column to its protobuf representation."""
        column = Schema_pb2.Column()
        column.info.CopyFrom(self.info)
        if self.has_data_annotation:
            column.data_annotation.CopyFrom(self.data_annotation)
        if self.has_clickhouse_annotation:
            column.clickhouse_annotation.CopyFrom(self.clickhouse_annotation)
        if self.has_scala_annotation:
            column.scala_annotation.CopyFrom(self.scala_annotation)
        return column

    def __repr__(self) -> str:
        return self.to_proto().__repr__()

    def _compare_subfields(self, column: 'Column', prefix: str):
        diffs = []
        name = full_name(prefix, self.name())
        diffs.extend(
            _compare_names(column.fields, self.fields,
                           f'Differences in sub-fields for column `{name}`',
                           name))
        for field in self.fields:
            c = column.find_field(field.name())
            if c is not None:
                diffs.extend(field.compare(c, name))
        return diffs

    def _compare_attributes(self,
                            column: 'Column',
                            prefix: str,
                            skip_label: bool = False):
        diffs = []
        name = full_name(prefix, self.name())
        if not skip_label and self.info.label != column.info.label:
            diff = SchemaDiffCode.LABEL
            if self.is_repeated() or (self.is_optional() and
                                      column.is_required()):
                diff = SchemaDiffCode.LABEL_CONVERTIBLE
            slabel = Schema_pb2.ColumnInfo.Label.Name(self.info.label)
            clabel = Schema_pb2.ColumnInfo.Label.Name(column.info.label)
            diffs.append(
                SchemaDiff(
                    diff, f'Different labels for `{self.name()}`: '
                    f'{slabel} vs {clabel}', name))
        if (self.info.column_type == Schema_pb2.ColumnInfo.TYPE_DECIMAL and
                self.info.column_type == column.info.column_type):
            if self.decimal_info().precision != column.decimal_info().precision:
                diff = SchemaDiffCode.DECIMAL
                if self.decimal_info().precision > column.decimal_info(
                ).precision:
                    diff = SchemaDiffCode.DECIMAL_CONVERTIBLE
                diffs.append(
                    SchemaDiff(
                        diff,
                        f'Different decimal precisions for `{self.name()}`: '
                        f'{self.decimal_info().precision} vs '
                        f'{column.decimal_info().precision}', name))
            if self.decimal_info().scale != column.decimal_info().scale:
                diff = SchemaDiffCode.DECIMAL
                if self.decimal_info().scale > column.decimal_info().scale:
                    diff = SchemaDiffCode.DECIMAL_CONVERTIBLE
                diffs.append(
                    SchemaDiff(
                        diff, f'Different decimal scale for `{self.name()}`: '
                        f'{self.decimal_info().scale} vs '
                        f'{column.decimal_info().scale}', name))
        if (self.info.column_type == Schema_pb2.ColumnInfo.TYPE_DATETIME_64 and
                self.info.column_type == column.info.column_type):
            if self.info.timestamp_info.precision != column.info.timestamp_info.precision:
                diffs.append(
                    SchemaDiff(
                        SchemaDiffCode.DATETIME,
                        f'Different timestamp precisions for `{self.name()}`: '
                        f'{self.info.timestamp_info.precision} vs '
                        f'{column.info.timestamp_info.precision}', name))
            if self.info.timestamp_info.timezone != column.info.timestamp_info.timezone:
                diffs.append(
                    SchemaDiff(
                        SchemaDiffCode.DATETIME,
                        f'Different timestamp timezone for `{self.name()}`: '
                        f'`{self.info.timestamp_info.timezone}` vs '
                        f'`{column.info.timestamp_info.timezone}`', name))
        return diffs

    def has_compatible_type(self, column: 'Column') -> bool:
        """If this column and provided `column` have compatible types.
        This means that values of `column` can be stored in this one."""
        return (self.info.column_type == column.info.column_type or
                (self.info.column_type in _CONVERTIBLE_TYPES and
                 column.info.column_type
                 in _CONVERTIBLE_TYPES[self.info.column_type]),
                (self.info.column_type in _SIGNLOSS_CONVERTIBLE_TYPES and
                 column.info.column_type
                 == _SIGNLOSS_CONVERTIBLE_TYPES[self.info.column_type]))

    def compare_type(self,
                     column: 'Column',
                     prefix: str,
                     skip_label: bool = False):
        """Compares just the types of this column w/ the argument."""
        diffs = []
        name = full_name(prefix, self.name())
        if self.info.column_type != column.info.column_type:
            if not self.has_compatible_type(column):
                diffs.append(
                    SchemaDiff(
                        SchemaDiffCode.TYPES_INCOMPATIBLE,
                        'Totally incompatible types found for '
                        f'column {self.name()}: '
                        f'source type `{column.type_name()}` '
                        f'vs. destination type `{self.type_name()}`', name))
                return diffs
            if (self.info.column_type in _SIGNLOSS_CONVERTIBLE_TYPES and
                    column.info.column_type
                    == _SIGNLOSS_CONVERTIBLE_TYPES[self.info.column_type]):
                diffs.append(
                    SchemaDiff(
                        SchemaDiffCode.TYPES_SIGNLOSS_CONVERTIBLE,
                        'Different but compatible with possible sign loss '
                        f'types found for column {self.name()}: '
                        f'source type `{column.type_name()}` '
                        f'vs. destination type `{self.type_name()}`', name))
            else:
                diffs.append(
                    SchemaDiff(
                        SchemaDiffCode.TYPES_DIFFERENT_CONVERTIBLE,
                        'Different but compatible types found for '
                        f'column {self.name()}: '
                        f'source type `{column.type_name()}` '
                        f'vs. destination type `{self.type_name()}`', name))
        diffs.extend(self._compare_subfields(column, prefix))
        diffs.extend(self._compare_attributes(column, prefix, skip_label))
        return diffs

    def array_set_mismatch(self, column: 'Column', prefix: str,
                           diffs: List[SchemaDiff]) -> bool:
        """Checks if there are different but compatible ARRAY/SET structures."""
        if self.info.column_type == column.info.column_type:
            return False
        name = full_name(prefix, self.name())
        if (self.info.column_type == Schema_pb2.ColumnInfo.TYPE_ARRAY and
                column.info.column_type == Schema_pb2.ColumnInfo.TYPE_SET):
            diffs.append(
                SchemaDiff(
                    SchemaDiffCode.ARRAY_SET_MISMATCH,
                    f'Difference in structure array vs. set for `{self.name()}`',
                    name))
            diffs.extend(self.fields[0].compare(column.fields[0], name))
            return True
        if (self.info.column_type in (Schema_pb2.ColumnInfo.TYPE_ARRAY,
                                      Schema_pb2.ColumnInfo.TYPE_SET) and
                column.is_repeated() and column.info.column_type
                not in (Schema_pb2.ColumnInfo.TYPE_ARRAY,
                        Schema_pb2.ColumnInfo.TYPE_SET,
                        Schema_pb2.ColumnInfo.TYPE_MAP)):
            diffs.append(
                SchemaDiff(
                    SchemaDiffCode.REPEATED_STRUCT_MISMATCH,
                    f'Difference in structure: {self.type_name()} '
                    f'vs. repeated for `{self.name()}`', name))
            diffs.extend(self.fields[0].compare_type(column, name, True))
            return True
        return False

    def compare(self, column: 'Column', prefix: str = '') -> List[SchemaDiff]:
        """Compares the structure and type of this Column with the other.

        The comparison is done in the light of reading data in the `column`
        structure into the structure represented by this Column.

        Args:
          column: the 'source' column we compare our structure with.
          prefix: used for prefixing the name of this column for sub-field case.

        Returns:
          The differences we find, in encoded form.
        """
        diffs = []
        name = full_name(prefix, self.name())
        if self.name() != column.name():
            diffs.append(
                SchemaDiff(
                    SchemaDiffCode.NAMES,
                    f'Column names different: `{self.name()}`'
                    f' vs `{column.name()}`', name))
        if self.info.column_type != column.info.column_type:
            if (not self.array_set_mismatch(column, prefix, diffs) and
                    not column.array_set_mismatch(self, prefix, diffs)):
                diffs.extend(self.compare_type(column, prefix))
        else:
            diffs.extend(self._compare_subfields(column, prefix))
            diffs.extend(self._compare_attributes(column, prefix))
        return diffs


class Table:
    """Code representation of a schema Table, parallel to Schema_pb2.Table.

    Attributes:
        msg: The original source of this table structure
            (e.g. proto descriptor, dataclass etc)
        source_type: From which source this table was created.
        info: Basic information about this table - proto format.
        has_data_annotation: If we have any information about data in this table.
        data_annotation: Specific information about data in this table.
        has_clickhouse_annotation: If we have any clickhouse information about
            this table.
        clickhouse_annotation: Clickhouse specific information about this table
        has_scala_annotation: If we have any scala specific information about
            this table.
        scala_annotation: Scala specific annotations about this table.
        columns: The columns of this table, in order.
        columns_by_name: Map from column name to column object.
        nested: Sub-table (ie. sub classes / protos) defined inside this table.
                This is not generally recommended.
    """

    def __init__(self):
        """Creates an empty table object."""
        # The original source of this table (proto descriptor, dataclass etc)
        self.msg = None
        # From which source this table was created
        self.source_type = SourceType.NONE
        # Basic information about this table - proto format.
        self.info = Schema_pb2.TableInfo()
        # If we have any information about data in this table.
        self.has_data_annotation = False
        # Specific information about data in this table.
        self.data_annotation = Schema_pb2.TableDataAnnotation()
        # If we have any clickhouse information about this table.
        self.has_clickhouse_annotation = False
        # Clickhouse specific information about this table
        self.clickhouse_annotation = Schema_pb2.TableClickhouseAnnotation()
        # If we have any scala specific information about this table.
        self.has_scala_annotation = False
        # Scala specific annotations about this table.
        self.scala_annotation = Schema_pb2.TableScalaAnnotation()
        # The columns of this table, in order.
        self.columns = []
        # Map from column name to column object.
        self.columns_by_name: Dict[str, Column] = {}
        # Sub-table (ie. sub classes / protos) defined inside this table.
        # This is not recommended.
        self.nested = []

    def add_column(self, column: Column):
        """Adds a internal column to our structure."""
        self.columns.append(column)
        self.info.column.append(column.to_proto())
        self.columns_by_name[column.info.name] = column

    def add_nested(self, nested: 'Table'):
        """Adds a nested sub-table to our structure."""
        self.nested.append(nested)
        self.info.nested.append(nested.info)

    def name(self) -> str:
        return self.info.name

    def full_name(self) -> str:
        return full_name(self.info.package, self.info.name)

    def validate(self) -> bool:
        """Validates this as an SQL table. Raises exceptions on error."""
        for column in self.columns:
            column.validate()
        self._validate_clickhouse_options()
        return True

    def _check_order_option(self, option_name: str, option_list: List[str],
                            allows_order):
        if option_list and not allows_order:
            raise ValueError(f'Cannot have {option_name} with the provided '
                             f'Engine for {self.full_name()}.')
        for exp in option_list:
            if exp.find(')') < 0 and exp not in self.columns_by_name:
                raise ValueError(f'{option_name} field `{exp}` not found '
                                 f'in `{self.full_name()}`.')
            if exp in self.columns_by_name:
                if (self.columns_by_name[exp].info.label !=
                        Schema_pb2.ColumnInfo.LABEL_REQUIRED):
                    raise ValueError(
                        f'{option_name} field allowed only for required ones. '
                        f'For field `{exp}` in `{self.full_name()}`.')

    def _validate_clickhouse_options(self) -> bool:
        if not self.has_clickhouse_annotation:
            return True
        opt = self.clickhouse_annotation
        allows_order = opt.engine in (
            # Allow also w/ default, assuming an engine would be set on top.
            Schema_pb2.TableClickhouseAnnotation.ENGINE_DEFAULT,
            Schema_pb2.TableClickhouseAnnotation.ENGINE_MERGE_TREE,
            Schema_pb2.TableClickhouseAnnotation.ENGINE_REPLICATED_MERGE_TREE)
        self._check_order_option('PARTITION BY',
                                 opt.partition_by_sql_expression, allows_order)
        self._check_order_option('SAMPLE BY', opt.sample_by_sql_expression,
                                 allows_order)
        self._check_order_option('ORDER BY', opt.order_by_fields, allows_order)
        if opt.HasField('index_granularity') and not allows_order:
            raise ValueError(
                'Cannot specify index granularity for the provided '
                'Engine for {self.full_name()}')
        return True

    def to_proto(self) -> Schema_pb2.Table:
        """Converts this Table to its protobuf representation."""
        table = Schema_pb2.Table()
        table.info.CopyFrom(self.info)
        if self.has_data_annotation:
            table.data_annotation.CopyFrom(self.data_annotation)
        if self.has_clickhouse_annotation:
            table.clickhouse_annotation.CopyFrom(self.clickhouse_annotation)
        if self.has_scala_annotation:
            table.scala_annotation.CopyFrom(self.scala_annotation)
        return table

    def __repr__(self) -> str:
        return self.to_proto().__repr__()

    def column_names(self) -> List[str]:
        """The column names of this table, in order."""
        return [column.name() for column in self.columns]

    def pick_columns(self, names: List[str]) -> List[Column]:
        """Selects all columns in this table whose names are included in `names`."""
        return [column for column in self.columns if column.name() in names]

    def compare(self,
                table: 'Table',
                columns: Optional[List[str]] = None) -> List[SchemaDiff]:
        """Compares the column structure of this Table with the other.

        The comparison is done in the light of reading data in the `table`
        structure into the structure represented by this Table.

        Args:
          table: the table we compare our structure with.
          columns: optional list of column names to compare.

        Returns:
          The differences we find, in encoded form.
        """
        if columns is None:
            s_columns = self.columns
            t_columns = table.columns
        else:
            s_columns = self.pick_columns(columns)
            t_columns = table.pick_columns(columns)

        diffs = []
        if self.name() != table.name():
            diffs.append(
                SchemaDiff(
                    SchemaDiffCode.TABLE_NAMES,
                    f'Table names different: `{self.name()}`'
                    f' vs `{table.name()}`'))
        elif self.info.full_name != table.info.full_name:
            diffs.append(
                SchemaDiff(
                    SchemaDiffCode.TABLE_NAMES,
                    f'Table full names different: `{self.info.full_name}`'
                    f' vs `{table.info.full_name}`'))
        diffs.extend(
            _compare_names(t_columns, s_columns,
                           'Differences in columns for table `{self.name()}`',
                           None))
        for s in s_columns:
            t = [t for t in t_columns if t.name() == s.name()]
            if t:
                diffs.extend(s.compare(t[0]))
        return diffs
