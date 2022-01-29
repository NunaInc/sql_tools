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
"""Converts SQL create statement to Schema structure."""

from antlr4.ParserRuleContext import ParserRuleContext
from dataschema import Schema, Schema_pb2, annotations
from sql_analyze.grammars import trees
from sql_analyze.grammars.ClickHouse.ClickHouseParser import ClickHouseParser
from typing import List, Optional


def _Error(ctx: ParserRuleContext, error: str):
    raise ValueError(f'Error: @{ctx.start.line}:{ctx.start.column}: {error}\n'
                     f'`{trees.recompose(ctx)}`\n')


class _SqlTypeAnnotate(annotations.ColumnAnnotation):

    def __init__(self, column_type):
        self.column_type = column_type

    def annotate_column(self, column: Schema.Column):
        column.info.column_type = self.column_type
        column.info.label = Schema_pb2.ColumnInfo.LABEL_REQUIRED


class _String(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_STRING)


class _Bytes(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_BYTES)


class _Int8(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_INT_8)


class _Int16(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_INT_16)


class _Int32(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_INT_32)


class _Int64(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_INT_64)


class _UInt8(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_UINT_8)


class _UInt16(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_UINT_16)


class _UInt32(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_UINT_32)


class _UInt64(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_UINT_64)


class _Float32(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_FLOAT_32)


class _Float64(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_FLOAT_64)


class _Date(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_DATE)


class _DateTime(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_DATETIME_64)


class _Decimal(_SqlTypeAnnotate):

    def __init__(self):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_DECIMAL)


class _Nested(_SqlTypeAnnotate):

    def __init__(self, columns: List[Schema.Column]):
        super().__init__(Schema_pb2.ColumnInfo.TYPE_NESTED)
        self.columns = columns

    def annotate_column(self, column: Schema.Column):
        super().annotate_column(column)
        for col in self.columns:
            column.add_sub_column(col)


class _Nullable(annotations.ColumnAnnotation):
    """Annotation for nullable column (ie. Optional) done as a function."""

    def annotate_column(self, column: Schema.Column):
        column.info.label = Schema_pb2.ColumnInfo.LABEL_OPTIONAL


class _Concat(annotations.ColumnAnnotation):
    """Concatenates column annotations and applies them."""

    def __init__(self, annot: List[annotations.ColumnAnnotation]):
        self.annot = annot

    def annotate_column(self, column: Schema.Column):
        for sub_annotation in self.annot:
            sub_annotation.annotate_column(column)


class _Unsupported(annotations.ColumnAnnotation):

    def __init__(self, name):
        self.name = name

    def annotate_column(self, column: Schema.Column):
        raise ValueError(f'Unsupported type `{self.name}` used '
                         f'for column `{column.name}`')


class _EnumValues(annotations.ColumnAnnotation):
    """Annotates a column with an Enum-like type."""

    def __init__(self, values: List[str]):
        self.values = values

    def annotate_column(self, column: Schema.Column):
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_STRING
        column.info.label = Schema_pb2.ColumnInfo.LABEL_REQUIRED
        column.has_data_annotation = True
        column.data_annotation.dq_field.enum_value.extend(self.values)
        column.has_clickhouse_annotation = True
        column.clickhouse_annotation.is_low_cardinality = True


def _SubColumn(column: Schema.Column, name: str):
    subcolumn = Schema.Column()
    subcolumn.table_name = Schema.full_name(column.table_name, column.name)
    subcolumn.java_class_name = Schema.full_name(column.java_class_name,
                                                 column.name)
    subcolumn.field = column.info
    subcolumn.source_type = Schema.SourceType.SQL_CREATE_TABLE
    subcolumn.info.name = name
    return subcolumn


class _Array(annotations.ColumnAnnotation):
    """Annotates a column as an Array(..) - which means a repeated or an Array struct."""

    def __init__(self, element: annotations.ColumnAnnotation):
        self.element = element

    def annotate_column(self, column: Schema.Column):
        element = _SubColumn(column, 'element')
        self.element.annotate_column(element)
        if (element.info.label == Schema_pb2.ColumnInfo.LABEL_REQUIRED and
                element.info.column_type
                not in (Schema_pb2.ColumnInfo.TYPE_ARRAY,
                        Schema_pb2.ColumnInfo.TYPE_SET,
                        Schema_pb2.ColumnInfo.TYPE_MAP)):
            self.element.annotate_column(column)
            column.info.label = Schema_pb2.ColumnInfo.LABEL_REPEATED
            return
        column.add_sub_column(element)
        column.info.label = Schema_pb2.ColumnInfo.LABEL_REPEATED
        column.info.column_type = Schema_pb2.ColumnInfo.TYPE_ARRAY


class _Map(annotations.ColumnAnnotation):
    """Annotates a column as a Map(..) - a structure w/ key and value."""

    def __init__(self, key: annotations.ColumnAnnotation,
                 value: annotations.ColumnAnnotation):
        self.key = key
        self.value = value

    def annotate_column(self, column: Schema.Column):
        key = _SubColumn(column, 'key')
        self.element.annotate_column(key)
        value = _SubColumn(column, 'value')
        self.element.annotate_column(value)
        column.add_sub_column(key)
        column.add_sub_column(value)


def _DateTimeType(tz='Etc/UTC'):
    return _Concat([_DateTime(), annotations.Timestamp(1, tz)])


def _DateTime64Type(precision=1, tz='Etc/UTC'):
    return _Concat([_DateTime(), annotations.Timestamp(precision, tz)])


def _DecimalType(precision, scale):
    return _Concat([_Decimal(), annotations.Decimal(precision, scale)])


# Per max precision from here:
#   https://clickhouse.com/docs/en/sql-reference/data-types/decimal/
def _Decimal32Type(scale=0):
    return _Concat([_Decimal(), annotations.Decimal(9, scale)])


def _Decimal64Type(scale=0):
    return _Concat([_Decimal(), annotations.Decimal(18, scale)])


def _FixedString(width=0):
    if not width:
        return _String()
    return _Concat([_String(), annotations.Width(width)])


def _Decimal128Type(scale=0):
    return _Concat([_Decimal(), annotations.Decimal(38, scale)])


def _Decimal256Type(scale=0):
    return _Concat([_Decimal(), annotations.Decimal(76, scale)])


def _LowCardinalityType(subtype):
    return _Concat([subtype, annotations.LowCardinality()])


def _NullableType(subtype):
    return _Concat([subtype, _Nullable()])


def _ArrayType(subtype):
    return _Array(subtype)


def _MapType(key, value):
    return _Map(key, _Concat([value, _Nullable()]))


_BASIC_TYPES = {
    'STRING': _String(),
    'INT8': _Int8(),
    'INT16': _Int16(),
    'INT32': _Int32(),
    'INT64': _Int64(),
    'UINT8': _UInt8(),
    'UINT16': _UInt16(),
    'UINT32': _UInt32(),
    'UINT64': _UInt64(),
    'FLOAT32': _Float32(),
    'FLOAT64': _Float64(),
    'DATE': _Date(),
    'DATE32': _Date(),
    'UUID': _Bytes(),
}

_COMPOSED_TYPES = {
    'LOWCARDINALITY': (_LowCardinalityType, 1),
    'NULLABLE': (_NullableType, 1),
    'ARRAY': (_ArrayType, 1),
    'MAP': (_MapType, 2),
}

_PARAM_TYPES = {
    'FIXEDSTRING': (_FixedString, 'i'),
    'DATETIME': (_DateTimeType, 's'),
    'DATETIME64': (_DateTime64Type, 'is'),
    'DECIMAL': (_DecimalType, 'ii'),
    'DECIMAL32': (_Decimal32Type, 'i'),
    'DECIMAL64': (_Decimal64Type, 'i'),
    'DECIMAL128': (_Decimal128Type, 'i'),
    'DECIMAL256': (_Decimal256Type, 'i'),
}


def _ParseString(ctx: ClickHouseParser.ColumnExprContext):
    text = None
    if isinstance(ctx, ClickHouseParser.ColumnExprLiteralContext):
        if ctx.literal() and ctx.literal().STRING_LITERAL():
            text = trees.recompose(ctx)
        else:
            return None
    elif isinstance(ctx, ClickHouseParser.ColumnExprIdentifierContext):
        text = trees.recompose(ctx)
        if not text.startswith('"') or not text.endswith('"'):
            return None
    else:
        return None
    # We know that the string literal is parsed to a correct format, so
    # it is safe to evaluate.
    # pylint: disable=eval-used
    return eval(text, {}, {})


def _ParseInt(ctx: ClickHouseParser.ColumnExprContext):
    if not isinstance(ctx, ClickHouseParser.ColumnExprLiteralContext):
        return None
    literal = ctx.literal()
    if (not literal.numberLiteral() or
            not literal.numberLiteral().DECIMAL_LITERAL()):
        return None
    return int(trees.recompose(literal))


def _ParseArgs(ctx, columns: List[ClickHouseParser.ColumnExprListContext],
               args: str, context: str):
    if len(columns) > len(args):
        return _Error(
            ctx, f'{context} expecting at most '
            f'{len(args)} arguments. Got {len(columns)}')
    values = []
    for (col, argtype) in zip(columns, args):
        if not isinstance(col, ClickHouseParser.ColumnsExprColumnContext):
            return _Error(
                col, f'{context} expecting basic literal expression argument')
        expr = col.columnExpr()
        if argtype == 's':
            text = _ParseString(col.columnExpr())
            if text is None:
                return _Error(expr,
                              f'{context} expecting string literal argument')
            values.append(text)
        elif argtype == 'i':
            value = _ParseInt(col.columnExpr())
            if value is None:
                return _Error(
                    expr, f'{context} expecting decimal int literal argument')
            values.append(value)
    return values


def _ParseColumnType(ctx: ClickHouseParser.ColumnTypeExprContext,
                     column: Schema.Column):
    if isinstance(ctx, ClickHouseParser.ColumnTypeExprSimpleContext):
        type_name = trees.recompose(ctx.identifier())
        if type_name.upper() not in _BASIC_TYPES:
            return _Error(ctx, f'Unsupported type `{type_name}`')
        return _BASIC_TYPES[type_name.upper()]
    if isinstance(ctx, ClickHouseParser.ColumnTypeExprNestedContext):
        typename = trees.recompose(ctx.identifier(0))
        if typename.upper != 'NESTED':
            return _Error(ctx, 'Expected `Nested` as type name')
        index = 0
        columns = []
        while ctx.identifier(index + 1):
            name = trees.recompose(ctx.identifier(index + 1))
            columns.append(
                ClickHouseConvertColumn(
                    ClickHouseColumnInfo(name, ctx.columnTypeExpr(index), None),
                    Schema.full_name(column.table_name, name),
                    Schema.full_name(column.java_class_name, name)))
            index += 1
        return _Nested(columns)
    if isinstance(ctx, ClickHouseParser.ColumnTypeExprEnumContext):
        values = []
        for val in ctx.enumValue():
            # We know that the string literal is parsed to a correct format, so
            # it is safe to evaluate.
            # pylint: disable=eval-used
            values.append(eval(val.STRING_LITERAL().getPayload().text, {}, {}))
        return _EnumValues(values)
    if isinstance(ctx, ClickHouseParser.ColumnTypeExprComplexContext):
        type_name = trees.recompose(ctx.identifier())
        if type_name.upper() not in _COMPOSED_TYPES:
            return _Error(ctx, f'Unsupported composed type `{type_name}`')
        fun, num = _COMPOSED_TYPES[type_name.upper()]
        sub_types = ctx.columnTypeExpr()
        if len(sub_types) != num:
            return _Error(
                ctx, f'Type `{type_name}` expecting {num} subtypes. '
                f'Got {len(sub_types)}')
        args = [_ParseColumnType(col_type, column) for col_type in sub_types]
        return fun(*args)
    if isinstance(ctx, ClickHouseParser.ColumnTypeExprParamContext):
        type_name = trees.recompose(ctx.identifier())
        if type_name.upper() not in _PARAM_TYPES:
            return _Error(ctx, f'Unsupported parametrized type `{type_name}`')
        fun, args = _PARAM_TYPES[type_name.upper()]
        if not ctx.columnExprList():
            return fun()
        columns = ctx.columnExprList().columnsExpr()
        values = _ParseArgs(ctx, columns, args,
                            f'Parametrized type `{type_name}`')
        return fun(*values)


def _ZstdCodec(level=None):
    return annotations.Compression('ZSTD', level)


def _Lz4HcCodec(level=None):
    return annotations.Compression('LZ4HC', level)


def _Lz4Codec():
    return annotations.Compression('LZ4')


def _NoneCodec():
    return annotations.Compression('UNCOMPRESSED')


def _DeltaCodec(delta=None):
    return annotations.DeltaCompression(delta)


_CODECS = {
    'ZSTD': (_ZstdCodec, 'i'),
    'LZ4HC': (_Lz4HcCodec, 'i'),
    'LZ4': (_Lz4Codec, None),
    'NONE': (_NoneCodec, None),
    'DELTA': (_DeltaCodec, 'i'),
}


def _ParseCodecExpr(ctx: ClickHouseParser.CodecExprContext):
    codecs = []
    for expr in ctx.codecArgExpr():
        codec_name = trees.recompose(expr.identifier())
        if codec_name.upper() not in _CODECS:
            return _Error(ctx, f'Unsupported codec type `{codec_name}`')
        (fun, args) = _CODECS[codec_name.upper()]
        if not expr.columnExprList():
            codecs.append(fun())
        else:
            columns = expr.columnExprList().columnsExpr()
            values = _ParseArgs(expr, columns, args, f'Codec `{codec_name}`')
            codecs.append(fun(*values))
    return codecs


class ClickHouseColumnInfo:
    """Information for obtaining a schema column from parsed tree structure."""

    def __init__(self,
                 name: str,
                 type_ctx: Optional[ClickHouseParser.ColumnTypeExprContext],
                 codec_ctx: Optional[ClickHouseParser.CodecExprContext] = None,
                 comment: Optional[str] = None):
        self.name = name
        self.type_ctx = type_ctx
        self.codec_ctx = codec_ctx
        self.comment = comment


def ClickHouseConvertColumn(info: ClickHouseColumnInfo, table_name: str,
                            java_class_name: str) -> Schema.Column:
    column = Schema.Column()
    column.table_name = table_name
    column.java_class_name = java_class_name
    column.field = info
    column.source_type = Schema.SourceType.SQL_CREATE_TABLE
    column.info.name = info.name
    annot = []
    if info.comment:
        annot.append(annotations.Comment(info.comment))
    if info.type_ctx:
        annot.append(_ParseColumnType(info.type_ctx, column))
    if info.codec_ctx:
        annot.extend(_ParseCodecExpr(info.codec_ctx))
    for annotation in annot:
        annotation.annotate_column(column)
    return column


def ColumnsFromTableSchema(ctx: ClickHouseParser.TableSchemaClauseContext,
                           table_name: str, java_class_name: str):
    if not isinstance(ctx, ClickHouseParser.SchemaDescriptionClauseContext):
        return []
    columns = []
    for expr in ctx.tableElementExpr():
        if not isinstance(expr, ClickHouseParser.TableElementExprColumnContext):
            continue
        expr = expr.tableColumnDfnt()
        if expr.COMMENT() and expr.STRING_LITERAL():
            # We know that the string literal is parsed to a correct format, so
            # it is safe to evaluate.
            # pylint: disable=eval-used
            comment = eval(expr.STRING_LITERAL().getPayload().text, {}, {})
        else:
            comment = None
        info = ClickHouseColumnInfo(trees.recompose(expr.nestedIdentifier()),
                                    expr.columnTypeExpr(), expr.codecExpr(),
                                    comment)

        columns.append(
            ClickHouseConvertColumn(info, table_name, java_class_name))
    return columns


_ENGINE = {'MergeTree', 'Log', 'TinyLog', 'ReplicatedMergeTree'}


def _SplitExpressions(ctx: ClickHouseParser.ColumnExprContext):
    if isinstance(ctx, ClickHouseParser.ColumnExprParensContext):
        return [ctx.columnExpr()]
    elif isinstance(ctx, ClickHouseParser.ColumnExprTupleContext):
        return ctx.columnExprList().columnsExpr()
    else:
        return [ctx]


def ExtractEngineAnnotations(ctx: ClickHouseParser.EngineClauseContext,
                             table: Schema.Table):

    annot = []
    engine = trees.recompose(ctx.engineExpr().identifierOrNull())
    if engine in _ENGINE:
        annot.append(annotations.ClickhouseEngine(engine))
    if ctx.orderByClause(0):
        if len(ctx.orderByClause(0).orderExprList().orderExpr()) == 1:
            annot.append(
                annotations.ClickhouseOrderBy([
                    trees.recompose(expr) for expr in _SplitExpressions(
                        ctx.orderByClause(0).orderExprList().orderExpr(
                            0).columnExpr())
                ]))
        else:
            annot.append(
                annotations.ClickhouseOrderBy([
                    trees.recompose(expr) for expr in ctx.orderByClause(
                        0).orderExprList().orderExpr()
                ]))
    if ctx.partitionByClause(0):
        annot.append(
            annotations.ClickhousePartitionBy([
                trees.recompose(expr) for expr in _SplitExpressions(
                    ctx.partitionByClause(0).columnExpr())
            ]))
    if ctx.sampleByClause(0):
        annot.append(
            annotations.ClickhouseSampleBy([
                trees.recompose(expr) for expr in _SplitExpressions(
                    ctx.sampleByClause(0).columnExpr())
            ]))
    if ctx.settingsClause(0):
        for setexpr in ctx.settingsClause(0).settingExprList().settingExpr():
            if trees.recompose(
                    setexpr.identifier()).upper() == 'INDEX_GRANULARITY':
                annot.append(
                    annotations.ClickhouseIndexGranularity(
                        int(trees.recompose(setexpr.literal()))))
    for annotation in annot:
        annotation.annotate_table(table)


def ClickHouseConvertTable(
        sql_statement: str,
        schema_ctx: Optional[ClickHouseParser.TableSchemaClauseContext],
        engine_ctx: Optional[ClickHouseParser.EngineClauseContext],
        full_name: str,
        java_package_name: Optional[str] = None) -> Schema.Table:
    table = Schema.Table()
    table.msg = sql_statement
    table.source_type = Schema.SourceType.SQL_CREATE_TABLE
    table.info.name = full_name.split('.')[-1]
    table.info.full_name = full_name
    table.info.package = '.'.join(full_name.split('.')[:-1])
    if java_package_name:
        table.info.java_package = java_package_name
    else:
        table.info.java_package = table.info.package
    java_full_name = Schema.full_name(table.info.java_package, table.info.name)
    if schema_ctx:
        columns = ColumnsFromTableSchema(schema_ctx, table.info.full_name,
                                         java_full_name)
        for col in columns:
            table.add_column(col)
    if engine_ctx:
        ExtractEngineAnnotations(engine_ctx, table)
    return table
