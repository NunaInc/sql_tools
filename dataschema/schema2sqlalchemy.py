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
"""Converts python data schema to Arrow Schema."""
from dataschema import Schema_pb2, Schema
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Table,
    Date,
    DateTime,
    Integer,
    String,
)
from sqlalchemy.types import (
    ARRAY,
    DECIMAL,
    REAL,
    VARBINARY,
)
from sqlalchemy.sql.sqltypes import SmallInteger, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

_DIRECT_TYPE_MAPPING = {
    Schema_pb2.ColumnInfo.TYPE_BOOLEAN: Boolean,
    Schema_pb2.ColumnInfo.TYPE_INT_8: SmallInteger,
    Schema_pb2.ColumnInfo.TYPE_INT_16: Integer,
    Schema_pb2.ColumnInfo.TYPE_INT_32: Integer,
    Schema_pb2.ColumnInfo.TYPE_INT_64: BigInteger,
    Schema_pb2.ColumnInfo.TYPE_UINT_8: Integer,
    Schema_pb2.ColumnInfo.TYPE_UINT_16: Integer,
    Schema_pb2.ColumnInfo.TYPE_UINT_32: Integer,
    Schema_pb2.ColumnInfo.TYPE_UINT_64: Integer,
    Schema_pb2.ColumnInfo.TYPE_FLOAT_32: Float,
    Schema_pb2.ColumnInfo.TYPE_FLOAT_64: REAL,
    Schema_pb2.ColumnInfo.TYPE_DATE: Date,
    Schema_pb2.ColumnInfo.TYPE_DATETIME_64: DateTime,
    Schema_pb2.ColumnInfo.TYPE_STRING: String,
    Schema_pb2.ColumnInfo.TYPE_BYTES: VARBINARY,
    Schema_pb2.ColumnInfo.TYPE_NESTED: JSON,
    Schema_pb2.ColumnInfo.TYPE_MAP: JSON,
}


def _GetColumnType(column: Schema.Column, treat_repeated: bool = True):
    if column.info.column_type in [
            Schema_pb2.ColumnInfo.TYPE_ARRAY, Schema_pb2.ColumnInfo.TYPE_SET
    ]:
        return ARRAY(_GetColumnType(column.fields[0]))
    if treat_repeated and column.is_repeated():
        return ARRAY(_GetColumnType(column, False))
    if column.info.column_type == Schema_pb2.ColumnInfo.TYPE_DECIMAL:
        info = column.decimal_info()
        if info is None:
            raise ValueError(
                f'No decimal info for decimal column `{column.name()}`.')
        return DECIMAL(info.precision, info.scale)

    if column.info.column_type in _DIRECT_TYPE_MAPPING:
        return _DIRECT_TYPE_MAPPING[column.info.column_type]
    raise ValueError(f'Cannot convert type for column: {column}')


def ConvertColumn(column: Schema.Column, is_primary_key: bool = None):
    """Converts a python data Schema column to an arrow field."""
    if is_primary_key is None:
        is_primary_key = column.is_id()
    nullable = not column.is_required()
    return Column(column.info.name,
                  _GetColumnType(column),
                  nullable=nullable,
                  primary_key=is_primary_key)


def ConvertTable(table: Schema.Table):
    """Converts a python data Schema table to an arrow schema."""
    id_columns = {column.name() for column in table.columns if column.is_id()}
    if not id_columns:
        # sqlalchemy requires a primary key.
        id_columns = {column.name() for column in table.columns}
    columns = [
        ConvertColumn(column,
                      column.name() in id_columns) for column in table.columns
    ]
    return Table(table.info.name, Base.metadata, *columns)
