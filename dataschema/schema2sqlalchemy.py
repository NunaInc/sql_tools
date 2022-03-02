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
"""Converts python data schema to Sql Alchemy schema.

We use only base Alchemy types, and we use JSON for
composed data types like map or nested.
"""
from dataschema import Schema_pb2, Schema
from sqlalchemy.types import (ARRAY, BOOLEAN, DATE, DATETIME, DECIMAL, FLOAT,
                              INTEGER, JSON, REAL, SMALLINT, VARCHAR, VARBINARY)
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from typing import Optional

Base = declarative_base()

# There may be a discussion if to use INTEGER of BIGINT for 64 bit integers here,
# as SqlAlchemy makes no comment about their size. However based on the conversions
# none in other places (e.g. the Sql Alchemy adapter for ClickHouse), decided to
# go with normal INTEGER.
_DIRECT_TYPE_MAPPING = {
    Schema_pb2.ColumnInfo.TYPE_BOOLEAN: BOOLEAN,
    Schema_pb2.ColumnInfo.TYPE_INT_8: SMALLINT,
    Schema_pb2.ColumnInfo.TYPE_INT_16: SMALLINT,
    Schema_pb2.ColumnInfo.TYPE_INT_32: INTEGER,
    Schema_pb2.ColumnInfo.TYPE_INT_64: INTEGER,
    Schema_pb2.ColumnInfo.TYPE_UINT_8: SMALLINT,
    Schema_pb2.ColumnInfo.TYPE_UINT_16: SMALLINT,
    Schema_pb2.ColumnInfo.TYPE_UINT_32: INTEGER,
    Schema_pb2.ColumnInfo.TYPE_UINT_64: INTEGER,
    Schema_pb2.ColumnInfo.TYPE_FLOAT_32: FLOAT,
    Schema_pb2.ColumnInfo.TYPE_FLOAT_64: REAL,
    Schema_pb2.ColumnInfo.TYPE_DATE: DATE,
    Schema_pb2.ColumnInfo.TYPE_DATETIME_64: DATETIME,
    Schema_pb2.ColumnInfo.TYPE_DECIMAL: DECIMAL,
    Schema_pb2.ColumnInfo.TYPE_STRING: VARCHAR,
    Schema_pb2.ColumnInfo.TYPE_BYTES: VARBINARY,
    Schema_pb2.ColumnInfo.TYPE_NESTED: JSON,
    Schema_pb2.ColumnInfo.TYPE_MAP: JSON,
}


def _GetColumnType(column: Schema.Column, treat_repeated: bool = True):
    if column.info.column_type in (Schema_pb2.ColumnInfo.TYPE_ARRAY,
                                   Schema_pb2.ColumnInfo.TYPE_SET):
        return ARRAY(_GetColumnType(column.fields[0]))
    if treat_repeated and column.is_repeated():
        return ARRAY(_GetColumnType(column, False))
    if column.info.column_type == Schema_pb2.ColumnInfo.TYPE_DECIMAL:
        info = column.decimal_info()
        return DECIMAL(info.precision, info.scale)
    if column.info.column_type in _DIRECT_TYPE_MAPPING:
        return _DIRECT_TYPE_MAPPING[column.info.column_type]
    raise ValueError(f'Cannot convert type {column.info.column_type} '
                     f'for column: {column}')


def ConvertColumn(column: Schema.Column, is_primary_key: bool = None) -> Column:
    """Converts a python data Schema column to an arrow field."""
    column.validate()
    if is_primary_key is None:
        is_primary_key = column.is_id()
    nullable = False
    if not column.is_required():
        nullable = True
    elif column.data_annotation.dq_field.is_nullable:
        nullable = True
    return Column(column.sql_name(),
                  _GetColumnType(column),
                  nullable=nullable,
                  primary_key=is_primary_key)


def ConvertTable(table: Schema.Table,
                 table_name: Optional[str] = None,
                 meta: Optional[MetaData] = None) -> Table:
    """Converts a python data Schema table to an arrow schema."""
    id_columns = {
        column.sql_name() for column in table.columns if column.is_id()
    }
    # sqlalchemy requires a primary key.
    if not id_columns:
        id_columns = {column.sql_name() for column in table.columns}
    columns = [
        ConvertColumn(column,
                      column.sql_name() in id_columns)
        for column in table.columns
    ]
    if meta is None:
        meta = Base.metadata
    if table_name is None:
        table_name = table.info.name
    return Table(table_name, meta, *columns)
