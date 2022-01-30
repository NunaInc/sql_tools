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
"""Converts Schema to DBML definition."""

from dataschema import Schema, Schema_pb2, schema2sql
from typing import List, Optional


def GetIndent(indent: int) -> str:
    return schema2sql.GetIndent(indent)


# DBML accepts any type name - so we use the Clickhouse types.
DBML_TYPE_NAME = schema2sql.CLICKHOUSE_TYPE_NAME


class TableConverter:
    """Converts a schema Table to a DBLM table definition."""

    def __init__(self, table: Schema.Table):
        self.table = table

    def _get_decimal_str(self, column: Schema.Column) -> str:
        info = column.decimal_info()
        if info is None:
            raise ValueError(
                f'No decimal info for decimal column `{column.name()}`.')
        return f'Decimal({info.precision}, {info.scale})'

    def _column_to_dbml(self,
                        column: Schema.Column,
                        indent: int,
                        type_only: bool,
                        is_primary_key: bool = False) -> str:
        """Returns a DBML column specification for `column`."""
        s = ''
        if not type_only:
            s += f'{GetIndent(indent)}{column.name()} '
        end = ''
        notes = []
        column_type = column.info.column_type
        if (column.info.label == Schema_pb2.ColumnInfo.LABEL_REPEATED and
                column_type != Schema_pb2.ColumnInfo.TYPE_MAP):
            s += 'Array('
            end += ')'
        if column.is_low_cardinality():
            s += 'LowCardinality('
            end += ')'
        if column.info.label == Schema_pb2.ColumnInfo.LABEL_REQUIRED:
            notes.append('not null')
        if column_type == Schema_pb2.ColumnInfo.TYPE_MAP:
            ktype = self._column_to_dbml(column.fields[0], 0, type_only=True)
            vtype = self._column_to_dbml(column.fields[1], 0, type_only=True)
            s += f'Map({ktype}, {vtype})'
        elif column_type in [
                Schema_pb2.ColumnInfo.TYPE_ARRAY, Schema_pb2.ColumnInfo.TYPE_SET
        ]:
            s += self._column_to_dbml(column.fields[0], 0, type_only=True)
        elif column_type == Schema_pb2.ColumnInfo.TYPE_DECIMAL:
            s += self._get_decimal_str(column)
        else:
            if column_type not in DBML_TYPE_NAME:
                raise KeyError(f'Unknown type: {column_type}')
            s += DBML_TYPE_NAME[column_type]
            if column_type == Schema_pb2.ColumnInfo.TYPE_DECIMAL:
                s += self._get_decimal_str(column)
            elif column_type == Schema_pb2.ColumnInfo.TYPE_NESTED:
                sub_columns = []
                for sub_column in column.fields:
                    sub_columns.append(
                        self._column_to_dbml(sub_column,
                                             indent + 2,
                                             type_only=False))
                sub_columns_str = ',\n'.join(sub_columns)
                s += f'(\n{sub_columns_str}\n{GetIndent(indent)})'
        s += end
        if is_primary_key:
            notes.append('primary key')
        if notes:
            s += f' [{", ".join(notes)}]'
        return s

    def columns_dbml(self, indent: int) -> List[str]:
        """Returns a list of DBML column specifications."""
        columns = []
        id_count = sum(column.is_id() for column in self.table.columns)
        for column in self.table.columns:
            columns.append(
                self._column_to_dbml(column,
                                     indent,
                                     type_only=False,
                                     is_primary_key=(id_count == 1 and
                                                     column.is_id())))
        return columns

    def to_dbml(self, table_name: Optional[str] = None) -> str:
        """Returns a DBLM snippet for this table."""
        if not table_name:
            table_name = self.table.name()
        columns_str = '\n'.join(self.columns_dbml(4))
        s = f'Table {table_name} {{\n{columns_str}\n'
        # TODO(cp): disabled for now, until the display supports it.
        # id_columns = [
        #     column.name() for column in self.table.columns if column.is_id()
        # ]
        # if len(id_columns) > 1:
        #    s += ('    indexes {\n'
        #          f'        ({", ".join(id_columns)}) [primary key]\n'
        #          '    }\n')
        s += '}\n'
        return s


def ConvertTable(table: Schema.Table, table_name: Optional[str] = None):
    """Converts schema table to DBML."""
    return TableConverter(table).to_dbml(table_name)
