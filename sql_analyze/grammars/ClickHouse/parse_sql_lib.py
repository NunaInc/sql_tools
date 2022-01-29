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
"""Utility to parse a Clickhouse formatted sql file."""
from antlr4 import CommonTokenStream, InputStream
from sql_analyze.grammars.ClickHouse import (ClickHouseLexer, ClickHouseParser,
                                             statement_builder)
from typing import Optional


def parse_clickhouse_sql_tree(sql_statement: str):
    """Creates a parse tree from the provided (multi) SQL statement."""
    input_stream = InputStream(sql_statement)
    lexer = ClickHouseLexer.ClickHouseLexer(input_stream)
    stream = CommonTokenStream(lexer)
    parser = ClickHouseParser.ClickHouseParser(stream)
    return (parser.multiQueryStmt(), parser)


def parse_clickhouse_sql_statement(sql_statement: str):
    """Parses a list of statements from the provided (multi) SQL statement."""
    tree, parser = parse_clickhouse_sql_tree(sql_statement)
    visitor = statement_builder.MultiQueryStmtVisitor()
    visitor.visit(tree)
    return (tree, parser, visitor.statements)


def parse_clickhouse_sql_create(sql_statement: str,
                                java_package_name: Optional[str] = None):
    """Parses a create statement from sql_statement text, and returns it."""
    input_stream = InputStream(sql_statement)
    lexer = ClickHouseLexer.ClickHouseLexer(input_stream)
    stream = CommonTokenStream(lexer)
    parser = ClickHouseParser.ClickHouseParser(stream)
    tree = parser.createStmt()
    visitor = statement_builder.QueryStmtVisitor(java_package_name)
    visitor.visitCreateStmt(tree)
    if visitor.statements:
        return visitor.statements[0]
    return None


def parse_clickhouse_expression(sql_expression: str):
    """Creates a parse tree from a SQL expression."""
    input_stream = InputStream(sql_expression)
    lexer = ClickHouseLexer.ClickHouseLexer(input_stream)
    stream = CommonTokenStream(lexer)
    parser = ClickHouseParser.ClickHouseParser(stream)
    return (parser.expression(), parser)
