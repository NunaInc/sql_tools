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
"""Utility to parse a Hive/SparkSQL formatted sql file."""
from antlr4 import CommonTokenStream, InputStream
from sql_analyze.grammars.Hive import HiveLexer, HiveParser, statement_builder


def parse_hive_sql_tree(sql_statement: str):
    input_stream = InputStream(sql_statement)
    lexer = HiveLexer.HiveLexer(input_stream)
    stream = CommonTokenStream(lexer)
    parser = HiveParser.HiveParser(stream)
    return (parser.statements(), parser)


def parse_hive_sql_statement(sql_statement: str):
    tree, parser = parse_hive_sql_tree(sql_statement)
    visitor = statement_builder.StatementVisitor()
    visitor.visit(tree)
    return (tree, parser, visitor.statements)


def parse_hive_expression(sql_expression: str):
    input_stream = InputStream(sql_expression)
    lexer = HiveLexer.HiveLexer(input_stream)
    stream = CommonTokenStream(lexer)
    parser = HiveParser.HiveParser(stream)
    return (parser.expression(), parser)
