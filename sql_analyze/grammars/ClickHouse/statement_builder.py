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
"""Parses a Clickhouse SQL and builds a statement source tree."""

# pylint: disable=invalid-name
from antlr4.ParserRuleContext import ParserRuleContext
from sql_analyze.grammars import statement, tokens, trees
from sql_analyze.grammars.ClickHouse.ClickHouseParserVisitor import ClickHouseParserVisitor
from sql_analyze.grammars.ClickHouse.ClickHouseParser import ClickHouseParser
from sql_analyze.grammars.ClickHouse import create_table_schema

from typing import List, Optional


def _Error(ctx: ParserRuleContext, error: str):
    raise ValueError(f'Error: @{ctx.start.line}:{ctx.start.column}: {error}\n'
                     f'`{trees.recompose(ctx)}`\n')


class ExpressionVisitor(ClickHouseParserVisitor):
    """Visits a computational expression in a statement."""

    def __init__(self, query: statement.Source, expression: tokens.Tokens):
        self.query = query
        self.expression = expression
        self.sub_queries = []
        self.references = set()

    def source(self,
               name: Optional[str] = None,
               parent: Optional[statement.Source] = None):
        if not parent:
            parent = self.query
        exp = statement.Expression(parent, name, self.expression,
                                   self.references)
        for sub_query in self.sub_queries:
            sub_query.set_parent(exp)
        return exp

    def visitColumnIdentifier(self,
                              ctx: ClickHouseParser.ColumnIdentifierContext):
        self.references.add(trees.recompose(ctx))

    def visitNestedIdentifier(self,
                              ctx: ClickHouseParser.NestedIdentifierContext):
        self.references.add(trees.recompose(ctx))

    def visitIdentifier(self, ctx: ClickHouseParser.IdentifierContext):
        self.references.add(trees.recompose(ctx))

    def visitSelectUnionStmt(self,
                             ctx: ClickHouseParser.SelectUnionStmtContext):
        sub_visitor = SelectUnionVisitor(self.query)
        sub_visitor.visit(ctx)
        self.sub_queries.append(sub_visitor.query.set_limits(ctx))


class SelectUnionVisitor(ClickHouseParserVisitor):
    """Visitor for a UNION ALL composed Statement."""

    def __init__(self, parent: Optional[statement.Source] = None):
        self.query = None
        self.parent = parent

    def visitSelectStmt(self, ctx: ClickHouseParser.SelectStmtContext):
        if self.query:
            query = statement.SetOpQuery('UNION ALL', self.query)
            self.query.set_ops.append(query)
        else:
            query = statement.Query(self.parent)
            self.query = query
        select_visitor = SelectVisitor(query=query)
        select_visitor.visit(ctx)


def _NameFromAlias(ctx: ClickHouseParser.ColumnExprAliasContext):
    if ctx.alias():
        return trees.recompose(ctx.alias())
    elif ctx.identifier():
        return trees.recompose(ctx.identifier())
    return None


class ColumnExprInfo:
    """Information about a columnExpr - basically a SQL expression."""

    def __init__(self, query: statement.Query,
                 column: ClickHouseParser.ColumnExprContext,
                 extract_name: bool):
        self.column = column
        self.name: Optional[str] = None
        self.expression: Optional[ExpressionVisitor] = None
        self.column_expr: Optional[ClickHouseParser.ColumnExprContext] = None
        self.is_asterisk = False
        if isinstance(column, ClickHouseParser.ColumnExprAsteriskContext):
            if column.tableIderntifier():
                self.name = trees.recompose(column.tableIdentifier())
            self.is_asterisk = True
        else:
            self.column_expr = column
            if extract_name and isinstance(
                    column, ClickHouseParser.ColumnExprAliasContext):
                self.column_expr = column.columnExpr()
                self.name = _NameFromAlias(column)
            self.expression = ExpressionVisitor(
                query, tokens.from_tree(self.column_expr))
            self.expression.visit(self.column_expr)


def _ParseColumnExprList(query: statement.Query,
                         ctx: ClickHouseParser.ColumnExprListContext,
                         extract_name: bool) -> List[ColumnExprInfo]:
    return [
        ColumnExprInfo(query, column, extract_name)
        for column in ctx.columnsExpr()
    ]


class LimitByClause(statement.QueryClause):
    """A ClickHouse special - the LIMIT..BY clause."""

    def __init__(self, parent: Optional[statement.Source],
                 limit: statement.QueryLimit):
        super().__init__(parent, 'LIMIT BY')
        self.limit = limit

    def recompose(self, sep: str = ''):
        expressions = ', '.join(child.recompose(sep) for child in self.children)
        return f'{self.limit.recompose()} BY {expressions}'


class FinalDesignator(statement.Source):
    """A special attachement to a table in ClickHouse."""

    def __init__(self, parent: statement.Source):
        super().__init__(parent, 'FINAL')

    def is_sql_construct(self):
        return True

    def recompose(self, sep: str = ' '):
        del sep
        return 'FINAL'


class SampleClause(statement.Source):
    """A special attachement to a table in ClickHouse."""

    def __init__(self, parent: statement.Source, ratio: str,
                 offset: Optional[str]):
        super().__init__(parent, 'SAMPLE')
        self.ratio = ratio
        self.offset = offset

    def is_sql_construct(self):
        return True

    def name_str(self, stype: str = '', indent: str = '', ext: str = ''):
        del stype, ext
        return f'{indent}{self.recompose()}'

    def recompose(self, sep: str = ' '):
        del sep
        s = f'SAMPLE {self.ratio}'
        if self.offset:
            s += f' OFFSET {self.offset}'
        return s


def _ParseTableExpr(query: statement.Query,
                    ctx: ClickHouseParser.TableExprContext):
    if isinstance(ctx, ClickHouseParser.TableExprIdentifierContext):
        name = trees.recompose(ctx.tableIdentifier())
        return statement.Table(query, name, name)
    if isinstance(ctx, ClickHouseParser.TableExprSubqueryContext):
        sub_visitor = SelectUnionVisitor(query)
        sub_visitor.visit(ctx)
        return sub_visitor.query.set_limits(ctx)
    if isinstance(ctx, ClickHouseParser.TableExprAliasContext):
        base_source = _ParseTableExpr(query, ctx.tableExpr())
        if ctx.alias():
            base_source.name = trees.recompose(ctx.alias())
        elif ctx.identifier():
            base_source.name = trees.recompose(ctx.identifier())
        return base_source.set_limits(ctx.tableExpr())
    if isinstance(ctx, ClickHouseParser.TableExprFunctionContext):
        expr_ctx = ctx.tableFunctionExpr()
        # Treating table functions:
        # https://clickhouse.com/docs/en/sql-reference/table-functions/
        exp = ExpressionVisitor(query, tokens.from_tree(expr_ctx))
        exp.visit(expr_ctx)
        return exp.source().set_limits(expr_ctx)
    return _Error(ctx, 'Cannot find a way to process table expression')


def _GetConstraintJoinName(ctx: ClickHouseParser.JoinExprContext):
    join_name = []
    if ctx.GLOBAL():
        join_name.append('GLOBAL')
    elif ctx.LOCAL():
        join_name.append('LOCAL')
    if ctx.joinOp():
        join_name.append(trees.recompose(ctx.joinOp()).upper())
    join_name.append('JOIN')
    return ' '.join(join_name)


def _ParseJoinExpr(query: statement.Query,
                   ctx: ClickHouseParser.JoinExprContext):
    if isinstance(ctx, ClickHouseParser.JoinExprTableContext):
        table = _ParseTableExpr(query, ctx.tableExpr())
        if ctx.FINAL():
            table.add_lateral(FinalDesignator(table))
        sample = ctx.sampleClause()
        if sample:
            ratio = trees.recompose(sample.ratioExpr(0))
            offset = None
            if sample.ratioExpr(1):
                offset = trees.recompose(sample.ratioExpr(1))
            table.add_lateral(SampleClause(table, ratio, offset))
        return table
    if isinstance(ctx, ClickHouseParser.JoinExprParensContext):
        return _ParseJoinExpr(query, ctx.joinExpr())
    source = statement.JoinSource(query).set_limits(ctx)
    src_left = _ParseJoinExpr(source, ctx.joinExpr(0))
    source.add_source(src_left)
    src_right = _ParseJoinExpr(source, ctx.joinExpr(1))
    if isinstance(ctx, ClickHouseParser.JoinExprCrossOpContext):
        join_expr = statement.JoinExpression.naked(
            trees.recompose(ctx.joinOpCross())).set_limits(ctx.joinOpCross())
    else:
        constr = ctx.joinConstraintClause()
        if not constr:
            return _Error(ctx, 'Join operation without constraint')
        join_name = _GetConstraintJoinName(ctx)
        exp_ctx = constr.columnExprList()
        if constr.ON():
            # Note the syntax allows for multiple expressions, and any
            # kind of expressions which is actually not legal in ClickHouse,
            # but we just go w/ the flow.
            exp = ExpressionVisitor(query, tokens.from_tree(exp_ctx))
            exp.visit(exp_ctx)
            join_expr = statement.JoinExpression.on_expression(
                join_name,
                exp.source(parent=source).set_limits(exp_ctx))
        elif constr.USING():
            # Same here, the columns can be any kind of expressions, but we don't
            # really do too much in cleaning up.
            columns = [
                statement.Column(source,
                                 trees.recompose(column)).set_limits(column)
                for column in exp_ctx.columnsExpr()
            ]
            join_expr = statement.JoinExpression.using_columns(
                join_name, columns).set_limits(exp_ctx)
    source.add_source(src_right, join_expr)
    return source


class SelectVisitor(ClickHouseParserVisitor):
    """Processes a select Statement."""

    def __init__(self,
                 parent: Optional[statement.Source] = None,
                 name: Optional[str] = None,
                 query: Optional[statement.Query] = None):
        super().__init__()
        if query:
            self.query = query
        else:
            self.query = statement.Query(parent, name)

    def visitSelectStmt(self, ctx: ClickHouseParser.SelectStmtContext):
        kws = []
        if ctx.DISTINCT():
            kws.append('DISTINCT')
        if ctx.topClause():
            top_clause = trees.recompose(ctx.topClause())
            kws.append(top_clause)
        if kws:
            self.query.select_items.keyword = ' '.join(kws)
        columns = ctx.columnExprList()
        self.query.select_items.set_limits(columns)
        for column in _ParseColumnExprList(self.query, columns, True):
            if column.is_asterisk:
                self.query.select_items.add_all_columns(column.name).set_limits(
                    column.column)
            else:
                self.query.select_items.add_expression_column(
                    column.expression.source(name=column.name).set_limits(
                        column.column_expr)).set_limits(column.column)
        return self.visitChildren(ctx)

    def visitWithClause(self, ctx: ClickHouseParser.WithClauseContext):
        for with_expr in ctx.withExprList().withExpr():
            name = trees.recompose(with_expr.identifier())
            if with_expr.selectUnionStmt():
                visitor = SelectUnionVisitor(self.query)
                visitor.visit(with_expr.selectUnionStmt())
                visitor.query.name = name
                visitor.query.set_limits(with_expr.selectUnionStmt())
                self.query.withs.append(visitor.query)
            elif with_expr.columnExpr():
                visitor = ExpressionVisitor(
                    self.query, tokens.from_tree(with_expr.columnExpr()))
                visitor.visit(with_expr.columnExpr())
                self.query.withs.append(
                    visitor.source(name=name).set_limits(with_expr))
            else:
                return _Error(with_expr, 'Invalid with expression.')

    def visitWhereClause(self, ctx: ClickHouseParser.WhereClauseContext):
        exp_ctx = ctx.columnExpr()
        exp = ExpressionVisitor(self.query, tokens.from_tree(exp_ctx))
        exp.visit(exp_ctx)
        self.query.where = exp.source().set_limits(ctx)
        self.query.where.role = 'WHERE'

    def visitPreWhereClause(self, ctx: ClickHouseParser.PrewhereClauseContext):
        pass

    def visitGroupByClause(self, ctx: ClickHouseParser.GroupByClauseContext):
        for column in _ParseColumnExprList(self.query, ctx.columnExprList(),
                                           False):
            if column.is_asterisk:
                return _Error(ctx, 'Invalid asterisk group by.')
            self.query.add_group_by(
                exp=column.expression.source().set_limits(column.column))
        if ctx.CUBE() is not None:
            self.query.group_by.keyword = 'CUBE'
        elif ctx.ROLLUP() is not None:
            self.query.group_by.keyword = 'ROLLUP'
        if ctx.LPAREN():
            self.query.group_by.with_parens = True
        keywords = []
        if ctx.groupByRollupClause():
            keywords.append(trees.recompose(ctx.groupByRollupClause()).upper())
        if ctx.groupByTotalsClause():
            keywords.append(trees.recompose(ctx.groupByTotalsClause()).upper())
        if keywords:
            self.query.ensure_group_by().post_keyword = ' '.join(keywords)

    def visitHavingClause(self, ctx: ClickHouseParser.HavingClauseContext):
        exp_ctx = ctx.columnExpr()
        exp = ExpressionVisitor(self.query, tokens.from_tree(exp_ctx))
        exp.visit(exp_ctx)
        self.query.having = exp.source().set_limits(exp_ctx)
        self.query.having.role = 'HAVING'

    def visitFromClause(self, ctx: ClickHouseParser.FromClauseContext):
        self.query.source = _ParseJoinExpr(self.query, ctx.joinExpr())

    def visitArrayJoinClause(self,
                             ctx: ClickHouseParser.ArrayJoinClauseContext):
        if not self.query.source:
            return _Error(ctx, 'Array join clause present, without a source')
        # We create a query clause that plays lateral on the source
        name = 'ARRAY JOIN'
        if ctx.LEFT():
            name = f'LEFT {name}'
        elif ctx.INNER():
            name = f'INNER {name}'
        clause = statement.QueryClause(self.query.source, name)
        for column in _ParseColumnExprList(self.query, ctx.columnExprList(),
                                           False):
            clause.add_child(
                column.expression.source(parent=clause).set_limits(
                    column.column))
        self.query.source.add_lateral(clause)

    def visitOrderByClause(self, ctx: ClickHouseParser.OrderByClauseContext):
        order_clause = statement.QueryClause(self.query,
                                             'ORDER BY').set_limits(ctx)
        for order_expr in ctx.orderExprList().orderExpr():
            exp = ExpressionVisitor(self.query,
                                    tokens.from_tree(order_expr.columnExpr()))
            exp.visit(order_expr.columnExpr())
            exp_statement = exp.source(
                parent=order_clause).set_limits(order_expr)
            decoration = trees.recompose(order_expr.orderExprDecoration())
            if decoration:
                exp_statement.attributes.append(decoration)
        self.query.clauses.append(order_clause)

    def _build_query_limit(self, ctx: ClickHouseParser.LimitExprContext):
        limit_expr = ctx.columnExpr(0)
        offset_expr = ctx.columnExpr(1)
        if ctx.OFFSET() and offset_expr:
            return statement.QueryLimit.with_offset(
                trees.recompose(limit_expr), trees.recompose(offset_expr))
        elif offset_expr is not None:
            return statement.QueryLimit.with_numbers(
                [trees.recompose(limit_expr),
                 trees.recompose(offset_expr)])
        else:
            return statement.QueryLimit.with_numbers(
                [trees.recompose(limit_expr)])

    def visitLimitByClause(self, ctx: ClickHouseParser.LimitByClauseContext):
        limit = self._build_query_limit(ctx.limitExpr())
        clause = LimitByClause(self.query, limit)
        for column in _ParseColumnExprList(self.query, ctx.columnExprList(),
                                           False):
            if column.is_asterisk:
                return _Error(ctx, 'Invalid asterisk limit by.')
            clause.add_child(
                column.expression.source(parent=clause).set_limits(
                    column.column))
        limit.name = f'{limit.recompose()} BY'
        self.query.clauses.append(clause)

    def visitLimitClause(self, ctx: ClickHouseParser.LimitClauseContext):
        clause = self._build_query_limit(ctx.limitExpr())
        if ctx.WITH() and ctx.TIES():
            clause.postfix = 'WITH TIES'
        self.query.limit = clause

    def visitSettingsClause(self, ctx: ClickHouseParser.SettingsClauseContext):
        clause = statement.QueryClause(self.query, 'SETTINGS').set_limits(ctx)
        for exp_ctx in ctx.settingExprList().settingExpr():
            exp = ExpressionVisitor(self.query, tokens.from_tree(exp_ctx))
            exp.visit(exp_ctx)
            clause.add_child(exp.source(parent=clause).set_limits(exp_ctx))
        self.query.clause.append(clause)


class QueryStmtVisitor(ClickHouseParserVisitor):
    """Processes the top level statement."""

    def __init__(self, java_package_name: Optional[str] = None):
        self.statements = []
        self.java_package_name = java_package_name

    def _build_query(self, ctx: ClickHouseParser.SelectUnionStmtContext):
        select = SelectUnionVisitor()
        select.visit(ctx)
        return select.query.set_limits(ctx)

    def visitQuery(self, ctx: ClickHouseParser.QueryContext):
        if ctx.selectUnionStmt() is not None:
            self.statements.append(self._build_query(ctx.selectUnionStmt()))
        if ctx.createStmt() is not None:
            self.visitCreateStmt(ctx.createStmt())

    def visitInsertStmt(self, ctx: ClickHouseParser.InsertStmtContext):
        if not isinstance(ctx.dataClause(),
                          ClickHouseParser.DataClauseSelectContext):
            return
        sname = 'INSERT INTO'
        if ctx.TABLE():
            sname += ' TABLE'
        query = self._build_query(ctx.dataClause().selectUnionStmt())
        if ctx.tableIdentifier():
            tid = ctx.tableIdentifier()
            name = trees.recompose(tid)
            query.destination = statement.Table(
                parent=None, name=name, original_name=name).set_limits(tid)
            query.name = name
        elif ctx.FUNCTION():
            sname += ' FUNCTION'
            expr = ctx.tableFunctionExpr()
            exp = ExpressionVisitor(query, tokens.from_tree(expr))
            exp.visit(expr)
            query.destination = exp.source().set_limits(expr)
        insert = statement.Insert(parent=None,
                                  name=sname,
                                  destination=query.destination,
                                  statement_tokens=tokens.from_tree(ctx),
                                  query=query).set_limits(ctx)
        self.statements.append(insert)

    def _visit_subquery(self, ctx: ClickHouseParser.SubqueryClauseContext):
        select = SelectUnionVisitor()
        select.visit(ctx.selectUnionStmt())
        return select.query.set_limits(ctx)

    def visitCreateStmt(self, ctx: ClickHouseParser.CreateStmtContext):
        if isinstance(ctx, ClickHouseParser.CreateDatabaseStmtContext):
            pass
        elif isinstance(ctx, ClickHouseParser.CreateDictionaryStmtContext):
            pass
        elif isinstance(ctx,
                        (ClickHouseParser.CreateLiveViewStmtContext,
                         ClickHouseParser.CreateMaterializedViewStmtContext,
                         ClickHouseParser.CreateViewStmtContext)):
            engine_clause = None
            if isinstance(ctx, ClickHouseParser.CreateLiveViewStmtContext):
                cname = 'CREATE LIVE VIEW'
            elif isinstance(ctx,
                            ClickHouseParser.CreateMaterializedViewStmtContext):
                cname = 'CREATE MATERIALIZED VIEW'
                engine_clause = ctx.engineClause()
            else:
                cname = 'CREATE VIEW'
            query = self._visit_subquery(ctx.subqueryClause())
            tid = ctx.tableIdentifier()
            tname = trees.recompose(tid)
            query.destination = statement.View(parent=None,
                                               name=tname,
                                               original_name=tname)
            query.name = tname
            if ctx.tableSchemaClause():
                schema = create_table_schema.ClickHouseConvertTable(
                    trees.recompose(ctx), ctx.tableSchemaClause(),
                    engine_clause, tname, self.java_package_name)
                query.destination.schema = schema
            else:
                schema = None
            self.statements.append(
                statement.Create(None, cname, query.destination,
                                 tokens.from_tree(ctx), query,
                                 schema).set_limits(ctx))
        elif isinstance(ctx, ClickHouseParser.CreateTableStmtContext):
            tid = ctx.tableIdentifier()
            tname = trees.recompose(tid)
            destination = statement.Table(parent=None,
                                          name=tname,
                                          original_name=tname)
            if ctx.subqueryClause():
                query = self._visit_subquery(ctx.subqueryClause())
                query.destination = destination
                query.name = tname
            else:
                query = None
            if ctx.tableSchemaClause():
                schema = create_table_schema.ClickHouseConvertTable(
                    trees.recompose(ctx), ctx.tableSchemaClause(),
                    ctx.engineClause(), tname, self.java_package_name)
                destination.schema = schema
            else:
                schema = None
            self.statements.append(
                statement.Create(None, 'CREATE TABLE', destination,
                                 tokens.from_tree(ctx), query,
                                 schema).set_limits(ctx))


class MultiQueryStmtVisitor(ClickHouseParserVisitor):
    """Processes multiple query statements."""

    def __init__(self, java_package_name: Optional[str] = None):
        self.statements = []
        self.java_package_name = java_package_name

    def visitQueryStmt(self, ctx: ClickHouseParser.QueryStmtContext):
        visitor = QueryStmtVisitor(self.java_package_name)
        visitor.visit(ctx)
        self.statements.extend(visitor.statements)
