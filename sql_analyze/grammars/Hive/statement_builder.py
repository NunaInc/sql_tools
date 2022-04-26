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
"""Parses a Hive SQL and builds a statement source tree."""
# pylint: disable=invalid-name
from antlr4.ParserRuleContext import ParserRuleContext
from sql_analyze.grammars import statement, tokens, trees
from sql_analyze.grammars.Hive.HiveParserVisitor import HiveParserVisitor
from sql_analyze.grammars.Hive.HiveParser import HiveParser
from typing import Optional, List, Union


def _GetIdentifier(ctx: Union[HiveParser.IdentifierContext,
                              HiveParser.NonReservedColumnNameContext]):
    if ctx is None:
        return None
    if isinstance(ctx, HiveParser.IdentifierContext):
        if ctx.Identifier() is not None:
            return ctx.Identifier().getPayload().text
        if ctx.nonReserved() is not None:
            return ctx.nonReserved().getChild(0).getPayload().text
    elif isinstance(ctx, HiveParser.NonReservedColumnNameContext):
        return ctx.getChild(0).getPayload().text
    return None


def _Error(ctx: ParserRuleContext, error: str):
    raise ValueError(f'Error: @{ctx.start.line}:{ctx.start.column}: {error}\n'
                     f'`{trees.recompose(ctx)}`\n')


def _AtomTableName(ctx: HiveParser.AtomExpressionContext):
    if ctx.tableOrColumn() is None:
        return None
    return _GetIdentifier(ctx.tableOrColumn().identifier())


def GetComposedIdentifier(ctx: ParserRuleContext):
    if isinstance(ctx, HiveParser.IdentifierContext):
        return _GetIdentifier(ctx)
    if not isinstance(ctx, HiveParser.ExpressionContext):
        return None
    n = ctx.getChildCount()
    if n % 2 != 1:
        return None
    if not ctx.atomExpression():
        return None
    s = _AtomTableName(ctx.atomExpression())
    if not s:
        return None
    ids = [s]
    # Expect id1.id2...idN - look for dots in right positions:
    ndots = int(n / 2)
    nids = int((n + 1) / 2) - 1
    for i in range(ndots):
        if not ctx.DOT(i):
            return None
    for i in range(nids):
        s = _GetIdentifier(ctx.identifier(i))
        if s is None:
            s = _GetIdentifier(ctx.nonReservedColumnName(i))
            return None
        ids.append(s)
    return '.'.join(ids)


def GetColumns(source: statement.Source,
               ctx: HiveParser.ColumnNameListContext) -> List[statement.Column]:
    return [
        statement.Column(source,
                         _GetIdentifier(column.identifier())).set_limits(column)
        for column in ctx.columnName()
    ]


class WithVisitor(HiveParserVisitor):
    """Visits WITH part in a select statement."""

    def __init__(self, query: statement.Query):
        self.query = query
        self.identifier = None

    def visitIdentifier(self, ctx: HiveParser.IdentifierContext):
        self.identifier = _GetIdentifier(ctx)

    def visitQueryStatementExpression(
            self, ctx: HiveParser.QueryStatementExpressionContext):
        if not self.identifier:
            return _Error(ctx, 'WITH query does not include an identifier')
        with_statement = QueryVisitor(self.query, self.identifier)
        with_statement.visit(ctx)
        self.query.withs.append(with_statement.query.set_limits(ctx))
        self.identifier = None


def _ExtractExpression(ctx: HiveParser.ExpressionsContext):
    if ctx.expressionsInParenthesis() is not None:
        exps = ctx.expressionsInParenthesis().expressionsNotInParenthesis()
    elif ctx.expressionsNotInParenthesis() is not None:
        exps = ctx.expressionsNotInParenthesis()
    else:
        return _Error(ctx, 'Expressions missing parts')
    result = [exps.expression()]
    if exps.expressionPart():
        result.extend(exps.expressionPart().expression())
    return result


class ExpressionVisitor(HiveParserVisitor):
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

    def visitExpression(self, ctx: HiveParser.ExpressionsContext):
        if ctx.atomExpression() is not None:
            identifier = GetComposedIdentifier(ctx)
            if identifier is not None:
                self.references.add(identifier)
                return
        elif ctx.subQueryExpression() is not None:
            self.visitSubQueryExpression(ctx.subQueryExpression())
            return
        super().visitExpression(ctx)

    def visitSubQueryExpression(self,
                                ctx: HiveParser.SubQueryExpressionContext):
        sub_query = statement.Query(self.query)
        select_visitor = SelectVisitor(sub_query)
        select_visitor.visit(ctx)
        # self.references.update(sub_query.get_all_references())
        self.sub_queries.append(sub_query.set_limits(ctx))


def _GetSubQuerySource(query: statement.Query,
                       ctx: HiveParser.SubQuerySourceContext):
    name = None
    if ctx.identifier():
        name = _GetIdentifier(ctx.identifier())
    sub_query = QueryVisitor(query, name)
    sub_query.visit(ctx.queryStatementExpression())
    return sub_query.query.set_limits(ctx.queryStatementExpression())


def _GetTableName(ctx: HiveParser.TableNameContext):
    name = _GetIdentifier(ctx.identifier(0))
    if ctx.identifier(1) is None:
        return name
    return f'{name}.{_GetIdentifier(ctx.identifier(1))}'


def _GetTableSource(query: statement.Query, ctx: HiveParser.TableSourceContext):
    name = _GetTableName(ctx.tableName())
    if ctx.identifier() is not None:
        alias = _GetIdentifier(ctx.identifier())
    else:
        alias = name
    return statement.Table(query, alias, name).set_limits(ctx)


def _ProcessLateralViewWithView(source: statement.Source,
                                ctx: HiveParser.LateralViewWithViewContext):
    name = None
    if ctx.tableAlias() is not None:
        name = trees.recompose(ctx.tableAlias())
    keyword = 'LATERAL VIEW'
    if ctx.KW_OUTER() is not None:
        keyword += ' OUTER'
    view = statement.LateralView(source, name, keyword,
                                 tokens.from_tree(
                                     ctx.function_())).set_limits(ctx)
    if ctx.aliasList():
        view.columns = [
            statement.Column(view,
                             _GetIdentifier(identifier)).set_limits(identifier)
            for identifier in ctx.aliasList().identifier()
        ]
    source.add_lateral(view)


def _ProcessLateralViewWithTable(source: statement.Source,
                                 ctx: HiveParser.LateralViewWithTableContext):
    name = None
    if ctx.tableAlias():
        name = trees.recompose(ctx.tableAlias())
    view = statement.LateralView(source, name, 'LATERAL TABLE',
                                 tokens.from_tree(
                                     ctx.valuesClause())).set_limits(ctx)
    if ctx.aliasList():
        view.columns = [
            statement.Column(view,
                             _GetIdentifier(identifier)).set_limits(identifier)
            for identifier in ctx.aliasList().identifier()
        ]
    source.add_lateral(view)


def _ProcessPivotView(query: statement.Query, source: statement.Source,
                      ctx: HiveParser.PivotViewContext):
    view = statement.PivotView(source).set_limits(ctx)
    for as_exp in ctx.asExpression():
        name = None
        if as_exp.identifier():
            name = _GetIdentifier(as_exp.identifier())
        exp = ExpressionVisitor(query, tokens.from_tree(as_exp.expression()))
        exp.visit(as_exp.expression())
        view.pivots.append(
            exp.source(name=name, parent=view).set_limits(as_exp))
    view.columns_source = [
        statement.Column(view,
                         _GetIdentifier(column.identifier())).set_limits(column)
        for column in ctx.columnNameList().columnName()
    ]
    for as_exp in ctx.multiNamedExpression().asExpression():
        name = None
        if as_exp.identifier():
            name = _GetIdentifier(as_exp.identifier())
        exp = ExpressionVisitor(query, tokens.from_tree(as_exp.expression()))
        exp.visit(as_exp.expression())
        view.columns_expressions.append(
            exp.source(name=name, parent=view).set_limits(as_exp))
    if view.pivots:
        for pivot in view.pivots:
            for exp in view.columns_expressions:
                if exp.name and pivot.name:
                    view.columns.append(
                        statement.Column(
                            view, f'{exp.name}_{pivot.name}').set_limits(exp))
    else:
        for exp in view.columns_expressions:
            if exp.name:
                view.columns.append(
                    statement.Column(view, exp.name).set_limits(exp))
    source.add_lateral(view)


def _ProcessLateralView(query: statement.Query, source: statement.Source,
                        ctx: HiveParser.LateralOrPivotViewContext):
    if ctx.lateralViewWithView() is not None:
        _ProcessLateralViewWithView(source, ctx.lateralViewWithView())
    elif ctx.lateralViewWithTable() is not None:
        _ProcessLateralViewWithTable(source, ctx.lateralViewWithTable())
    elif ctx.pivotView() is not None:
        _ProcessPivotView(query, source, ctx.pivotView())


def _ProcessAddJoinSource(
        query: statement.Query, parent: statement.Source,
        join_token: HiveParser.JoinTokenContext,
        join_source: HiveParser.JoinSourcePartContext,
        expression: Optional[HiveParser.ExpressionContext],
        columns: Optional[HiveParser.ColumnParenthesesListContext]):
    join_name = trees.recompose(join_token)
    if join_source.partitionedTableFunction() is not None:
        return _Error(join_source, 'Partitioned Table Source not supported')
    if join_source.virtualTableSource() is not None:
        return _Error(join_source, 'Virtual Table Source not supported')
    if join_source.tableSource() is not None:
        source = _GetTableSource(parent, join_source.tableSource())
    elif join_source.subQuerySource() is not None:
        source = _GetSubQuerySource(parent, join_source.subQuerySource())
    else:
        return _Error(join_source, 'Cannot determine the join source.')
    if expression:
        exp = ExpressionVisitor(query, tokens.from_tree(expression))
        exp.visit(expression)
        join_expression = statement.JoinExpression.on_expression(
            join_name,
            exp.source(
                parent=parent).set_limits(expression)).set_limits(expression)
    elif columns:
        join_expression = statement.JoinExpression.using_columns(
            join_name, GetColumns(parent,
                                  columns.columnNameList())).set_limits(columns)
    else:
        join_expression = statement.JoinExpression.naked(join_name).set_limits(
            join_token)
    return (source, join_expression)


def _ProcessJoinSource(query: statement.Query,
                       ctx: HiveParser.JoinSourceContext):
    atom = ctx.atomjoinSource()
    if atom is None:
        return _Error(ctx, 'Source missing actual source atom.')
    if atom.partitionedTableFunction() is not None:
        return _Error(atom, 'Partitioned Table Source not supported')
    if atom.virtualTableSource() is not None:
        return _Error(atom, 'Virtual Table Source not supported')
    parent = query
    is_join = False
    if ctx.joinToken():
        parent = statement.JoinSource(query).set_limits(ctx)
        is_join = True
    if atom.joinSource() is not None:
        source = _ProcessJoinSource(parent, atom.joinSource())
    elif atom.subQuerySource() is not None:
        source = _GetSubQuerySource(parent, atom.subQuerySource())
    elif atom.tableSource() is not None:
        source = _GetTableSource(parent, atom.tableSource())
    else:
        return _Error(atom, 'Cannot determine join source')
    source.set_limits(atom)
    for lateral in atom.lateralOrPivotView():
        _ProcessLateralView(query, source, lateral)
    if not is_join:
        return source

    parent.add_source(source)
    index = 0
    while ctx.joinToken(index) is not None:
        (source,
         joiner) = _ProcessAddJoinSource(query, parent, ctx.joinToken(index),
                                         ctx.joinSourcePart(index),
                                         ctx.expression(index),
                                         ctx.columnParenthesesList(index))
        assert isinstance(source, statement.Source)
        parent.add_source(source, joiner)
        index += 1
    return parent


class AtomSelectVisitor(HiveParserVisitor):
    """Processes an atomSelectStatement parse tree."""

    def __init__(self, query: statement.Source):
        self.query = query

    def visitSelectClause(self, ctx: HiveParser.SelectClauseContext):
        if ctx.selectTrfmClause() is not None or ctx.trfmClause() is not None:
            return _Error(ctx, 'Transformation clauses not supported in SELECT')
        select_list = ctx.selectList()
        if select_list is None:
            return _Error(ctx, 'Expecting selection list in SELECT')
        self.query.select_items.set_limits(select_list)
        if ctx.KW_ALL():
            self.query.select_items.keyword = 'ALL'
        elif ctx.KW_DISTINCT():
            self.query.select_items.keyword = 'DISTINCT'
        n = select_list.getChildCount()
        nitems = int((n + 1) / 2)
        ncommas = int(n / 2)
        for i in range(0, ncommas):
            if select_list.COMMA(i) is None:
                return _Error(select_list, f'Expecting a comma at index {i}')
        for i in range(0, nitems):
            item = select_list.selectItem(i)
            if item is None:
                return _Error(select_list,
                              f'Expecting proper selection item at index {i}')
            all_columns = item.tableAllColumns()
            if all_columns is not None:
                table_name = None
                if all_columns.tableName() is not None:
                    table_name = _GetTableName(all_columns.tableName())
                self.query.select_items.add_all_columns(table_name).set_limits(
                    item)
            else:
                exp = item.expression()
                expression = ExpressionVisitor(self.query,
                                               tokens.from_tree(exp))
                expression.visit(exp)
                if item.identifier() is not None:
                    self.query.select_items.add_expression_column(
                        expression.source(_GetIdentifier(
                            item.identifier())).set_limits(exp)).set_limits(
                                item)
                elif item.aliasList() is not None:
                    identifiers = [
                        _GetIdentifier(identifier)
                        for identifier in item.aliasList().identifier()
                    ]
                    exp = expression.source(identifiers[0]).set_limits(exp)
                    exp.extra_names = identifiers[1:]
                    self.query.select_items.add_expression_column(
                        exp).set_limits(item)
                else:
                    exp_id = GetComposedIdentifier(exp)
                    if (exp_id and exp_id == expression.expression and
                            exp.getChildCount() == 1):
                        self.query.select_items.add_direct_column(
                            expression.expression).set_limits(item)
                    else:
                        self.query.select_items.add_expression_column(
                            expression.source().set_limits(exp)).set_limits(
                                item)

    def visitFromClause(self, ctx: HiveParser.FromClauseContext):
        if ctx.fromSource().uniqueJoinToken():
            return _Error(ctx, 'UNIQUEJOIN not supported')
        if not ctx.fromSource().joinSource():
            return _Error(ctx, 'Missing FROM source')
        source = _ProcessJoinSource(self.query, ctx.fromSource().joinSource())
        assert isinstance(source, statement.Source)
        self.query.source = source

    def visitWhereClause(self, ctx: HiveParser.WhereClauseContext):
        sc_ctx = ctx.searchCondition()
        if sc_ctx is None:
            return _Error(ctx, 'No search condition in WHERE clause')
        exp_ctx = sc_ctx.expression()
        if exp_ctx is None:
            return _Error(ctx, 'No expression in WHERE search condition')
        exp = ExpressionVisitor(self.query, tokens.from_tree(exp_ctx))
        exp.visit(exp_ctx)
        self.query.where = exp.source().set_limits(ctx)
        self.query.where.role = 'WHERE'

    def _add_group_by(self, exp_ctx: HiveParser.ExpressionsContext):
        exp_tree = tokens.from_tree(exp_ctx)
        exp_num = None
        if len(exp_tree) == 1 and isinstance(exp_tree[0], str):
            try:
                exp_num = int(exp_tree[0])
                self.query.add_group_by(exp_num=exp_num)
                return
            except ValueError:
                pass  # not a number - just an expression
        exp = ExpressionVisitor(self.query, exp_tree)
        exp.visit(exp_ctx)
        self.query.add_group_by(exp=exp.source())

    def visitGroupByClause(self, ctx: HiveParser.GroupByClauseContext):
        gexp = ctx.groupby_expression()
        if gexp.groupByEmpty() is not None:
            return _Error(ctx, 'No GROUP BY expression')
        if gexp.rollupStandard() is not None:
            rstd = gexp.rollupStandard()
            index = 0
            while rstd.expression(index) is not None:
                self._add_group_by(rstd.expression(index))
                index += 1
            if rstd.KW_ROLLUP():
                self.query.group_by.keyword = 'ROLLUP'
            elif rstd.KW_CUBE():
                self.query.group_by.keyword = 'CUBE'
            self.query.group_by.with_parens = True
        elif gexp.rollupOldSyntax() is not None:
            grp = gexp.rollupOldSyntax()
            np_exp = grp.expressionsNotInParenthesis()
            self._add_group_by(np_exp.expression())
            exp_part = np_exp.expressionPart()
            if exp_part is not None:
                index = 0
                while exp_part.expression(index) is not None:
                    self._add_group_by(exp_part.expression(index))
                    index += 1
            if grp.groupingSetExpression():
                group_by = self.query.ensure_group_by()
                for expr in grp.groupingSetExpression():
                    visitor = ExpressionVisitor(self.query,
                                                tokens.from_tree(expr))
                    visitor.visit(expr)
                    group_by.add_grouping_set(visitor.source(parent=group_by))
            if grp.KW_ROLLUP():
                self.query.ensure_group_by().keyword = 'WITH ROLLUP'
            elif grp.KW_CUBE():
                self.query.ensure_group_by().keyword = 'WITH CUBE'
            self.query.ensure_group_by().post_syntax = True

    def visitHavingClause(self, ctx: HiveParser.HavingClauseContext):
        exp_ctx = ctx.havingCondition().expression()
        exp = ExpressionVisitor(self.query, tokens.from_tree(exp_ctx))
        exp.visit(exp_ctx)
        self.query.having = exp.source().set_limits(exp_ctx)
        self.query.having.role = 'HAVING'

    # TODO(cpopescu): treat window ?


class SelectVisitor(HiveParserVisitor):
    """Processes the main `selectStatement` part of a query statement."""

    def __init__(self, query: statement.Source):
        self.query = query

    def visitAtomSelectStatement(self,
                                 ctx: HiveParser.AtomSelectStatementContext):
        if ctx.selectStatement() is not None:
            sub_select = SelectVisitor(self.query)
            sub_select.visit(ctx.selectStatement())
        else:
            atom_visitor = AtomSelectVisitor(self.query)
            atom_visitor.visit(ctx)

    def visitSetOpSelectStatement(self,
                                  ctx: HiveParser.SetOpSelectStatementContext):
        index = 0
        while ctx.setOperator(index) is not None:
            select_ctx = ctx.atomSelectStatement(index)
            set_query = statement.SetOpQuery(
                trees.recompose(ctx.setOperator(index)), self.query)
            set_query.set_limits(select_ctx)
            if select_ctx.selectStatement() is not None:
                sub_select_visitor = SelectVisitor(set_query)
                sub_select_visitor.visit(select_ctx.selectStatement())
            else:
                atom_visitor = AtomSelectVisitor(set_query)
                atom_visitor.visit(select_ctx)
            self.query.set_ops.append(set_query)
            index += 1

    def _process_column_ref(self, clause: statement.QueryClause,
                            column_ref: HiveParser.ColumnRefOrderContext):
        exp = ExpressionVisitor(self.query,
                                tokens.from_tree(column_ref.expression()))
        exp.visit(column_ref.expression())
        exp_statement = exp.source(parent=clause).set_limits(column_ref)
        exp_statement.role = clause.name
        if column_ref.orderSpecification() is not None:
            exp_statement.attributes.append(
                trees.recompose(column_ref.orderSpecification()))
        if column_ref.nullOrdering() is not None:
            exp_statement.attributes.append(
                trees.recompose(column_ref.nullOrdering()))

    def visitOrderByClause(self, ctx: HiveParser.OrderByClauseContext):
        order_clause = statement.QueryClause(self.query,
                                             'ORDER BY').set_limits(ctx)
        for column_ref in ctx.columnRefOrder():
            self._process_column_ref(order_clause, column_ref)
        self.query.clauses.append(order_clause)

    def visitSortByClause(self, ctx: HiveParser.SortByClauseContext):
        if ctx.columnRefOrderInParenthesis() is not None:
            column_refs = ctx.columnRefOrderInParenthesis().columnRefOrder()
        elif ctx.columnRefOrderNotInParenthesis() is not None:
            column_refs = ctx.columnRefOrderNotInParenthesis().columnRefOrder()
        else:
            return _Error(ctx, 'Missing column references in SORT BY clause')
        sort_clause = statement.QueryClause(self.query,
                                            'SORT BY').set_limits(ctx)
        for column_ref in column_refs:
            self._process_column_ref(sort_clause, column_ref)
        self.query.clauses.append(sort_clause)

    def visit_generic_clause(self, name: str, ctx: ParserRuleContext):
        clause = statement.QueryClause(self.query, name).set_limits(ctx)
        for exp_ctx in _ExtractExpression(ctx.expressions()):
            exp = ExpressionVisitor(self.query, tokens.from_tree(exp_ctx))
            exp.visit(exp_ctx)
            exp_stmt = exp.source(parent=clause).set_limits(exp_ctx)
            exp_stmt.role = name
        self.query.clauses.append(clause)

    def visitClusterByClause(self, ctx: HiveParser.ClusterByClauseContext):
        self.visit_generic_clause('CLUSTER BY', ctx)

    def visitPartitionByClause(self, ctx: HiveParser.PartitionByClauseContext):
        self.visit_generic_clause('PARTITION BY', ctx)

    def visitDistributeByClause(self,
                                ctx: HiveParser.DistributeByClauseContext):
        self.visit_generic_clause('DISTRIBUTE BY', ctx)

    def visitLimitClause(self, ctx: HiveParser.LimitClauseContext):
        if ctx.KW_OFFSET():
            try:
                self.query.limit = statement.QueryLimit.with_offset(
                    str(ctx.Number(0)), str(ctx.Number(1)))
            except ValueError:
                return _Error(ctx, 'Invalid offset limit for query')
        else:
            limits = [str(number) for number in ctx.Number()]
            self.query.limit = statement.QueryLimit.with_numbers(limits)


class QueryVisitor(HiveParserVisitor):
    """Processes a `queryStatementExpression` parse subtree."""

    def __init__(self,
                 parent: Optional[statement.Source] = None,
                 name: Optional[str] = None):
        super().__init__()
        self.query = statement.Query(parent, name)

    def visitWithClause(self, ctx: HiveParser.WithClauseContext):
        with_visitor = WithVisitor(self.query)
        with_visitor.visit(ctx)

    def visitQueryStatementExpressionBody(
            self, ctx: HiveParser.QueryStatementExpressionBodyContext):
        if ctx.fromStatement() is not None:
            return _Error(ctx, 'FROM source statement not supported')
        body = ctx.regularBody()
        if body is None:
            return _Error(ctx,
                          'Query expression expecting regular statement body')
        # TODO(cpopescu): treat this case.
        destination = None
        # if body.insertClause() is not None:
        #     destination = get_insert_destination(body.insertClause())
        if body.valuesClause() is not None:
            # TODO(cpopescu): implement
            # set_destination_values(self.query, destination, body.valuesClause())
            return _Error(body,
                          'Values clause not supported in query statement.')
        if body.selectStatement() is None:
            return _Error(body, 'Query expression has no SELECT and no VALUES')
        select_visitor = SelectVisitor(self.query)
        select_visitor.visit(body.selectStatement())
        if destination:
            self.query.destination = destination


class QueryCTEVisitor(HiveParserVisitor):
    """Processes select statements from create expressions."""

    def __init__(self,
                 parent: Optional[statement.Source] = None,
                 name: Optional[str] = None):

        super().__init__()
        self.query = statement.Query(parent, name)

    def visitWithClause(self, ctx: HiveParser.WithClauseContext):
        with_visitor = WithVisitor(self.query)
        with_visitor.visit(ctx)

    def visitSelectStatement(self, ctx: HiveParser.SelectStatementContext):
        select_visitor = SelectVisitor(self.query)
        select_visitor.visit(ctx)


class StatementVisitor(HiveParserVisitor):
    """Processes a top `statements` parse tree."""

    def __init__(self):
        self.statements = []

    def visitQueryStatementExpression(
            self, ctx: HiveParser.QueryStatementExpressionContext):
        query = QueryVisitor()
        query.visit(ctx)
        self.statements.append(query.query.set_limits(ctx))

    def visitCTEStatement(self, ctx):
        name = trees.recompose(ctx.tableName())
        destination = statement.View(parent=None, name=name, original_name=name)
        destination.set_limits(ctx.tableName())
        if not ctx.selectStatementWithCTE():
            return (None, destination)
        query = QueryCTEVisitor(name=name)
        query.visit(ctx.selectStatementWithCTE())
        query.query.destination = destination
        query.query.set_limits(ctx)
        return (query.query, destination)

    def extractInputFile(self, ctx, create_stmt):
        using_clause = ctx.createUsing()
        if not using_clause:
            return
        create_stmt.using_format = trees.recompose(
            using_clause.identifier()).upper()
        if not using_clause.createOptions():
            return
        create_stmt.options = {}
        for elem in using_clause.createOptions().createOptionsElement():
            opt_name = trees.recompose(elem.identifier()).upper()
            create_stmt.options[opt_name] = trees.recompose(elem.expression())
            if opt_name == 'PATH':
                exp = elem.expression()
                if (exp.atomExpression() and exp.atomExpression().constant() and
                        exp.atomExpression().constant().StringLiteral()):
                    # We know is the right format - so is safe
                    # pylint: disable=eval-used
                    create_stmt.input_path = eval(
                        trees.recompose(
                            exp.atomExpression().constant().StringLiteral()))

    def visitCreateViewStatement(self,
                                 ctx: HiveParser.CreateViewStatementContext):
        (query, destination) = self.visitCTEStatement(ctx)
        if ctx.temporary():
            name = 'CREATE TEMPORARY VIEW'
        else:
            name = 'CREATE VIEW'
        create_stmt = statement.Create(None, name, destination,
                                       tokens.from_tree(ctx),
                                       query).set_limits(ctx)
        self.extractInputFile(ctx, create_stmt)
        self.statements.append(create_stmt)

    def visitCreateMaterializedViewStatement(
            self, ctx: HiveParser.CreateMaterializedViewStatementContext):
        (query, destination) = self.visitCTEStatement(ctx)
        self.statements.append(
            statement.Create(None, 'CREATE MATERIALIZED VIEW', destination,
                             tokens.from_tree(ctx), query).set_limits(ctx))

    def visitCreateTableStatement(self,
                                  ctx: HiveParser.CreateTableStatementContext):
        name = trees.recompose(ctx.tableName(0))
        destination = statement.Table(parent=None,
                                      name=name,
                                      original_name=name).set_limits(
                                          ctx.tableName(0))
        query = None
        if ctx.selectStatementWithCTE():
            visitor = QueryCTEVisitor(name=name)
            visitor.visit(ctx.selectStatementWithCTE())
            visitor.query.destination = destination
            query = visitor.query
        create_stmt = statement.Create(None, 'CREATE TABLE', destination,
                                       tokens.from_tree(ctx),
                                       query).set_limits(ctx)
        self.extractInputFile(ctx, create_stmt)
        if ctx.tableLocation():
            # We know is the right format - so is safe
            # pylint: disable=eval-used
            create_stmt.location_path = eval(
                trees.recompose(ctx.tableLocation().StringLiteral()))

        self.statements.append(create_stmt)
