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
"""Classes to represent the structure of a sql statement."""

import re
from collections import deque
from dataschema import Schema
from enum import Enum, auto
from io import StringIO
from typing import Dict, Iterable, List, Optional, Set
from sql_analyze.grammars import tokens, trees
from antlr4.tree.Tree import Tree


def _split_name(name):
    """Splits a name in two components divided by '.'"""
    comp = name.split('.')
    if len(comp) > 1:
        return (comp[0], '.'.join(comp[1:]))
    return (None, name)


_INDENT = '    '


class LinkType(Enum):
    """How various Nodes in the statement graph are linked.

    Attributes:
        INCLUDED: Simple, included child - for placing it in the same 'box'.
        INCLUDED_AUX: Similar with INCLUDED, but placed outside of the same 'box'.
        EXPRESSION: A general expression used by a source.
        EXPRESSION_AUX: An expression with a special role used by a
            source (e.g. WHERE or HAVING).
        LINEAGE: Lineage of a variable or column used in a Expression.
        AS: Points to a name assigned to a source via the AS keyword.
        FROM: Denotes a source used in a FROM clause of a query.
        WITH: Denotes a source defined via a WITH clause in a query.
        SOURCE: A general source of data in a query or clause.
        OUTPUT: Generally an expression that provides the output of a parent source.
        SET_OP: A query used via a set operation (e.g. union, intersection)
            with the parent query.
    """
    INCLUDED = auto()
    INCLUDED_AUX = auto()
    EXPRESSION = auto()
    EXPRESSION_AUX = auto()
    LINEAGE = auto()
    AS = auto()
    FROM = auto()
    WITH = auto()
    SOURCE = auto()
    OUTPUT = auto()
    SET_OP = auto()


class GraphLink:
    """A link between two nodes in a graph.

    Attributes:
        src: The source node of the link (source, child).
        dest: The destination of the link (user, parent).
        link_type: the relation between `src` and `dest`.
    """

    def __init__(self, src: 'GraphNode', dst: 'GraphNode', link_type: LinkType):
        self.src = src
        self.dst = dst
        self.link_type = link_type

    def __hash__(self):
        return hash((self.src.node_id, self.dst.node_id, self.link_type))


class GraphNode:
    """Node in a statement graph.

    Attributes:
        node_id: A unique integer identifier of the node.
        source: The statement `Source` associated with the node (query, expression,
           table).
        children: Likes to this node by a child-like relationship ie. included by this
           node. The links point from `children` to self.
        inlinks: Other data links to this node e.g. sources, lineage and such.
    """

    def __init__(self, source: 'Source', node_id: int):
        assert source is not None
        self.node_id = node_id
        self.source = source
        # Link to nodes that are inside this node - we are the destination.
        self.children: List[GraphLink] = []
        # Nodes that point to us.
        self.inlinks: List[GraphLink] = []

    def add_child(self,
                  child: 'GraphNode',
                  link_type: LinkType = LinkType.INCLUDED):
        """Adds a child to this graph node, with `link_type` relation."""
        assert isinstance(child, GraphNode)
        self.children.append(GraphLink(child, self, link_type))

    def add_inlink(self, source: 'GraphNode', link_type: LinkType):
        """Adds a data link from source to this node in the graph."""
        assert isinstance(source, GraphNode)
        self.inlinks.append(GraphLink(source, self, link_type))


class Graph:
    """A statement graph: a collection of interlinked `GraphNode`s

    Attributes:
        id2node: maps from node id to `GraphNode`
        src2id: maps from statement `Source` to node id
    """

    def __init__(self, root_source: Optional['Source'] = None):
        self._next_id = 1
        self.id2node: Dict[int, GraphNode] = {}
        self.src2id: Dict['Source', id] = {}
        if root_source:
            self.populate(root_source)
        self._outlinks = None

    def add_source(self, source: 'Source'):
        """Adds one node to the graph."""
        if source in self.src2id:
            return
        node = GraphNode(source, self._next_id)
        self._next_id += 1
        self.id2node[node.node_id] = node
        self.src2id[node.source] = node

    def node_by_id(self, node_id: int):
        return self.id2node[node_id]

    def node_by_source(self, source: 'Source'):
        return self.src2id[source]

    def populate(self, root_source: 'Source'):
        """Populates the graph with the source and its children, recursively."""
        source_queue = deque()
        source_queue.append(root_source)
        num_added = 0
        while source_queue:
            source = source_queue.popleft()
            num_added += 1
            self.add_source(source)
            source_queue.extend(
                child for child in source.children if child not in self.src2id)
        return num_added

    def build_outlinks(self, force=False):
        """Builds the inverted graph, for each node, in outlinks map."""
        if self._outlinks is not None and not force:
            return
        self._outlinks = {node: set() for node in self.id2node.values()}
        for node in self.id2node.values():
            for linkset in (node.children, node.inlinks):
                for link in linkset:
                    self._outlinks[link.src].add(node)

    def collect_linked_set(self, node):
        """Collectes all nodes who transiently point to `node`
           or are pointed by `node`, in a set of nodes.
        """
        to_process = deque([node])
        collected = set()
        collected.add(node)
        while to_process:
            crt = to_process.popleft()
            for linkset in (crt.children, crt.inlinks):
                for link in linkset:
                    if link.src not in collected:
                        collected.add(link.src)
                        to_process.append(link.src)
        self.build_outlinks()
        out_processed = set()
        to_process = deque([node])
        while to_process:
            crt = to_process.popleft()
            for child in self._outlinks[crt]:
                if child not in out_processed:
                    out_processed.add(child)
                    collected.add(child)
                    to_process.append(child)
        return collected

    def get_input_tables(self) -> List['Table']:
        """Returns the list of tables read in the graph."""
        inputs = []
        for source in self.src2id:
            if isinstance(source, Table) and source.is_end_source:
                inputs.append(source)
        return inputs

    def get_output_tables(self) -> List['Table']:
        """Returns the list of tables created by the graph."""
        outputs = set()
        for source in self.src2id:
            if (isinstance(source, (Query, Insert, Create)) and
                    isinstance(source.destination, Table)):
                outputs.add(source.destination)
        return list(outputs)


class SchemaProvider:
    """Base class for an object that provides schema from a name."""

    def __init__(self):
        pass

    def find_schema(self, name: str) -> Optional[Schema.Table]:
        del name


class SchemaInfo:
    """Represents schema information for a statement.

    Attributes:
      tables: maps from table name to schema table.
      output: schema for the output of a query.
    """

    def __init__(self,
                 tables: Optional[Dict[str, Schema.Table]] = None,
                 output: Schema.Table = None):
        if tables:
            self.tables = tables
        else:
            self.tables = {}
        self.output = output

    @classmethod
    def from_statement(cls, sql_code: str, schema_provider: SchemaProvider):
        schema_re = re.compile(r'^--@@?Schema:\s*(\w*)\s*([\w\.]*)\s*$')
        output_re = re.compile(r'^--@@?Output\s*:\s*([\w\.]*)\s*$')
        tables = {}
        output = None
        for line in sql_code.splitlines():
            schema = schema_re.match(line)
            if schema:
                schema_table = schema_provider.find_schema(schema.group(2))
                if not schema_table:
                    raise ValueError(
                        f'Schema for table `{schema.group(1)}` named '
                        f'`{schema.group(2)}` cannot be found')
                tables[schema.group(1)] = schema_table
            else:
                output_name = output_re.match(line)
                if output_name:
                    output = schema_provider.find_schema(output_name.group(1))
                    if not output:
                        raise ValueError(
                            f'Output schema for `{output_name.group(1)}` cannot be found'
                        )

        return cls(tables, output)


class Source:
    """Base class for a component in a sql statement - a source of names.

    The optional `parent` represents the source that uses this object
    (e.g. the Query that uses a Table).
    The optional `name` is the SQL name of this source
    (e.g in an 'AS <name>' for an expression, table or query)
    The `children` list is the Source-s that are used (referenced) by this
    `Source`.

    Attributes:
        parent: the `Source` that includes / uses this `Source` (eg. `Query` using
            an `Expression`)
        name: name of the source - the meaning of this varies with the `Source` type.
        children: list of children - `Source`s which have this `Source` as `parent`.
        laterals: list of children that are lateral sources to this source.
           These include lateral views, pivots and such.
        start: position in the sql statement where the code of this `Source` starts.
        stop: position in the sql statement where the code of this `Source` stops.
    """

    def __init__(self, parent: 'Source', name: Optional[str] = None):
        assert parent is None or isinstance(parent,
                                            Source), f'Type: {type(name)}'
        assert name is None or isinstance(name, str), f'Type: {type(name)}'
        self.parent: Optional[Source] = parent
        self.name: Optional[str] = name
        self.children: List[Source] = []
        self.laterals: List[Source] = []
        self.start: Optional[trees.CodeLocation] = None
        self.stop: Optional[trees.CodeLocation] = None
        if parent:
            parent.add_child(self)

    def set_limits(self, tree: Tree):
        """Extracts the `start` and `stop` limits of this source from a parse tree."""
        self.start = trees.start_location(tree)
        self.stop = trees.stop_location(tree)
        return self

    def add_child(self, child: 'Source'):
        """Adds a source to the list of children."""
        if not self.is_child(child):
            self.children.append(child)

    def add_lateral(self, lateral: 'Source'):
        """Adds a lateral source to this one."""
        lateral.set_parent(self)
        self.laterals.append(lateral)

    def remove_child(self, child: 'Source'):
        """Removes a source from the list of children."""
        self.children.remove(child)

    def is_child(self, child: 'Source'):
        """If `child` source is in the list of children."""
        return child in self.children

    def set_parent(self, parent: 'Source'):
        """Sets the parent of this source."""
        if self.parent == parent:
            return
        if self.parent:
            self.parent.remove_child(self)
        self.parent = parent
        if parent:
            parent.add_child(self)

    def is_sql_construct(self):
        """If this source is not really a source, but a SQL auxiliary construct."""
        return False

    def children_str(self,
                     ctype: str = '',
                     indent: str = '',
                     children_only: bool = False):
        """Returns a multi-line string including the children of this Source."""
        if children_only:
            if not ctype:
                ctype = type(self).__name__
        else:
            ctype = 'Children'
        if not self.children:
            if children_only:
                return f'{self.name_str(indent=indent)}\n'
            return ''
        child_indent = indent + _INDENT

        def reindent(s: str):
            return indent + '  * ' + s[len(child_indent):]

        return f'{indent}{ctype}:\n' + ''.join(
            reindent(
                child.
                children_str(indent=child_indent, children_only=children_only)
                if children_only else child.tree_str(child_indent))
            for child in self.children)

    def name_str(self, stype: str = '', indent: str = '', ext: str = ''):
        """Returns the one-line string for this Source."""
        if not stype:
            stype = type(self).__name__
        if self.name:
            return f'{indent}{stype}: `{self.name}`{ext}'
        return f'{indent}{stype}{ext}'

    def tree_str(self, indent: str = ''):
        """Returns a multi-line representation of this Source."""
        return f'{self.name_str(indent=indent)}\n{self.children_str(indent=indent)}'

    def recompose(self, sep: str = ' '):
        """Recomposes the SQL statement for this Source."""
        raise NotImplementedError(
            f'recompose() not implemented for {type(self).__name__}')

    def laterals_recompose(self, sep: str = ' '):
        """Recomposes the lateral clauses."""
        if not self.laterals:
            return ''
        last_is_lateral = False
        with StringIO() as s:
            for lateral in self.laterals:
                if last_is_lateral:
                    s.write(',')
                s.write(sep)
                s.write(lateral.recompose(sep))
                last_is_lateral = not lateral.is_sql_construct()
            return s.getvalue()

    def resolve_name(self, name: str, in_scope: bool, find_source: bool):
        """Provided a (composed) name for us or for a child, resolve it."""
        del in_scope
        del find_source
        if name == self.name or not name:
            return self
        return None

    def chain_resolve(self, name: str, resolvers: Iterable['Source'],
                      in_scope: bool, find_source: bool):
        """Resolves the name in the provided resolvers. Returns first resolve."""
        for resolver in resolvers:
            resolved = resolver.resolve_name(name, in_scope, find_source)
            if resolved is not None:
                return resolved
        return None

    def _chain_resolve_to_list(self, name: str, resolvers: Iterable['Source'],
                               in_scope: bool, find_source: bool,
                               output: Set['Source']):
        """Resolves the name in the provided resolvers, and appends it to output."""
        src = self.chain_resolve(name, resolvers, in_scope, find_source)
        if src:
            output.add(src)

    def _is_processed(self, processed: Set['Source']):
        """Checks if this node is in a set of nodes, and if not adds it."""
        if self in processed:
            return True
        processed.add(self)
        return False

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        """Links the node corresponding to this source in the graph."""
        raise NotImplementedError(
            f'link_graph() not implemented for {type(self).__name__}')

    def apply_schemas(self, schema_info: SchemaInfo):
        """Sets the schema of underlying tables based on the ones
        declared in schema_info."""
        for child in self.children:
            child.apply_schemas(schema_info)


class Column(Source):
    """A column selected from a parent source, with an optional schema attached.

    Attributes:
        schema: the schema (type and data annotations) of this column (if known).
    """

    def __init__(self,
                 parent: Source,
                 name: Optional[str],
                 schema: Optional[Schema.Column] = None):
        super().__init__(parent, name)
        self.schema = schema

    def recompose(self, sep: str = ' '):
        return self.name

    def link_graph(self, graph: Graph, resolvers: List[Source],
                   processed: Set[Source]):
        pass


class Expression(Source):
    """An expression present in a parent statement / source.

    Attributes:
        expression: represents the actual SQL string representation.
        expression_tokens: the individual tokens in the expression.
        expression_block: the expression tokens split in blocks for formatting.
        referenced_names: contains the identifiers referenced by this expression.
        extra_names: is used when multiple names are used in an 'AS' expression,
           first one being `name`, while the rest are stored here.
        attributes: any extra attributes associated with the expression
           (e.g. DESC or ASC for an ordering expression).
        role: the role this expression carries with the parent
           (e.g. 'WHERE' or 'GROUP BY' etc).
        name: is generally the 'AS' name for this expression.
        children: contains any subqueries used in this expression.
    """

    def __init__(self,
                 parent: Source,
                 name: str,
                 expression_tokens: tokens.Tokens,
                 referenced_names: Optional[Set[str]] = None):
        super().__init__(parent, name)
        self.expression, _, _ = tokens.recompose(expression_tokens)
        self.expression_tokens = expression_tokens
        self.expression_block = tokens.to_block(expression_tokens)
        if referenced_names is None:
            referenced_names = {}
        self.referenced_names = referenced_names
        self.extra_names = []
        self.attributes = []
        self.role = None

    def short_exp(self, limit: int = 80):
        if len(self.expression) < limit:
            exp = self.expression
        else:
            exp = self.expression[:limit] + ' ... '
        if not self.attributes:
            return exp
        return f'{exp} {" ".join(self.attributes)}'

    def name_str(self, stype: str = '', indent: str = '', ext: str = ''):
        return super().name_str(stype, indent, f' {{ {self.short_exp()} }}')

    def tree_str(self, indent: str = ''):
        s = f'{self.name_str(indent=indent)}\n'
        if self.extra_names:
            s += f'{indent}{_INDENT}Extra Names: ", ".join(self.extra_names)\n'
        if self.referenced_names:
            names = list(self.referenced_names)
            names.sort()
            s += f'{indent}{_INDENT}References: {names}\n'
        return s + self.children_str('SubQuery', indent=indent + _INDENT)

    def recompose(self, sep: str = ' '):
        if self.attributes:
            return f'{self.expression}{sep}{" ".join(self.attributes)}'
        return self.expression

    def link_references(self, name: str, link_type: LinkType, graph: Graph,
                        resolvers: List[Source]):
        node = graph.node_by_source(self)
        srcs = set()
        self._chain_resolve_to_list(name, resolvers, False, False, srcs)
        parent_name, _ = _split_name(name)
        if parent_name:
            self._chain_resolve_to_list(parent_name, resolvers, False, True,
                                        srcs)
        for i in range(1, len(resolvers)):
            self._chain_resolve_to_list(name, resolvers[i:], False, False, srcs)
        for src in srcs:
            node.add_inlink(graph.node_by_source(src), link_type)

    def link_graph(self, graph: Graph, resolvers: List[Source],
                   processed: Set[Source]):
        if self._is_processed(processed):
            return
        node = graph.node_by_source(self)
        for name in self.referenced_names:
            self.link_references(name, LinkType.LINEAGE, graph, resolvers)

        for child in self.children:
            child.link_graph(graph, resolvers, processed)
            node.add_inlink(graph.node_by_source(child), LinkType.EXPRESSION)

    def has_role(self):
        return self.role is not None


class Table(Source):
    """A source table in statement, with `name` being the SQL name, possibly
    defined via an 'AS' while the `original_name` is the name of the table.

    Attributes:
        original_name: the actual name of the table. `name` holds the alias value
           if present, or can be same as `original_name`.
        schema: the schema associated with the table.
        is_end_source: filled in after `link_graph(..)` is called, to denote a table
           that is present in the actual database (ie. is not a reference of another
           query or renamed table).
    """

    def __init__(self,
                 parent: Source,
                 name: Optional[str],
                 original_name: Optional[str],
                 schema: Optional[Schema.Table] = None):
        super().__init__(parent, name)
        self.original_name = original_name
        self.schema = schema
        self.is_end_source = False

    def tree_str(self, indent: str = ''):
        return (f'{indent}Table: `{self.original_name}` AS `{self.name}`\n' +
                self.children_str('Columns', indent))

    def source_str(self):
        if self.name != self.original_name:
            return f'{self.original_name} AS {self.name}'
        return self.name

    def recompose(self, sep: str = ' '):
        return self.source_str() + self.laterals_recompose(sep)

    def _local_find(self, name: str):
        return self.chain_resolve(name, self.children, True, False)

    def _find_source(self, name: str):
        if name in (self.name, self.original_name):
            return self
        return None

    def resolve_name(self, name: str, in_scope: bool, find_source: bool):
        if find_source:
            return self._find_source(name)
        if in_scope:
            return self._local_find(name)
        parent_name, field_name = _split_name(name)
        if not parent_name:
            return self._find_source(name)
        if self._find_source(parent_name) is not None:
            if not self.children:
                return self
            return self._local_find(field_name)
        return self._local_find(name)

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed):
            return
        node = graph.node_by_source(self)
        for child in self.children:
            node.add_child(graph.node_by_source(child))
            child.link_graph(graph, resolvers, processed)
        srcs = []
        for i in range(0, len(resolvers)):
            src = self.chain_resolve(self.original_name, resolvers[i:], False,
                                     True)
            if src and src != self and src not in srcs:
                srcs.append(src)
        if not srcs:
            self.is_end_source = True
        for src in srcs:
            node.add_inlink(graph.node_by_source(src), LinkType.SOURCE)

    def set_schema(self, schema: Schema.Table):
        if self.schema:
            diffs = self.schema.compare(schema)
            major_diffs = [
                diff for diff in diffs if not diff.is_struct_mismatch()
            ]
            if major_diffs:
                raise ValueError(
                    f'Cannot update schema for table {self.name_str()} '
                    f'per major differences: {major_diffs}')
        self.schema = schema

    def apply_schemas(self, schema_info: SchemaInfo):
        if self.original_name in schema_info.tables:
            self.set_schema(schema_info.tables[self.name])


class View(Table):
    """A view is the same as a table, but using a different class."""
    pass


class LateralView(Source):
    """Source describing lateral view or tables to a source.

    Attributes:
        keyword: the keyword(s) instantiating this source e.g. `LATERAL VIEW` or so.
        expression_block: the SQL expression that generates the view
        expression_tokens: the tokens in the SQL expression that generates the view.
        expression_block: the expression tokens grouped in blocks for output
            formatting.
        columns: output columns of this lateral view
    """

    def __init__(self,
                 parent: Source,
                 name: str,
                 keyword: str,
                 expression_tokens: tokens.Tokens,
                 columns: Optional[List[Column]] = None):
        super().__init__(parent, name)
        self.keyword = keyword
        self.expression_tokens = expression_tokens
        self.expression_block = tokens.to_block(expression_tokens)
        self.expression, _, _ = tokens.recompose(expression_tokens)
        if columns is None:
            columns = []
        self.columns = columns

    def expression_str(self):
        if self.keyword == 'LATERAL TABLE':
            return f'({self.expression}) AS {self.name}'
        name = f' {self.name}' if self.name else ''
        return f'{self.expression}{name}'

    def columns_str(self, sep: str = ' '):
        if not self.columns:
            return ''
        columns = ', '.join(column.recompose(sep) for column in self.columns)
        if self.keyword == 'LATERAL TABLE':
            return f' ({columns})'
        else:
            return f' AS {columns}'

    def recompose(self, sep: str = ' '):
        return f'{self.keyword} {self.expression_str()}{self.columns_str(sep)}'

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed):
            return
        node = graph.node_by_source(self)
        for column in self.columns:
            node.add_child(graph.node_by_source(column))
        graph.node_by_source(self.parent.parent).add_inlink(
            node, LinkType.SOURCE)


class PivotView(Source):
    """Source describing the pivot side clause of a source.

    Consider this statement:
    ```
    SELECT * FROM person
    PIVOT (
        SUM(age) AS a, AVG(class) AS c
        FOR name IN ('John' AS john, 'Mike' AS mike)
    );
    ```

    Attributes:
        pivots: the (aggregate) expressions generating the pivot columns.
           For example above this includes `SUM(age) AS a` and `AVG(class) AS c`
        columns_source: the source columns of the pivots. In the example above
           this includes `name`
        columns_expressions: the expressions taken by `columns_source`. In the
           example above, these includes `'John' AS john` and `'Mike' AS mike`.
        columns: the output columns of the pivot - a combination of `columns_source`
           and `pivots` name. In our example these are `john_a`, `mike_a`,
           `john_c` and `mike_c`.
    """

    def __init__(self, parent: Source):
        super().__init__(parent, 'PIVOT')
        self.pivots: List[Expression] = []
        self.columns_source: List[Column] = []
        self.columns_expressions: List[Expression] = []
        self.columns: List[Column] = []

    def recompose(self, sep: str = ' '):
        with StringIO() as s:
            s.write('PIVOT (')
            if self.pivots:
                s.write(', '.join(f'{pivot.recompose(sep)}' +
                                  (f' AS {pivot.name}' if pivot.name else '')
                                  for pivot in self.pivots))
            s.write(f'{sep}FOR{sep}')
            s.write(', '.join(
                column.recompose(sep) for column in self.columns_source))
            s.write(f'{sep}IN{sep}')
            if self.columns_expressions:
                s.write('(')
                s.write(', '.join(f'{exp.recompose(sep)}' +
                                  (f' AS {exp.name}' if exp.name else '')
                                  for exp in self.columns_expressions))
                s.write(')')
            s.write(')')
            return s.getvalue()

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed):
            return
        node = graph.node_by_source(self)
        for pivot in self.pivots:
            node.add_child(graph.node_by_source(pivot), LinkType.EXPRESSION_AUX)
        for column in self.columns_source:
            colnode = graph.node_by_source(column)
            node.add_child(colnode, LinkType.EXPRESSION_AUX)
            for column_exp in self.columns_expressions:
                colnode.add_child(graph.node_by_source(column_exp),
                                  LinkType.EXPRESSION)
        parent_node = graph.node_by_source(self.parent.parent)
        for column in self.columns:
            colnode = graph.node_by_source(column)
            node.add_child(colnode)
            colnode.add_inlink(node, LinkType.EXPRESSION)
            parent_node.add_inlink(colnode, LinkType.EXPRESSION)
        graph.node_by_source(self.parent.parent).add_inlink(
            node, LinkType.SOURCE)


class JoinExpression(Source):
    """Presents how two sources are joined.

    Attributes:
        expression: the join expression if `ON <expression>` is employed.
        columns: represents the join columns in case of `USING (<columns>)`.
        name: contains the way the join is invoked (e.g. `LEFT JOIN`)
    """

    def __init__(self,
                 name: str,
                 expression: Optional[Expression] = None,
                 columns: Optional[List[Column]] = None):
        super().__init__(None, name.upper())
        self.expression = expression
        if self.expression:
            self.expression.set_parent(self)
        self.columns = columns

    @classmethod
    def on_expression(cls, join_type: str, expression: Expression):
        return cls(join_type, expression=expression)

    @classmethod
    def using_columns(cls, join_type: str, columns: List[Column]):
        return cls(join_type, columns=columns)

    @classmethod
    def naked(cls, join_type):
        return cls(join_type)

    def recompose(self, sep: str = ' '):
        if self.expression:
            return self.name + ' ON ' + self.expression.recompose(sep)
        elif self.columns:
            return self.name + ' ON ' + ', '.join(
                column.recompose(sep) for column in self.columns)
        return self.name

    def name_str(self, stype: str = '', indent: str = '', ext: str = ''):
        if self.columns:
            return f'{indent}[{self.name}] by Columns'
        elif self.expression:
            return f'{indent}[{self.name}] by Expression'
        else:
            return f'{indent}[{self.name}]'

    def tree_str(self, indent: str = ''):
        sub_indent = indent + _INDENT
        name = f'{self.name_str(indent=indent)}\n'
        if self.columns:
            return f'{indent}{name}\n' + ''.join(
                column.tree_str(sub_indent) for column in self.columns)
        elif self.expression:
            return f'{indent}{name}\n{self.expression.tree_str(sub_indent)}'
        else:
            return f'{indent}{name}\n'

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed):
            return
        node = graph.node_by_source(self)
        if self.expression:
            node.add_child(graph.node_by_source(self.expression),
                           LinkType.OUTPUT)
            self.expression.link_graph(graph, resolvers, processed)
            for name in self.expression.referenced_names:
                self.expression.link_references(name, LinkType.SOURCE, graph,
                                                resolvers)
        elif self.columns:
            for column in self.columns:
                column_node = graph.node_by_source(column)
                node.add_child(column_node, LinkType.OUTPUT)
                resolved = self.chain_resolve(column.name, resolvers, False,
                                              False)
                if resolved:
                    column_node.add_inlink(graph.node_by_source(resolved),
                                           LinkType.EXPRESSION)


class JoinSource(Source):
    """This class denotes a source that is a join of multiple children.

    Attributes:
        sources: represents the sources to be joined - can be a `Table`, `Query`,
           or `JoinSource`.
        join_expressions: defines how the joins are performed. An expression
           at index `i` defines how sources `i - 1` and `i` are joined, thus the
           len of this list should be `len(sources) - 1`
    """

    def __init__(self,
                 parent: Source,
                 sources: Optional[List[Source]] = None,
                 join_expressions: Optional[List[JoinExpression]] = None):
        super().__init__(parent, None)
        if sources is None:
            sources = []
        if join_expressions is None:
            join_expressions = []
        for source in sources:
            source.set_parent(self)
        self.sources = sources
        self.join_expressions = join_expressions

    def add_source(self,
                   source: Source,
                   join_expression: Optional[JoinExpression] = None):
        assert join_expression or not self.sources
        if join_expression:
            join_expression.set_parent(self)
            self.join_expressions.append(join_expression)
        source.set_parent(self)
        self.sources.append(source)

    def tree_str(self, indent: str = ''):
        if not self.sources:
            return 'Empty Join'
        assert len(self.sources) == len(self.join_expressions) + 1
        with StringIO() as s:
            s.write(self.sources[0].tree_str(indent))
            for src, join in zip(self.sources[1:], self.join_expressions):
                s.write(join.tree_str(indent + _INDENT))
                s.write(src.tree_str(indent))
            return s.getvalue()

    def recompose(self, sep: str = ''):
        with StringIO() as s:
            if isinstance(self.parent, JoinSource):
                s.write('(')
            s.write(self.sources[0].recompose(sep))
            for src, join in zip(self.sources[1:], self.join_expressions):
                s.write(f'{sep}{join.name}{sep}')
                if isinstance(src, JoinSource):
                    s.write(f'({src.recompose(sep)})')
                elif isinstance(src, (Query, Expression)):
                    s.write(f'({src.recompose(sep)})')
                    if src.name:
                        s.write(f' AS {src.name}')
                else:
                    s.write(src.recompose(sep))
                if join.expression:
                    s.write(f'{sep}ON {join.expression.recompose(sep)}')
                elif join.columns:
                    columns = ', '.join(
                        column.recompose(sep) for column in join.columns)
                    s.write(f'{sep}USING ({columns})')
            if isinstance(self.parent, JoinSource):
                s.write(')')
            s.write(self.laterals_recompose(sep))
            return s.getvalue()

    def _local_resolve(self, name: str):
        return self.chain_resolve(name, self.sources, True, False)

    def _find_source(self, name: str):
        return self.chain_resolve(name, self.sources, False, True)

    def resolve_name(self, name: str, in_scope: bool, find_source: bool):
        if find_source:
            return self._find_source(name)
        if in_scope:
            return self._local_resolve(name)
        parent_name, field_name = _split_name(name)
        if parent_name is None:
            return self._local_resolve(name)
        sub_resolver = self._find_source(name)
        if sub_resolver:
            return sub_resolver.resolve_name(field_name, True, False)
        return self._local_resolve(name)

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed):
            return
        node = graph.node_by_source(self)
        for source in self.sources:
            node.add_child(graph.node_by_source(source), LinkType.FROM)
        for join in self.join_expressions:
            node.add_child(graph.node_by_source(join), LinkType.OUTPUT)
        last_source = None
        for index in range(len(self.sources)):
            self.sources[index].link_graph(graph, resolvers, processed)
            if last_source:
                join_resolvers = [last_source, self.sources[index]]
                join_resolvers.extend(resolvers)
                self.join_expressions[index - 1].link_graph(
                    graph, join_resolvers, processed)
            last_source = self.sources[index]


class SelectItem(Source):
    """An item selected via a sql statement.


    Attributes:
        all_columns: if an asterisk is involved in this item selection.
    """

    def __init__(self,
                 all_columns: bool,
                 parent: 'Query',
                 name: Optional[str] = None):
        super().__init__(parent, name)
        assert isinstance(parent, SelectItems)
        self.all_columns = all_columns

    def recompose(self, sep: str = ''):
        raise NotImplementedError(
            f'recompose() not implemented for {type(self).__name__}')

    def resolve_name(self, name: str, in_scope: bool, find_source: bool):
        if find_source:
            return None
        return super().resolve_name(name, in_scope, find_source)


class SelectItemDirect(SelectItem):
    """An item selected directly, w/o expression."""

    def __init__(self, parent: 'Query', name: Optional[str] = None):
        super().__init__(False, parent, name)

    def recompose(self, sep: str = ''):
        return self.name

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed):
            return
        return


class SelectItemExpression(SelectItem):
    """An expression based selection via a sql statement.

    Attributes:
        expression: the expression used in the select item as in `<expression> AS <name>`
    """

    def __init__(self,
                 parent: 'Query',
                 name: Optional[str] = None,
                 expression: Optional[Expression] = None):
        super().__init__(False, parent, name)
        self.expression = expression
        self.expression.set_parent(self)

    def name_str(self, stype: str = '', indent: str = '', ext: str = ''):
        return super().name_str(stype, indent,
                                f' {{ {self.expression.short_exp()} }}')

    def tree_str(self, indent: str = ''):
        return (f'{self.name_str(indent=indent)}\n' +
                self.expression.tree_str(indent + _INDENT))

    def recompose(self, sep: str = ''):
        s = self.expression.recompose(sep)
        if self.name:
            if self.expression.extra_names:
                extra = ', '.join(self.expression.extra_names)
                s += f'{sep}AS ({self.name}, {extra})'
            else:
                s += f'{sep}AS {self.name}'
        return s

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed):
            return
        node = graph.node_by_source(self)
        exp_node = graph.node_by_source(self.expression)
        graph.node_by_source(self.parent).add_child(exp_node)
        # node.add_child(exp_node)
        node.add_inlink(exp_node, LinkType.AS)
        self.expression.link_graph(graph, resolvers, processed)


class SelectItemAllColumns(SelectItem):
    """An all-column(*) item selected via a sql statement."""

    def __init__(self, parent: 'Query', name: Optional[str] = None):
        super().__init__(True, parent, name)

    def tree_str(self, indent: str = ''):
        return self.name_str('', indent, ' *') + '\n'

    def recompose(self, sep: str = ' '):
        if self.name:
            return f'{self.name}.*'
        return '*'

    def resolve_name(self, name: str, in_scope: bool, find_source: bool):
        if find_source:
            return None
        if self.name is not None and name.starts_with(f'{self.name}'):
            return self
        return None

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed) or not self.name:
            return
        resolved = self.chain_resolve(self.name, resolvers, False, True)
        if not resolved:
            return
        graph.node_by_source(self).add_inlink(graph.node_by_source(resolved),
                                              LinkType.LINEAGE)


class SelectItems(Source):
    """The selection of items in a query - children are select items.

    Attributes:
        keyword: the way in which selection is made. E.g. `DISTINCT`, `ALL` etc.
    """

    def __init__(self, parent: Optional[Source] = None):
        super().__init__(parent, None)
        # This can contain something like ALL or DISTINCT
        self.keyword: Optional[str] = None

    def add_all_columns(self, table_name: Optional[str] = None):
        item = SelectItemAllColumns(self, table_name)
        self.add_child(item)
        return item

    def add_direct_column(self, name: str):
        item = SelectItemDirect(self, name)
        self.add_child(item)
        return item

    def add_expression_column(self, expression: Expression):
        assert isinstance(expression, Expression)
        item = SelectItemExpression(self, expression.name, expression)
        self.add_child(item)
        return item

    def add_child(self, child: Source):
        assert isinstance(child, SelectItem)
        super().add_child(child)

    def has_select_all(self, max_len: Optional[int] = None):
        if max_len is None or max_len > len(self.children):
            max_len = len(self.children)
        for i in range(max_len):
            if isinstance(self.children[i], SelectItemAllColumns):
                return True
        return False

    def is_valid_group_by_index(self, index: int):
        return (0 < index <= len(self.children) and
                not self.has_select_all(index) and
                self.children[index - 1].name)

    def tree_str(self, indent: str = ''):
        if not self.children:
            return ''
        sub_indent = indent + _INDENT
        with StringIO() as s:
            s.write(f'{indent}SELECT')
            if self.keyword:
                s.write(f' {self.keyword}')
            s.write('\n')
            s.write(''.join(si.tree_str(sub_indent) for si in self.children))
            return s.getvalue()

    def recompose(self, sep: str = ''):
        with StringIO() as s:
            s.write('SELECT')
            if self.keyword:
                s.write(f' {self.keyword}')
            s.write(sep)
            s.write(f',{sep}'.join(si.recompose(sep) for si in self.children))
            return s.getvalue()

    def resolve_name(self, name: str, in_scope: bool, find_source: bool):
        del find_source
        if in_scope:
            for child in self.children:
                if child.name == name:
                    return child
        else:
            for child in self.children:
                if (isinstance(child, SelectItemAllColumns) and
                    (child.name is None or name.starts_with(f'{child.name}.'))):
                    return child
        return None

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed):
            return
        node = graph.node_by_source(self)
        for child in self.children:
            node.add_child(graph.node_by_source(child), LinkType.OUTPUT)
            child.link_graph(graph, resolvers, processed)


class QueryClause(Source):
    """Side clauses that can appear along select statement (e.g. `ORDER BY`)."""

    def __init__(self,
                 parent: Optional[Source] = None,
                 name: Optional[str] = None):
        super().__init__(parent, name)

    def is_sql_construct(self):
        return True

    def add_child(self, child: Source):
        assert isinstance(child, Expression)
        child.role = self.name
        super().add_child(child)

    def recompose(self, sep: str = ''):
        expressions = ', '.join(child.recompose(sep) for child in self.children)
        return f'{self.name} {expressions}'

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed):
            return
        node = graph.node_by_source(self)
        for child in self.children:
            node.add_child(graph.node_by_source(child), LinkType.EXPRESSION_AUX)
            child.link_graph(graph, resolvers, processed)


class QueryLimit:
    """Contains the limits for a query.

    Attributes:
        limit: numeric LIMIT of the query.
        offset: numeric OFFSET of the limit clause.
        extra_limits: some dialects support multiple limit values (which complement
            the limit)
        postfix: some dialects support some postfix keywords after the limit clause.
    """

    def __init__(self,
                 limit: str,
                 offset: Optional[str] = None,
                 extra_limits: Optional[List[str]] = None):
        self.limit = limit
        self.offset = offset
        self.extra_limits = extra_limits
        self.postfix: Optional[str] = None

    def recompose(self):
        postfix = ''
        if self.postfix:
            postfix = f' {self.postfix}'
        if self.offset is not None:
            return f'LIMIT {self.limit} OFFSET {self.offset}{postfix}'
        if self.extra_limits:
            extra = ', '.join(str(l) for l in self.extra_limits)
            return f'LIMIT {self.limit}, {extra}{postfix}'
        return f'LIMIT {self.limit}{postfix}'

    @classmethod
    def with_offset(cls, limit: str, offset: str):
        return cls(limit, offset=offset)

    @classmethod
    def with_numbers(cls, limits: List[str]):
        return cls(limits[0], extra_limits=limits[1:])


class GroupByClause(Source):
    """Contains the group by clauses in a query.

    Attibutes:
       keyword: any initial keyword contained after the `GROUP BY`
           (e.g. `CUBE`, `ROLLUP` etc).
       post_keyword: posterior keywords in the `GROUP BY` (e.g. `WITH CUBE`).
       with_parens: if grouping expressions are to be reconstructed in parenthesis.
       post_syntax: place the decoration keywords after the group by clause
       grouping_sets: list of GROUPING SETS expressions.
    """

    def __init__(self, parent: Optional[Source] = None):
        super().__init__(parent, 'GROUP BY')
        self.group_by: List[Expression] = []
        self.keyword: Optional[str] = None
        self.post_keyword: Optional[str] = None
        self.with_parens = False
        self.post_syntax = False
        self.grouping_sets: List[Expression] = []

    def is_sql_construct(self):
        return True

    def add_group_by(self, expression: Expression):
        expression.role = 'GROUP BY'
        assert expression is not None
        expression.set_parent(self)
        self.group_by.append(expression)

    def add_grouping_set(self, expression: Expression):
        expression.role = 'GROUPING SET'
        assert expression is not None
        expression.set_parent(self)
        self.grouping_sets.append(expression)

    def tree_str(self, indent: str = ''):
        with StringIO() as s:
            s.write(f'{indent}GROUP BY\n')
            sub_indent = indent + _INDENT
            for exp in self.group_by:
                s.write(exp.tree_str(sub_indent))
            for exp in self.grouping_sets:
                s.write(exp.tree_str(sub_indent))
            return s.getvalue()

    def recompose(self, sep: str = ' '):
        with StringIO() as s:
            kw = f' {self.keyword}' if self.keyword else ''
            if self.group_by:
                s.write(f'{sep}GROUP BY')
                if not self.post_syntax:
                    s.write(kw)
                s.write(sep)
                if self.with_parens:
                    s.write('(')
                s.write(', '.join(gb.recompose(sep) for gb in self.group_by))
                if self.with_parens:
                    s.write(')')
                if self.post_syntax:
                    s.write(kw)
            if self.post_keyword:
                s.write(f'{sep}{self.post_keyword}')
            if self.grouping_sets:
                s.write(f'{sep}GROUPING SETS (')
                s.write(f',{sep}'.join(
                    gs.recompose(sep) for gs in self.grouping_sets))
                s.write(')')
            return s.getvalue()

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed):
            return
        node = graph.node_by_source(self)
        for child in self.children:
            node.add_child(graph.node_by_source(child), LinkType.EXPRESSION)
            child.link_graph(graph, resolvers, processed)


class Query(Source):
    """A SELECT statement.

    Attributes:
       withs: queries or expressions introduced with a `WITH` clause. Their name
          denote their alias implied by associated `AS`.
       source: the main data source of the query. Can be composed with `JoinSource`.
       destination: if producing a table or a view, this contains the
           produced source.
       select_items: the expression and names selected in this query.
       where: the main `WHERE` filter expression.
       group_by: any `GROUP BY` clause contained.
       having: the `HAVING` post select filter expression.
       clauses: any associated clauses (`ORDER BY` and such).
       set_ops: associated queries, in a set-like operation w/ this one
           (e.g. `UNION`).
       limit: any associated `LIMIT` clause.
    """

    def __init__(self,
                 parent: Optional[Source] = None,
                 name: Optional[str] = None):
        super().__init__(parent, name)
        self.withs: List[Source] = []
        self.source: Source = None
        self.destination: Optional[Source] = None
        self.select_items = SelectItems(self)
        self.where: Optional[Expression] = None
        self.group_by: Optional[GroupByClause] = None
        self.having: Optional[Expression] = None
        self.clauses: List[QueryClause] = []
        self.set_ops: List['SetOpSelect'] = []
        self.limit: Optional[QueryLimit] = None

    def ensure_group_by(self):
        if not self.group_by:
            self.group_by = GroupByClause(self)
        return self.group_by

    def add_group_by(self,
                     exp: Optional[Expression] = None,
                     exp_num: Optional[int] = None):
        if exp_num is not None:
            assert exp is None
            if self.select_items.is_valid_group_by_index(exp_num):
                exp = Expression(self, None,
                                 self.select_items.children[exp_num - 1].name)
            else:
                exp = Expression(self, None, f'{exp_num}')
        self.ensure_group_by().add_group_by(exp)

    def tree_str(self, indent: str = ''):
        name_indent = indent + _INDENT
        sub_indent = name_indent + _INDENT
        with StringIO() as s:
            s.write(self.name_str(indent=indent))
            s.write('\n')
            for ws in self.withs:
                s.write(ws.name_str('WITH', name_indent))
                s.write('\n')
                s.write(ws.tree_str(sub_indent))
            if self.source:
                s.write(self.source.name_str('FROM', name_indent))
                s.write('\n')
                s.write(self.source.tree_str(sub_indent))
            s.write(self.select_items.tree_str(name_indent))
            if self.where:
                s.write(self.where.name_str('WHERE', name_indent))
                s.write('\n')
                s.write(self.where.tree_str(sub_indent))
            if self.group_by:
                s.write(self.group_by.tree_str(name_indent))
            if self.having:
                s.write(self.having.name_str('HAVING', name_indent))
                s.write('\n')
                s.write(self.having.tree_str(sub_indent))
            for clause in self.clauses:
                s.write(clause.name_str(indent=name_indent))
                s.write('\n')
                s.write(clause.tree_str(sub_indent))
            for set_op in self.set_ops:
                s.write(set_op.tree_str(name_indent))
            for lateral in self.laterals:
                s.write(lateral.tree_str(name_indent))
            return s.getvalue()

    def recompose(self, sep: str = ' '):
        with StringIO() as s:
            if self.withs:
                s.write(f'WITH{sep}')
                withs = []
                for ws in self.withs:
                    if isinstance(ws, Query):
                        withs.append(f'{ws.name} AS ({ws.recompose(sep)})')
                    else:
                        withs.append(f'{ws.recompose(sep)} AS {ws.name}')
                s.write(f',{sep}'.join(withs))
                s.write(sep)
            s.write(self.select_items.recompose(sep))
            s.write(f'{sep}')
            if self.source:
                s.write(f'FROM{sep}')
                if isinstance(self.source, Query):
                    s.write(f'({self.source.recompose(sep)})')
                    if self.source.name:
                        s.write(f'{sep}AS self.source.name')
                    s.write(self.source.laterals_recompose(sep))
                else:
                    s.write(self.source.recompose(sep))
            if self.where:
                s.write(f'{sep}WHERE{sep}{self.where.recompose(sep)}')
            if self.group_by:
                s.write(self.group_by.recompose(sep))
            if self.having:
                s.write(f'{sep}HAVING{sep}{self.having.recompose(sep)}')
            if self.limit:
                s.write(f'{sep}{self.limit.recompose()}')
            for clause in self.clauses:
                s.write(f'{sep}{clause.recompose(sep)}')
            for set_op in self.set_ops:
                s.write(f'{sep}{set_op.recompose(sep)}')
            return s.getvalue()

    def _recompose_source(self, source: Source, sep: str = ' '):
        if isinstance(source, Query):
            return f'({source.recompose(sep)}) AS {source.name}'

    def _local_resolve(self, name: str, in_scope: bool):
        if in_scope:
            resolved = self.select_items.resolve_name(name, True, False)
            if resolved:
                return resolved
        if self.source:
            return self.source.resolve_name(name, True, False)
        return self.select_items.resolve_name(name, False, False)

    def _find_source(self, name: str):
        for ws in self.withs:
            if ws.name == name:
                return ws
        if self.source:
            return self.source.resolve_name(name, False, True)
        return None

    def resolve_name(self, name: str, in_scope: bool, find_source: bool):
        if name == self.name or not name:
            return self
        if find_source:
            return self._find_source(name)
        parent_name, field_name = _split_name(name)
        local_resolve = (parent_name is not None and parent_name == self.name)
        if parent_name is None or local_resolve or in_scope:
            return self._local_resolve(field_name if local_resolve else name,
                                       in_scope or local_resolve)
        sub_resolver = self._find_source(parent_name)
        if not sub_resolver:
            return self._local_resolve(name, True)
        return sub_resolver.resolve_name(field_name, True, False)

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed):
            return
        node = graph.node_by_source(self)
        sub_resolvers = [self]
        sub_resolvers.extend(resolvers)
        for ws in self.withs:
            node.add_inlink(graph.node_by_source(ws), LinkType.WITH)
            ws.link_graph(graph, sub_resolvers, processed)
        if self.source:
            node.add_inlink(graph.node_by_source(self.source), LinkType.FROM)
            node.add_child(graph.node_by_source(self.source))
            self.source.link_graph(graph, sub_resolvers, processed)
        self.select_items.link_graph(graph, sub_resolvers, processed)
        node.add_child(graph.node_by_source(self.select_items), LinkType.OUTPUT)
        if self.where:
            node.add_child(graph.node_by_source(self.where),
                           LinkType.EXPRESSION_AUX)
            self.where.link_graph(graph, sub_resolvers, processed)
        if self.group_by:
            node.add_child(graph.node_by_source(self.group_by),
                           LinkType.EXPRESSION_AUX)
            self.group_by.link_graph(graph, sub_resolvers, processed)
        if self.having:
            node.add_child(graph.node_by_source(self.having),
                           LinkType.EXPRESSION_AUX)
            self.having.link_graph(graph, sub_resolvers, processed)
        for set_op in self.set_ops:
            node.add_inlink(graph.node_by_source(set_op), LinkType.SET_OP)
            set_op.link_graph(graph, resolvers, processed)
        for clause in self.clauses:
            node.add_child(graph.node_by_source(clause),
                           LinkType.EXPRESSION_AUX)
            clause.link_graph(graph, resolvers, processed)
        for lateral in self.laterals:
            node.add_child(graph.node_by_source(lateral), LinkType.SOURCE)
            lateral.link_graph(graph, resolvers, processed)

    def apply_schemas(self, schema_info: SchemaInfo):
        if (schema_info.output and self.destination and
                isinstance(self.destination, Table)):
            self.destination.set_schema(schema_info.output)
            schema_info.output = None
        super().apply_schemas(schema_info)


class SetOpQuery(Query):
    """A select statment which is an operand to a set operation w/ the main select.

    Attributes:
       set_op: the set operation to apply to this query when processing e.g. `UNION`
    """

    def __init__(self, set_op: str, parent: Source, name: Optional[str] = None):
        super().__init__(parent, name)
        self.set_op = set_op.upper()

    def tree_str(self, indent: str = ''):
        return (f'{indent}SET OPERATION: {self.set_op}\n' +
                super().tree_str(indent))

    def recompose(self, sep: str = ' '):
        return f'{self.set_op}{sep}{super().recompose(sep)}'


class GeneralStatement(Source):
    """A general SQL statement, w/o specific information extracted.

    Attributes:
        statement_tokens: the list of tokens in this statement.
        statement: the statement tokens as a string.
    """

    def __init__(self, parent: Optional[Source], name: Optional[str],
                 statement_tokens: tokens.Tokens):
        super().__init__(parent, name)
        self.statement_tokens = statement_tokens
        self.statement, _, _ = tokens.recompose(statement_tokens)

    def recompose(self, sep: str = ' '):
        del sep
        return self.statement

    def link_graph(self, graph: Graph, resolvers: List['Source'],
                   processed: Set['Source']):
        if self._is_processed(processed):
            return
        node = graph.node_by_source(self)
        for child in self.children:
            child.link_graph(graph, resolvers, processed)
            node.add_inlink(graph.node_by_source(child), LinkType.INCLUDED_AUX)


class StatementWithQuery(GeneralStatement):
    """A statement that wraps a query and writes to a destination."""

    def __init__(self,
                 parent: Optional[Source],
                 name: Optional[str],
                 destination: Source,
                 statement_tokens: tokens.Tokens,
                 query: Optional[Query] = None):
        super().__init__(parent, name, statement_tokens)
        self.destination = destination
        self.query = query
        if query is not None:
            query.set_parent(self)
        if destination:
            destination.set_parent(self)

    def apply_schemas(self, schema_info: SchemaInfo):
        if (schema_info.output and self.destination and
                isinstance(self.destination, Table)):
            self.destination.set_schema(schema_info.output)
            schema_info.output = None
        if self.query:
            self.query.apply_schemas(schema_info)


class Create(StatementWithQuery):
    """A statement that creates a view / table (possible via a query).

    Attributes:
        destination: the source created by this statement (e.g. a `Table` or `View`).
        query: if this create statement includes a query as a source, this contains
            that query.
        schema: if schema is specified in the create statement, this contains that
            schema.
        name: contains the type of the statement e.g. `CREATE TABLE` or
            `CREATE MATERIALIZED VIEW`.
    """

    def __init__(self,
                 parent: Optional[Source],
                 name: Optional[str],
                 destination: Source,
                 statement_tokens: tokens.Tokens,
                 query: Optional[Query] = None,
                 schema: Optional[Schema.Table] = None):
        super().__init__(parent, name, destination, statement_tokens, query)
        self.schema = schema
        self.using_format = None
        self.input_path = None
        self.location_path = None
        self.options: Optional[Dict[str]] = None


class Insert(StatementWithQuery):
    """A statemenent that inserts into a table.

    Attributes:
        destination: the source in which this statement inserts
            (e.g. a `Table` or `View`).
        query: if this create statement includes a query as a source,
            this contains that query.
    """
    pass


def find_end_sources(query: Source) -> List[Table]:
    """Returns the set of bottom-end sources of a statement."""
    graph = Graph()
    graph.populate(query)
    query.link_graph(graph, [], set())
    end_sources = []
    dest = find_destination(query)
    for src in graph.src2id:
        if isinstance(src, Table) and src.is_end_source and src != dest:
            end_sources.append(src)
    return end_sources


def find_destination(query: Source) -> Source:
    """Returns the final destination of a statement."""
    if isinstance(query, StatementWithQuery):
        return query.destination
    if isinstance(query, Query):
        return query.destination
    return None
