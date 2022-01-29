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
"""Module for graphing SQL statements."""
import dataclasses
import graphviz
import os
import sys
import tempfile

from contextlib import contextmanager
from sql_analyze.grammars import statement, trees
from typing import Dict, List, Optional, Set, Tuple


def _split_in_lines(s, slen, joiner='\n'):
    last_start = 0
    ss = ''
    crt = 0
    for c in s:
        ss += c
        crt += 1
        if c in {'(', ' ', ')', '[', ']', '{', '}', ','}:
            if crt - last_start > slen and len(s) - crt > int(slen * 1.2):
                ss += joiner
                last_start = crt
    return ss


_LINK_STYLE = {
    statement.LinkType.INCLUDED: 'dotted',
    statement.LinkType.INCLUDED_AUX: 'dashed',
    statement.LinkType.EXPRESSION: 'dotted',
    statement.LinkType.EXPRESSION_AUX: 'dashed',
    statement.LinkType.LINEAGE: 'dashed',
    statement.LinkType.AS: 'dashed',
    statement.LinkType.FROM: 'solid',
    statement.LinkType.WITH: 'dotted',
    statement.LinkType.SOURCE: 'solid',
    statement.LinkType.OUTPUT: 'dashed',
    statement.LinkType.SET_OP: 'solid',
}


def _link_dot_style(link):
    return _LINK_STYLE.get(link.link_type, 'solid')


_LINK_COLOR = {
    statement.LinkType.INCLUDED: 'seagreen4',
    statement.LinkType.INCLUDED_AUX: 'seagreen4',
    statement.LinkType.EXPRESSION: 'webmaroon',
    statement.LinkType.EXPRESSION_AUX: 'webmaroon',
    statement.LinkType.LINEAGE: 'slateblue',
    statement.LinkType.AS: 'black',
    statement.LinkType.FROM: 'black',
    statement.LinkType.WITH: 'plum1',
    statement.LinkType.SOURCE: 'black',
    statement.LinkType.OUTPUT: 'steelblue',
    statement.LinkType.SET_OP: 'blue',
}


def _link_dot_color(link):
    return _LINK_COLOR.get(link.link_type, 'black')


_LINK_ARROW = {
    statement.LinkType.INCLUDED: 'none',
    statement.LinkType.INCLUDED_AUX: 'normal',
    statement.LinkType.EXPRESSION: 'normal',
    statement.LinkType.EXPRESSION_AUX: 'vee',
    statement.LinkType.LINEAGE: 'halfopen',
    statement.LinkType.AS: 'normal',
    statement.LinkType.FROM: 'vee',
    statement.LinkType.WITH: 'ediamond',
    statement.LinkType.SOURCE: 'vee',
    statement.LinkType.OUTPUT: 'vee',
    statement.LinkType.SET_OP: 'dot',
}


def _link_dot_arrowhead(link):
    return _LINK_ARROW.get(link.link_type, 'vee')


_LINK_NAME = {
    statement.LinkType.WITH: 'WITH',
    statement.LinkType.FROM: 'FROM',
    statement.LinkType.AS: 'AS',
    statement.LinkType.SOURCE: 'SOURCE',
}


def _link_dot_name(link):
    if link.link_type == statement.LinkType.SET_OP:
        return link.src.source.set_op
    return _LINK_NAME.get(link.link_type, '')


def _link_dot_minlen(link):
    if link.link_type in (statement.LinkType.FROM,
                          statement.LinkType.EXPRESSION,
                          statement.LinkType.EXPRESSION_AUX,
                          statement.LinkType.AS):
        return 1
    if link.link_type == statement.LinkType.SOURCE:
        if (isinstance(link.src.source, statement.Query) or
                isinstance(link.dst.source, statement.Query)):
            return 3
    return 1


def _link_dot_weight(link):
    if link.link_type in (statement.LinkType.FROM, statement.LinkType.SOURCE,
                          statement.LinkType.AS):
        return 5
    return 1


DETAIL_MIN = 0
# We start showing join expressions
DETAIL_JOIN_EXPRESSION = 2
# We start showing GROUP BY / HAVING expressions
DETAIL_SPECIAL_EXPRESSIONS = 3
# We start showing select items as a group
DETAIL_SELECT_ITEMS_GROUP = 4
# We start showing select items separately
DETAIL_SELECT_ITEMS = 5
# We show all expressions
DETAIL_ALL_EXPRESSIONS = 6
# We start showing more links
DETAIL_LINKS_WITH = 7
DETAIL_LINKS_LINEAGE = 8
DETAIL_GROUP_BY = 8
DETAIL_LINKS_INCLUDED = 9
DETAIL_MAX = 10

CLEAR_COLOR = '#00000000'


@dataclasses.dataclass
class DotNodeAttr:
    """Arguments for a dot node - all values must be strings."""
    comment: Optional[str] = None
    shape: str = 'box'
    margin: Optional[str] = None
    # Colors from: https://graphviz.org/doc/info/colors.html
    color: str = 'black'
    fontname: str = 'Helvetica'
    # Font size in 'pt' or pixels
    fontsize: str = '12pt'
    fontcolor: str = 'black'
    # styles: filled, invisible, diagonals, rounded. dashed, dotted, solid and bold
    style: str = 'filled'
    fillcolor: str = 'beige'
    pencolor: Optional[str] = None
    penwidth: Optional[str] = None
    # height / width in inches
    height: Optional[str] = None
    width: Optional[str] = None
    fixedsize: Optional[str] = None
    # style class for the node
    style_class: List[str] = dataclasses.field(default_factory=list)

    def make_invisible(self):
        self.color = CLEAR_COLOR
        self.fontcolor = CLEAR_COLOR
        self.fillcolor = CLEAR_COLOR
        self.pencolor = CLEAR_COLOR
        return self

    def make_highlighted(self):
        self.add_style('highlighted')

    def add_style(self, name: str, is_subgraph: Optional[bool] = None):
        if not name:
            return self
        if is_subgraph:
            self.style_class.append(f'{name}_subgraph')
        else:
            self.style_class.append(name)
        return self

    def apply_style(self, styles: Dict[str, Dict[str, str]],
                    options: 'DotOptions'):
        for style in self.style_class:
            if style in styles:
                for k, v in styles[style].items():
                    setattr(self, k, v)
        if self.shape == 'directional':
            self.shape = options.directional_shape()
        self.style_class = ' '.join(self.style_class)
        return self

    def as_style_dict(self, styles: Dict[str, Dict[str, str]],
                      options: 'DotOptions'):
        attrs = dataclasses.asdict(self.apply_style(styles, options))
        if options.attrs.stylesheet:
            attrs['class'] = attrs['style_class']
        del attrs['style_class']
        return {k: str(v) for k, v in attrs.items() if v is not None}


_STYLES = {
    'subgraph': {
        'fontsize': '14pt',
        'fontcolor': 'darkmagenta',
        'fillcolor': 'azure',
        'style': 'filled,bold,rounded',
    },
    'column': {
        'shape': 'note',
        'fillcolor': 'mistyrose',
        'color': 'mediumvioletred',
    },
    'WHERE': {
        'margin': '0.25',
        'color': 'teal',
        'fillcolor': 'turquoise1',
    },
    'GROUP BY': {
        'margin': '0.25',
        'color': 'royalblue3',
        'fillcolor': 'paleturquoise1',
    },
    'HAVING': {
        'margin': '0.25',
        'color': 'purple3',
        'fillcolor': 'plum1',
    },
    'expression': {
        'shape': 'directional',
        'margin': '0.2',
        'fillcolor': 'thistle2',
        'fontname': 'Courier New',
        'fontsize': '10pt',
    },
    'table': {
        'fontsize': '13pt',
        'fillcolor': 'palegreen1',
        'color': 'olivedrab4',
        'fontcolor': 'olivedrab4',
        'shape': 'cylinder',
        'margin': '0.2',
    },
    'ref_source': {
        'shape': 'directional',
    },
    'join_expression': {
        'shape': 'component',
        'fillcolor': 'oldlace',
    },
    'join_source': {
        'shape': 'diamond',
        'style': 'filled,rounded',
        'fillcolor': 'orange1',
        'color': 'orangered3',
        'fontcolor': 'orangered3',
    },
    'join_source_subgraph': {
        'fillcolor': 'lightyellow',
    },
    'select_item': {
        'shape': 'directional',
        'fillcolor': 'skyblue1',
        'fontsize': '12pt',
        'fontname': 'Courier New',
        'color': 'steelblue4',
    },
    'select_items_large': {
        'fillcolor': 'skyblue1',
        'fontsize': '12pt',
        'fontname': 'Courier New',
    },
    'select_items_subgraph': {
        'fillcolor': 'paleturquoise1',
    },
    'select_items': {
        'fillcolor': 'cornflowerblue',
        'shape': 'circle',
        'height': '0.2',
        'weight': '0.2',
        'fixedsize': 'true',
    },
    'lateral_view_subgraph': {
        'color': 'forestgreen',
        'fontcolor': 'forestgreen',
        'fillcolor': '#e0f0e7',
    },
    'lateral_view': {
        'fontsize': '10pt',
        'fontname': 'Courier New',
        'shape': 'tab',
        'color': 'forestgreen',
    },
    'pivot_subgraph': {
        'color': 'forestgreen',
        'fontcolor': 'forestgreen',
        'fillcolor': '#e0f0e7',
    },
    'pivot': {
        'shape': 'diamond',
        'fontcolor': 'forestgreen',
        'color': 'forestgreen',
    },
    'group_by': {
        'fillcolor': 'mistyrose',
        'color': 'maroon3',
        'fontcolor': 'maroon3',
    },
    'group_by_subgraph': {
        'fillcolor': 'floralwhite',
        'color': 'maroon3',
    },
    'group_clause': {
        'fontsize': '10pt',
        'fontname': 'Courier New',
        'fillcolor': 'lavenderblush',
        'color': 'maroon3',
    },
    'group_clause_subgraph': {
        'color': CLEAR_COLOR,
        'fontcolor': CLEAR_COLOR,
        'fillcolor': CLEAR_COLOR,
        'pencolor': CLEAR_COLOR,
    },
    'query_subgraph': {},
    'query': {
        'shape': 'folder',
        'fillcolor': 'wheat1',
        'color': 'webmaroon',
        'margin': '0.2',
    },
    'highlighted': {
        'penwidth': '10.0',
        'pencolor': 'fuchsia',
        'color': 'fuchsia',
        'fontsize': '24pt',
        'margin': '0.75',
    },
    'highlighted_edge': {
        'penwidth': '6.0',
        'pencolor': 'fuchsia',
        'color': 'fuchsia',
        'fontsize': '24pt',
    },
}


class DotSource:
    """Dot formatting for statement sources."""

    def __init__(self, source: statement.Source):
        self.source = source

    def hide_node(self, options: 'DotOptions'):
        del options
        return False

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        del options
        attrs = DotNodeAttr(comment=self.source.name_str())
        if is_subgraph:
            attrs.add_style('subgraph')
        return attrs

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        del options
        del is_subgraph
        if self.source.name:
            return self.source.name
        else:
            return type(self.source).__name__

    def svg_id(self, prefix: str, number: int):
        root_id = f'{prefix}_{number}'
        if not self.source.start or not self.source.stop:
            return root_id
        start = f'{self.source.start.line}-{self.source.start.column}'
        stop = f'{self.source.stop.line}-{self.source.stop.column}'
        return f'{root_id}_{start}_{stop}'


class DotColumn(DotSource):
    """Dot formatting for statement.Column sources."""

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        return super().dot_attr(is_subgraph, options).add_style('column')

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return ''
        return super().dot_label(False, options)


class DotExpression(DotSource):
    """Dot formatting for statement.Expression sources."""

    def hide_node(self, options: 'DotOptions'):
        if (isinstance(self.source.parent, statement.SelectItemExpression) and
                self.source.name is None and self.source.parent.name is None):
            return True
        if isinstance(self.source.parent, statement.JoinExpression):
            return options.detail_level < DETAIL_JOIN_EXPRESSION
        if self.source.has_role() and self.source.role not in ('GROUP BY'):
            return options.detail_level < DETAIL_SPECIAL_EXPRESSIONS
        return options.detail_level < DETAIL_ALL_EXPRESSIONS

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return ''
        content = self.source.expression_block.format(
            options.expression_line_length)
        content = content.replace('\n', '\\l')
        if isinstance(self.source.parent,
                      statement.PivotView) and self.source.name:
            content += f' AS {self.source.name}\\l'
        if self.source.has_role(
        ) and self.source.role != self.source.parent.name:
            return f'{self.source.role}\\l{content}\\l'
        return content

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        return super().dot_attr(is_subgraph,
                                options).add_style('expression').add_style(
                                    self.source.role)


class DotTable(DotSource):
    """Dot formatting for statement.Table sources."""

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return ''
        return self.source.source_str()

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        attr = super().dot_attr(is_subgraph, options)
        if not is_subgraph:
            attr.add_style('table')
            if not self.source.is_end_source:
                attr.add_style('ref_source')
        return attr


class DotJoinExpression(DotSource):
    """Dot formatting for statement.JoinExpression sources."""

    def hide_node(self, options: 'DotOptions'):
        return options.detail_level < DETAIL_JOIN_EXPRESSION

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return ''
        return f'<<B>{self.source.name}</B>>'

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        if options.detail_level < DETAIL_SPECIAL_EXPRESSIONS and is_subgraph:
            return super().dot_attr(is_subgraph, options).make_invisible()
        if not is_subgraph and self.source.expression:
            return make_dot_node(self.source.expression).dot_attr(
                False, options)
        return super().dot_attr(is_subgraph,
                                options).add_style('join_expression')


class DotJoinSource(DotSource):
    """Dot formatting for statement.JoinSource sources."""

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return ''
        return 'Joint'

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        return super().dot_attr(is_subgraph,
                                options).add_style('join_source', is_subgraph)


class DotSelectItem(DotSource):
    """Dot formatting for statement.SelectItem sources."""

    def hide_node(self, options: 'DotOptions'):
        return options.detail_level < DETAIL_SELECT_ITEMS

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        return super().dot_attr(is_subgraph, options).add_style('select_item')


class DotSelectItemExpression(DotSelectItem):
    """Dot formatting for statement.SelectItemExpression sources."""

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return ''
        if self.source.name is None:
            return make_dot_node(self.source.expression).dot_label(
                False, options)
        return self.source.name


class DotSelectItemAllColumns(DotSelectItem):
    """Dot formatting for statement.SelectItemAllColumns sources."""

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return ''
        return self.source.recompose()


class DotSelectItems(DotSource):
    """Dot formatting for statement.SelectItems."""

    def hide_node(self, options: 'DotOptions'):
        return options.detail_level < DETAIL_SELECT_ITEMS_GROUP

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        attr = super().dot_attr(is_subgraph, options)
        if options.detail_level < DETAIL_SELECT_ITEMS:
            if is_subgraph:
                return attr.make_invisible()
            return attr.add_style('select_items_large')
        return attr.add_style('select_items', is_subgraph)

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return ''
        if options.detail_level < DETAIL_SELECT_ITEMS:
            return '\\l'.join(
                make_dot_node(child).dot_label(False, options)
                for child in self.source.children) + '\\l'
        return ''


class DotQueryClause(DotSource):
    """Dot formatting for query clauses."""

    def hide_node(self, options: 'DotOptions'):
        return options.detail_level < DETAIL_SPECIAL_EXPRESSIONS

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return ''
        return self.source.name

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        attr = super().dot_attr(is_subgraph, options)
        if is_subgraph:
            return attr.make_invisible()
        return attr


class DotLateralView(DotSource):
    """Dot formatting for lateral source clauses."""

    def hide_node(self, options: 'DotOptions'):
        return options.detail_level < DETAIL_JOIN_EXPRESSION

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return self.source.keyword
        return self.source.expression_str()

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        return super().dot_attr(is_subgraph,
                                options).add_style('lateral_view', is_subgraph)


class DotPivotView(DotSource):
    """Dot formatting for PIVOT source clauses."""

    def hide_node(self, options: 'DotOptions'):
        return options.detail_level < DETAIL_JOIN_EXPRESSION

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return ''
        return self.source.name

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        return super().dot_attr(is_subgraph,
                                options).add_style('pivot', is_subgraph)


class DotGroupByClause(DotSource):
    """Dot formatting for a group by set of clauses."""

    def hide_node(self, options: 'DotOptions'):
        return options.detail_level < DETAIL_SPECIAL_EXPRESSIONS

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return ''
        if options.detail_level >= DETAIL_GROUP_BY:
            return self.source.name
        content = 'GROUP BY\n  '
        content += ',\n  '.join(
            child.expression_block.format(options.expression_line_length)
            for child in self.source.children) + '\n'
        content = content.replace('\n', '\\l')
        return content

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        attr = super().dot_attr(is_subgraph, options)
        if options.detail_level >= DETAIL_GROUP_BY:
            return attr.add_style('group_by', is_subgraph)
        else:
            return attr.add_style('group_clause', is_subgraph)


class DotQuery(DotSource):
    """Dot formatting for statement.Query sources."""

    def dot_label(self, is_subgraph: bool, options: 'DotOptions'):
        if is_subgraph:
            return ''
        kw = ''
        if self.source.select_items.keyword:
            kw = f' {self.source.select_items.keyword}'
        body = f'<B>SELECT{kw}</B>'
        if self.source.name:
            body += f'<br/>AS {self.source.name}'
        if self.source.limit:
            body += f'<br/>{self.source.limit.recompose()}'
        return f'<{body}>'

    def dot_attr(self, is_subgraph: bool, options: 'DotOptions'):
        return super().dot_attr(is_subgraph,
                                options).add_style('query', is_subgraph)


_NODE_DOT_CLASS = [
    (statement.Column, DotColumn),
    (statement.Expression, DotExpression),
    (statement.Table, DotTable),
    (statement.JoinExpression, DotJoinExpression),
    (statement.JoinSource, DotJoinSource),
    (statement.SelectItems, DotSelectItems),
    (statement.SelectItemExpression, DotSelectItemExpression),
    (statement.SelectItemAllColumns, DotSelectItemAllColumns),
    (statement.SelectItem, DotSelectItem),
    (statement.Query, DotQuery),
    (statement.QueryClause, DotQueryClause),
    (statement.LateralView, DotLateralView),
    (statement.PivotView, DotPivotView),
    (statement.GroupByClause, DotGroupByClause),
    (statement.Source, DotSource),
]


def make_dot_node(source: statement.Source):
    for src, dot in _NODE_DOT_CLASS:
        if isinstance(source, src):
            return dot(source)
    raise ValueError(f'Node source cannot be converted to dot: `{source}`')


def _node_dot_name(node: statement.GraphNode, prefix: str = 'Node'):
    return f'{prefix}_{node.node_id}'


_STR2LINK = {
    'INCLUDED': statement.LinkType.INCLUDED,
    'INCLUDED_AUX': statement.LinkType.INCLUDED_AUX,
    'EXPRESSION': statement.LinkType.EXPRESSION,
    'LINEAGE': statement.LinkType.LINEAGE,
    'AS': statement.LinkType.AS,
    'FROM': statement.LinkType.FROM,
    'WITH': statement.LinkType.WITH,
    'SOURCE': statement.LinkType.SOURCE,
    'OUTPUT': statement.LinkType.OUTPUT,
    'SET_OP': statement.LinkType.SET_OP,
}
_MAX_TB_INGRADE = 6
_MAX_TB_NODES = 50


@dataclasses.dataclass
class GraphAttrs:
    """Top level attributes for a graph - exact names and meanings as dot spec."""
    compound: str = 'true'
    size: str = 'ideal'
    esep: str = '5'
    rankdir: str = 'detect'
    ratio: str = 'auto'
    newrank: str = 'false'
    clusterrank: str = 'local'
    concentrate: str = 'false'
    ranksep: str = '0.5'
    URL: str = None  # pylint: disable=invalid-name
    stylesheet: str = None

    def as_dict(self):
        return {
            k: str(v)
            for k, v in dataclasses.asdict(self).items()
            if v is not None
        }


class DotOptions:
    """General options for formatting a dot graph output."""

    def __init__(self,
                 detail_level: int = DETAIL_ALL_EXPRESSIONS,
                 expression_line_length: int = 40,
                 attrs: GraphAttrs = None):
        self.detail_level = detail_level
        self.expression_line_length = expression_line_length
        self.hide_link_types = set()
        self.mute_style = False
        if attrs is None:
            attrs = GraphAttrs()
        self.attrs = attrs
        self.label = None
        self.sql_tooltips = True
        if detail_level < DETAIL_LINKS_WITH:
            self.hide_link_types.add(statement.LinkType.WITH)
        if detail_level < DETAIL_LINKS_INCLUDED:
            self.hide_link_types.add(statement.LinkType.INCLUDED)
        if detail_level < DETAIL_LINKS_LINEAGE:
            self.hide_link_types.add(statement.LinkType.LINEAGE)
        self.start_highlight: Optional[Tuple[int, int]] = None
        self.stop_highlight: Optional[Tuple[int, int]] = None
        self.highlighted_nodes = set()
        self._hidden_nodes = set()

    def _hide_source(self, source: 'Source') -> bool:
        if make_dot_node(source).hide_node(self):
            return True
        if source.parent:
            return self._hide_source(source.parent)
        return False

    def hide_node(self, node: statement.GraphNode) -> bool:
        if node.node_id in self._hidden_nodes:
            return True
        if self.is_highlighted(node, False):
            return False
        if self._hide_source(node.source):
            self._hidden_nodes.add(node.node_id)
            return True
        return False

    def hide_link(self, link: statement.GraphLink) -> bool:
        if self.is_highlighted_link(link):
            return False
        if link.link_type in self.hide_link_types:
            return True
        if self.hide_node(link.src) or self.hide_node(link.dst):
            return True
        return False

    def is_highlighted_link(self, link: statement.GraphLink) -> bool:
        return (self.is_highlighted(link.src, False) and
                self.is_highlighted(link.dst, False))

    def in_limits(self, pos: trees.CodeLocation):
        # pylint: disable=unsubscriptable-object
        return ((pos.line > self.start_highlight[0] or
                 (pos.line == self.start_highlight[0] and
                  pos.column >= self.start_highlight[1])) and
                (pos.line < self.stop_highlight[0] or
                 (pos.line == self.stop_highlight[0] and
                  pos.column <= self.start_highlight[1])))

    def is_highlighted(self, node: statement.GraphNode,
                       is_subgraph: bool) -> bool:
        if not is_subgraph and node in self.highlighted_nodes:
            return True
        if not self.start_highlight or not self.stop_highlight:
            return False
        if not node.source.start or not node.source.stop:
            return False
        if (self.in_limits(node.source.start) or
                self.in_limits(node.source.stop)):
            return True
        return False

    def max_display_ingrade(self, graph: statement.Graph):
        max_ingrade = (0, None)
        for node in graph.id2node.values():
            if self.hide_node(node):
                continue
            ingrade = sum(
                not self.hide_link(link) for link in node.children) + sum(
                    not self.hide_link(link) for link in node.inlinks)
            if ingrade > max_ingrade[0]:
                max_ingrade = (ingrade, node)
        return max_ingrade

    def num_display_nodes(self, graph: statement.Graph):
        return sum(not self.hide_node(node) for node in graph.id2node.values())

    def detect_rankdir(self, graph: statement.Graph):
        if self.attrs.rankdir != 'detect':
            return
        max_ingrade, _ = self.max_display_ingrade(graph)
        num_display = self.num_display_nodes(graph)
        if max_ingrade <= _MAX_TB_INGRADE and num_display <= _MAX_TB_NODES:
            self.attrs.rankdir = 'TB'
        self.attrs.rankdir = 'RL'

    def directional_shape(self):
        if self.attrs.rankdir == 'TB':
            return 'box'
        if self.attrs.rankdir == 'BT':
            return 'box'
        if self.attrs.rankdir == 'RL':
            return 'larrow'
        return 'cds'


class DotState:
    """Helper class for building a graphviz dot representation. """

    def __init__(self,
                 graph: statement.Graph,
                 options: Optional[DotOptions] = None):
        self.graph = graph
        if options is None:
            options = DotOptions()
        self.options = options
        self.nodes_processed: Set[int] = set()
        self.links_added: Set[Tuple[int, int]] = set()
        self.dot = graphviz.graphs.Digraph()
        options.detect_rankdir(graph)
        attrs = options.attrs.as_dict()
        self.dot.attr(**attrs)
        if options.label:
            self.dot.attr(
                label=options.label,
                fontname='Helvetica',
                fontcolor='webpurple',
                fontsize='16pt',
                labelloc='t',
                labeljust='l',
            )
        self.dot_group = None

    def start_node(self, node: statement.GraphNode):
        if self.options.hide_node(node):
            return False
        if node.node_id in self.nodes_processed:
            return False
        self.nodes_processed.add(node.node_id)
        return True

    def start_edge(self, link: statement.GraphLink):
        if self.options.hide_link(link):
            return False
        key = (link.src.node_id, link.dst.node_id)
        if key in self.links_added:
            return False
        self.links_added.add(key)
        return True

    def reprocess(self):
        self.nodes_processed = set()
        self.links_added = set()

    @contextmanager
    def subgraph(self, node: statement.GraphNode):
        dot = self.dot
        src = make_dot_node(node.source)
        with dot.subgraph(name=_node_dot_name(node, 'cluster')) as sub_dot:
            self.dot = sub_dot
            if self.options.mute_style:
                attrs = {}
            else:
                attrs_object = src.dot_attr(True, self.options)
                if self.options.is_highlighted(node, True):
                    attrs_object.make_highlighted()
                attrs = attrs_object.as_style_dict(_STYLES, self.options)
            attrs['label'] = src.dot_label(True, self.options)
            attrs['id'] = src.svg_id('cluster', node.node_id)
            if self.options.sql_tooltips:
                attrs['tooltip'] = src.source.recompose()
            else:
                attrs['tooltip'] = attrs['label']
            sub_dot.attr(**attrs)
            yield self
        self.dot = dot

    @contextmanager
    def subgroup(self, subgroup: str):
        group = self.dot_group
        self.group = subgroup
        yield self
        self.group = group

    def build_dot(self):
        """Generates a dot source for this graph."""
        self.reprocess()
        for node in self.graph.id2node.values():
            self.add_dot_node(node)
        self.reprocess()
        for node in self.graph.id2node.values():
            self.add_dot_links(node)
        return self.dot.source

    def create_dot_node(self, node: statement.GraphNode):
        src = make_dot_node(node.source)
        label = src.dot_label(False, self.options)
        if self.options.mute_style:
            attrs = {}
        else:
            attrs_object = src.dot_attr(False, self.options)
            if self.options.is_highlighted(node, False):
                attrs_object.make_highlighted()
            attrs = attrs_object.as_style_dict(_STYLES, self.options)
        if self.dot_group:
            attrs['group'] = self.dot_group
        if node.node_id == 1:
            attrs['root'] = 'true'
        if self.options.sql_tooltips:
            attrs['tooltip'] = src.source.recompose()
        elif label:
            attrs['tooltip'] = label
        attrs['id'] = src.svg_id('cluster', node.node_id)
        attrs['label'] = label
        self.dot.node(_node_dot_name(node), **attrs)

    def add_dot_node(self, node: statement.GraphNode):
        if not self.start_node(node):
            return
        if not node.children:
            self.create_dot_node(node)
            return
        with self.subgraph(node) as sub_dot:
            sub_dot.create_dot_node(node)
            sub_group = _node_dot_name(node, 'group')
            for child in node.children:
                gclass = ''
                if (isinstance(child.src.source, statement.Expression) and
                        child.src.source.has_role()):
                    gclass = f'_{child.src.source.role}'
                elif isinstance(child.src.source, statement.SelectItem):
                    gclass = '_SelectItem'
                else:
                    gclass = f'_{child.link_type}'
                with sub_dot.subgroup(f'{sub_group}{gclass}'):
                    sub_dot.add_dot_node(child.src)
                if sub_dot.start_edge(child):
                    sub_dot.dot.edge(_node_dot_name(child.src),
                                     _node_dot_name(node),
                                     **self.link_dot_attrs(child))

    def add_dot_links(self, node: statement.GraphNode):
        if not self.start_node(node):
            return
        for link in node.inlinks:
            if self.start_edge(link):
                self.dot.edge(_node_dot_name(link.src), _node_dot_name(node),
                              **self.link_dot_attrs(link))

    def link_dot_attrs(self, link: statement.GraphLink):
        if self.options.mute_style:
            return {}
        label = _link_dot_name(link)
        attrs = {
            'label': label,
            'style': _link_dot_style(link),
            'minlen': str(_link_dot_minlen(link)),
            'weight': str(_link_dot_weight(link)),
            'color': _link_dot_color(link),
            'arrowhead': _link_dot_arrowhead(link),
            'fontname': 'Helvetica',
            'tooltip': label,
        }
        if (self.options.is_highlighted(link.src, False) and
                self.options.is_highlighted(link.dst, False)):
            attrs.update(_STYLES['highlighted_edge'])
        return attrs


def generate_svg(query: statement.Query,
                 dot_file: Optional[str] = None,
                 svg_file: Optional[str] = None,
                 graph_dir: Optional[str] = None,
                 graph_detail: Optional[int] = DETAIL_SELECT_ITEMS,
                 open_file: bool = False,
                 label: Optional[str] = None,
                 url: Optional[str] = None,
                 line_len: int = 60,
                 mute_style: bool = False):
    """Generates an SVG file from a query statement.
    0 requires `dot` program for svg, else pass svg_file='-',
    Optionally views the file (on macos) if `open_file`."""
    graph = statement.Graph()
    graph.populate(query)
    query.link_graph(graph, [], set())

    tmp_dir = None
    if not dot_file or not svg_file:
        tmp_dir = tempfile.mkdtemp()
    dot_file_name = (dot_file if dot_file else os.path.join(
        tmp_dir, 'graph.dot'))
    svg_file_name = (svg_file if svg_file else os.path.join(
        tmp_dir, 'graph.svg'))
    if graph_detail < DETAIL_MIN:
        graph_detail = DETAIL_MIN
    elif graph_detail > DETAIL_MAX:
        graph_detail = DETAIL_MAX
    options = DotOptions(graph_detail)
    options.expression_line_length = line_len
    options.mute_style = mute_style
    if label:
        options.label = label
    if graph_dir:
        options.attrs.rankdir = graph_dir
    if url:
        options.attrs.URL = url
    with open(dot_file_name, mode='w', encoding='utf-8') as dot_file_ob:
        state = DotState(graph, options)
        dot_file_ob.write(state.build_dot())
    if svg_file == '-':
        return (dot_file_name, None)
    cmd = f'dot -T svg {dot_file_name} > {svg_file_name}'
    if os.system(cmd):
        sys.exit(f'Error running dot command: "{cmd}"')
    if open_file:
        os.system(f'open {svg_file_name}')
    return (dot_file_name, svg_file_name)
