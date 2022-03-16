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
"""Utilities for processing antlr syntax trees."""
from dataclasses import dataclass
from io import StringIO
from antlr4 import Parser
from antlr4.Token import Token
from antlr4.tree import Trees
from antlr4.tree.Tree import ErrorNode, RuleNode, TerminalNode, Tree
from sql_analyze.grammars import tokens
from typing import Iterable, Optional, List


def has_sub_children(t: Tree) -> bool:
    """Returns true of this node has children on a second level below."""
    for child in t.getChildren():
        if child.getChildCount():
            return True
    return False


def _tree_type(t: Tree, parser: Parser) -> str:
    s = ''
    if isinstance(t, RuleNode):
        s += 'R'
    elif isinstance(t, ErrorNode):
        s += 'E'
    elif isinstance(t, TerminalNode):
        s += 'T'
        if isinstance(t.getPayload(), Token):
            s += f'OK `{parser.symbolicNames[t.getPayload().type]}`'
    else:
        s += 'X'
    return s


def to_string(tree: Tree, parser: Parser, indent='', first_indent='') -> str:
    """Converts a tree to a multiline string w/ visual indentation depth."""
    s = _tree_type(tree, parser) + ' ' + Trees.Trees.getNodeText(
        tree, parser.ruleNames)
    num_children = tree.getChildCount()
    if not num_children:
        return s
    sub_children = has_sub_children(tree)
    sub_indent = indent + '  |   '
    sub_first_indent = indent + '  +-- '
    with StringIO() as buf:
        if first_indent:
            buf.write(f'{first_indent}{s}\n')
        else:
            buf.write(f'{indent}+-- {s}\n')
        if not sub_children:
            buf.write(f'{indent}    +-- ')
            buf.write(' '.join(
                to_string(child, parser, sub_indent)
                for child in tree.getChildren()))
            buf.write('\n')
        else:
            for i in range(num_children):
                child = tree.getChild(i)
                has_children = (child.getChildCount() > 0)
                if i + 1 < num_children:
                    crt_indent = sub_indent
                else:
                    crt_indent = indent + '    '
                if not has_children:
                    buf.write(indent + '  +-- ')
                buf.write(to_string(child, parser, crt_indent,
                                    sub_first_indent))
                if not has_children:
                    buf.write('\n')
        return buf.getvalue()


@dataclass
class CodeLocation:
    """Defines a code location."""
    position: Optional[int]
    line: Optional[int]
    column: Optional[int]


def start_location(tree: Tree) -> CodeLocation:
    """Return the location of the first token / character in a tree."""
    if isinstance(tree, TerminalNode):
        token = tree.getPayload()
        if isinstance(token, Token):
            return CodeLocation(token.start, token.line, token.column)
    if not tree.children:
        return None
    return start_location(tree.children[0])


def stop_location(tree: Tree) -> CodeLocation:
    """Return the location of the last token / character in a tree."""
    if isinstance(tree, TerminalNode):
        token = tree.getPayload()
        if isinstance(token, Token):
            return CodeLocation(token.stop, token.line,
                                token.column + len(token.text))
    if not tree.children:
        return None
    return stop_location(tree.children[-1])


def _code_snippet(code: str, location: CodeLocation):
    if not code:
        return ''
    if location.position >= 0:
        with StringIO() as buf:
            index = location.position
            while index < len(code) and code[index] != '\n':
                buf.write(code[index])
                index += 1
            return buf.getvalue()
    else:
        lines = code.splitlines()
        if 0 < location.line <= len(lines):
            return lines[location.line - 1][location.column:]
    return ''


def code_extract(code: str, start: CodeLocation, stop: CodeLocation):
    """Extracts a piece from code between start and stop."""
    if start.position >= 0 and stop.position >= 0:
        return code[start.position:stop.position]
    lines = code.splitlines(keepends=True)
    if (start.line < 1 or start.line > len(lines) or stop.line < 1 or
            stop.line > len(lines)):
        return ''
    if start.line == stop.line:
        return lines[start.line - 1][start.column:stop.column]
    code_lines = [lines[start.line - 1][start.column:]]
    code_lines.extend(lines[start.line:stop.line - 1])
    code_lines.append(lines[stop.line - 1][:stop.column])
    return ''.join(code_lines)


def find_errors(tree: Tree, code: Optional[str] = None) -> List[str]:
    """Finds and returns the errors in the tree."""
    if isinstance(tree, ErrorNode):
        error = Trees.Trees.getNodeText(tree)
        location = start_location(tree)
        line = ''
        snippet = ''
        if location:
            line = f'line {location.line}:{location.column} '
            snippet = _code_snippet(code, location)
        return [f'{line}{error}: at `{snippet}`']
    if not tree.getChildCount():
        return []
    errors = []
    for child in tree.getChildren():
        errors.extend(find_errors(child, code))
    return errors


def recompose(tree: Tree) -> str:
    """Recreates the original SQL from a parse tree.

    It is not in the same structure, but as a long line, and does
    not include the comments or other decorations."""
    if not tree:
        return ''
    return tokens.recompose(tokens.from_tree(tree))[0]


def recompose_list(trees: Iterable[Tree], joiner: str = ' ') -> str:
    """Recomposes a list of of trees."""
    return joiner.join(recompose(t) for t in trees)
