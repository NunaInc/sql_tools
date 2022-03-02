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
"""Utilities for manipulating tokens and formatting statements, from
trees obtained from antlr.
"""

from antlr4.Token import Token
from io import StringIO
from antlr4.tree.Tree import TerminalNode, Tree
from typing import List, NewType, Optional, Set, Tuple, Union

Tokens = NewType('Tokens', List[Union[str, 'Tokens']])


def _token_text(token):
    """Helper to get the text of a antlr token w/o the <EOF>"""
    istream = token.getInputStream()
    if istream is None:
        return token.text
    n = istream.size
    if token.start >= n or token.stop >= n:
        return []
    return token.text


def from_tree(tree: Tree) -> Tokens:
    """Returns the individual token texts for a antlr parse tree."""
    if tree is None:
        return []
    if (isinstance(tree, TerminalNode) and
            isinstance(tree.getPayload(), Token)):
        return _token_text(tree.getPayload())
    return [from_tree(child) for child in tree.getChildren()]


def recompose(tokens: Tokens) -> Tuple[str, str, str]:
    """Recomposes the tokens in a string, separated by spaces.

    Returns: (recomposed_string, first_token, last_token)
    """
    if isinstance(tokens, str):
        return (tokens, tokens, tokens)
    first_token = None
    last_token = None
    with StringIO() as buf:
        for t in tokens:
            if isinstance(t, str):
                crt, first, last = (t, t, t)
            else:
                crt, first, last = recompose(t)
            if not crt:
                continue
            if not first_token:
                first_token = first
            if _recompose_with_space(last_token, first):
                buf.write(' ')
            buf.write(crt)
            last_token = last
        return (buf.getvalue(), first_token, last_token)


def to_block(tokens: Tokens) -> 'CodeBlock':
    """Builds a code block out of a list of string tokens.
    You can use block.format(..) to reformat the returned code block.
    """
    block = CodeBlock(True)
    index = 0
    while index < len(tokens):
        index = block.add_tokens(tokens, index)
    return block


# Separators that do not require space before them
_SEPARATORS = {'(', ')', ',', '[', ']', ';', '.'}

# Keywords that would require a space after tham when followed by a bracket.
_SPACED_KW = {'IN', 'NOT', 'AS', 'AND', 'OR', 'WHEN', 'OVER', 'FROM', 'VALUES'}

# Map of brackets - open / close.
_BRACKETS = {'(': ')', '[': ']', '{': '}'}

# Expression breakers when code formatting.
_BREAKERS = {
    'CASE': {'WHEN', 'ELSE', 'END'},
}

# Operators which can break a line when code formatting.
_BREAK_OPS = {'AND', 'OR', 'WHEN', 'OVER', 'FROM'}


def _recompose_with_space(last: str, first: str):
    """If space should be added between recomposed spaces."""
    return (last and last not in {'(', '[', '.'} and
            (first not in _SEPARATORS or
             (first == '(' and
              (not _is_identifier(last) or last.upper() in _SPACED_KW))))


def _is_alnum(c):
    return ('a' <= c <= 'z') or ('A' <= c <= 'Z') or ('0' <= c <=
                                                      '9') or c == '_'


def _is_identifier(s):
    return all(_is_alnum(c) for c in s)


class _FormatLine:
    """A code line used for formatting."""

    def __init__(self, indent: int, line_len: int):
        self.indent = indent
        self.line_len = line_len
        self.line = ''
        self.first_token = None
        self.last_token = None
        self.breakable = False

    def add_token(self, token: str):
        self.add_composed(token, token, token)

    def add_composed(self, s: str, first_token: str, last_token: str):
        if not self.first_token:
            self.first_token = first_token
        if _recompose_with_space(self.last_token, first_token):
            self.line += ' '
        self.line += s
        self.last_token = last_token

    def add_line(self, line: '_FormatLine'):
        self.add_composed(line.line, line.first_token, line.last_token)

    def break_on_token(self, token: str):
        return not self.can_add(token) and (
            self.breakable or
            (self.last_token and
             (not (_is_identifier(self.last_token) or self.last_token == '.') or
              self.last_token.upper() in _BREAK_OPS)))

    def can_add(self, s: str):
        return len(self.line) + len(s) + self.indent <= self.line_len

    def can_add_line(self, line: Optional['_FormatLine']):
        if not line:
            return False
        return self.can_add(line.line)

    def ends_with_bracket(self):
        return self.line and self.line[-1] in {')', ']', '}'}

    def __str__(self):
        return (' ' * self.indent) + self.line


class _FormatLines:
    """A set of lines used for formatting."""

    def __init__(self, indent, line_len):
        self.indent = indent
        self.line_len = line_len
        self.lines = []

    def new_line(self) -> _FormatLine:
        return _FormatLine(self.indent, self.line_len)

    def next_line(self, line: _FormatLine):
        if not line.line:
            return line
        self.add_line(line)
        return self.new_line()

    def add_line(self, line: _FormatLine):
        if line.line:
            self.lines.append(line)

    def add_lines(self, lines: List[_FormatLine]):
        self.lines.extend(lines)

    def first_line(self):
        if not self.lines:
            return None
        return self.lines[0]

    def pop_first_line(self):
        return self.lines.pop(0)

    def add_lines_with_bracket_check(self, line: _FormatLine,
                                     lines: '_FormatLines'):
        if not lines.lines:
            return line
        self.add_line(line)
        if not lines.ends_with_bracket():
            self.add_lines(lines.lines)
            return self.new_line()
        self.add_lines(lines.lines[:-1])
        lines.lines[-1].breakable = True
        return lines.lines[-1]

    def ends_with_bracket(self):
        return self.lines and self.lines[-1].ends_with_bracket()

    def __str__(self):
        return '\n'.join(str(line) for line in self.lines)


# Default indent size, in spaces.
_INDENT = 4


class CodeBlock:
    """A block of code that can be used to format a SQL expression.

    Using it for SQL statements can have mixed results.
    """

    def __init__(self, is_container: bool, breakers: Optional[Set[str]] = None):
        self.is_container = is_container
        self.components = []
        self.first_token = None
        self.last_token = None
        if breakers is None:
            breakers = {}
        self._breakers = breakers
        self._recomposed = None

    def _add_sub_block(self, sub_block: 'CodeBlock'):
        if not self.first_token:
            self.first_token = sub_block.first_token
        self.last_token = sub_block.last_token
        self.components.append(sub_block)

    def _add_token(self, token: str):
        if not self.first_token:
            self.first_token = token
        self.last_token = token
        self.components.append(token)

    def _treat_paren(self, tokens, index):
        t = tokens[index]
        if not isinstance(t, str) or t not in _BRACKETS:
            return index, None
        self._add_token(t)
        return (index + 1, _BRACKETS[t])

    def _treat_sub_breakers(self, tokens: Tokens, index: int) -> int:
        t = tokens[index]
        if self._breakers or (not isinstance(t, str) or
                              t.upper() not in _BREAKERS):
            return index
        breakers = _BREAKERS[t.upper()]
        while index < len(tokens):
            sub_block = CodeBlock(False, breakers)
            index = sub_block.add_tokens(tokens, index)
            self._add_sub_block(sub_block)
        return index

    def _treat_sub_brakets(self, tokens: Tokens, index: int) -> int:
        t = tokens[index]
        if not isinstance(t, str) or t.upper() not in _BRACKETS:
            return index
        sub_block = CodeBlock(True)
        index = sub_block.add_tokens(tokens, index)
        self._add_sub_block(sub_block)
        return index

    def _add_blocks(self, tokens: Tokens, index: int) -> int:
        index, closing = self._treat_paren(tokens, index)
        while index < len(tokens):
            t = tokens[index]
            if closing and t == closing:
                self._add_token(t)
                return index + 1
            sub_block = CodeBlock(False)
            index = sub_block.add_tokens(tokens, index)
            self._add_sub_block(sub_block)
        return index

    def add_tokens(self, tokens: Tokens, index: int) -> int:
        """Adds tokens to this block, starting from index.
        Returns the index of the first unconsumed token."""
        if index >= len(tokens):
            return index
        if self.is_container:
            return self._add_blocks(tokens, index)
        if index == 0:
            bindex = self._treat_sub_breakers(tokens, index)
            if bindex:
                return bindex
        start_index = index
        while index < len(tokens):
            t = tokens[index]
            if isinstance(t, str):
                if (t in self._breakers and start_index < index and
                        len(tokens) > index + 1):
                    return index
                if t == ',':
                    self._add_token(t)
                    return index + 1
                if t in _BRACKETS:
                    index = self._treat_sub_brakets(tokens, index)
                else:
                    self._add_token(t)
                    index += 1
            else:
                sub_index = self.add_tokens(t, 0)
                while sub_index < len(t):
                    sub_block = CodeBlock(False)
                    sub_index = sub_block.add_tokens(t, sub_index)
                    self._add_sub_block(sub_block)
                index += 1
        return index

    def recompose(self):
        """Builds the one-liner SQL from this code block."""
        if self._recomposed is None:
            self._recomposed = self._recompose()
        return self._recomposed

    def _recompose(self):
        last = None
        with StringIO() as buf:
            for t in self.components:
                if isinstance(t, str):
                    if _recompose_with_space(last, t):
                        buf.write(' ')
                    buf.write(t)
                    last = t
                else:
                    if _recompose_with_space(last, t.first_token):
                        buf.write(' ')
                    buf.write(t.recompose())
                    last = t.last_token
            return buf.getvalue()

    def __str__(self):
        """A string representation of this block."""
        is_first = True
        with StringIO() as buf:
            if self.is_container:
                buf.write('{')
            else:
                buf.write('<')
            for t in self.components:
                if not is_first:
                    buf.write(' ')
                else:
                    is_first = False
                if isinstance(t, str):
                    buf.write(f'`{t}`')
                else:
                    buf.write(str(t))
            if self.is_container:
                buf.write('}')
            else:
                buf.write('>')
            return buf.getvalue()

    def format_lines(self, line_len: int, indent: int = 0) -> _FormatLines:
        lines = _FormatLines(indent, line_len)
        line = lines.new_line()
        start = 0
        sub_indent = 0 if self.is_container else _INDENT
        while self.components[start] in _BRACKETS:
            line.add_token(self.components[start])
            start += 1
        line = lines.next_line(line)
        for index in range(start, len(self.components)):
            t = self.components[index]
            if isinstance(t, str):
                if line.break_on_token(t):
                    line = lines.next_line(line)
                line.add_token(t)
                continue
            s = t.recompose()
            if line.can_add(s):
                line.add_composed(s, t.first_token, t.last_token)
                continue
            if s[0] not in _BRACKETS and lines.new_line().can_add(s):
                line = lines.next_line(line)
                line.add_composed(s, t.first_token, t.last_token)
                continue
            sub_lines = t.format_lines(line_len, indent + sub_indent)
            is_first = True
            while line.can_add_line(sub_lines.first_line()):
                front_line = sub_lines.pop_first_line()
                if is_first:
                    is_first = False
                else:
                    front_line.indent = sub_indent
                line.add_line(front_line)
            line = lines.add_lines_with_bracket_check(line, sub_lines)
        lines.add_line(line)
        return lines

    def format(self, line_len):
        return str(self.format_lines(line_len))
