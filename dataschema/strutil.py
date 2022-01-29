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
"""Some string utilities used for generating proto schema files."""


def StripSuffix(s: str, suffix: str):
    """Strips the provided suffix from s, if present at its end."""
    if s.endswith(suffix):
        return s[:-len(suffix)]
    return s


def StripPrefix(s: str, prefix: str):
    """Skips the provided prefix from s, if present at its beginning."""
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


def EscapeWithUnderscores(s: str):
    """Replaces the non alphanumeric characters in a strings w/ _ escape codes."""
    r = ''
    for c in s:
        if c == '_':
            r += '__'
        elif ('0' <= c <= '9') or ('a' <= c <= 'z') or ('A' <= c <= 'Z'):
            r += c
        else:
            r += f'_{ord(c):02X}_'
    return r


def UnescapeUnderscores(s: str):
    """Reverses EscapeWithUnderscores."""
    i = 0
    r = ''
    while i < len(s):
        if s[i] == '_':
            j = s.find('_', i + 1)
            if j == -1:
                raise ValueError('Not a valid string escaped with `_`')
            ss = s[i + 1:j]
            if not ss:
                r += '_'
            else:
                r += chr(int(ss, 16))
            i = j + 1
        else:
            r += s[i]
            i += 1
    return r


def EscapeString(s: str):
    return s.translate(
        str.maketrans({
            '\\': '\\\\',
            '\n': '\\n',
            '\t': '\\t',
            '"': '\\"',
            '\b': '\\b',
            '\r': '\\r'
        }))
