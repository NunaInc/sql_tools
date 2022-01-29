#!/usr/bin/env python3
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
"""Script replacing the case sensitive Hive lexer to a case insensitive one."""

import re
import sys

fragments = """
fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];
"""


def main():
    with open(sys.argv[1], 'r', encoding='utf-8') as f:
        lines = f.readlines()
    kw_reg = re.compile(r'(^KW_[A-Z]+\s*:\s*)(\'[A-Z]+\')(.*$)')
    out = []
    for l in lines:
        m = kw_reg.match(l)
        if not m:
            out.append(l)
            continue
        name = m.group(2)[1:-1]
        new_line = m.group(1) + (' '.join([c for c in name])) + m.group(3)
        if not new_line.endswith('\n'):
            new_line += '\n'
        out.append(new_line)

    out.append(fragments)
    with open(sys.argv[1], 'w', encoding='utf-8') as f:
        f.write(''.join(out))


if __name__ == '__main__':
    main()
