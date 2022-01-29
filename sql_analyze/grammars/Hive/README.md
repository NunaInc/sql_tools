
The `.g4` files come from Antler [`grammars-v4`](https://github.com/antlr/grammars-v4)
project, under `sql/hive/grammar/`. Licensed under Apache 2.0 License.

Adaptations done by Nuna Inc. for supporting SparkSql grammar.
Licensed under Apache 2.0 License as well.

## Generating `.py` files from grammar files:

From these grammar definitions, we generated `.py` files w/ antlr.
We don't have this in the bazel build as we don't want to
depend on the a random bazel antler wrapper. Here are the commands
to generate `.py` files.

1. Install `antlr` if needed (e.g. `brew install antlr@4` on macOS).
2. Make small changes:

  * the lexing case insensitive (from the original which is case
  sensitive): `./mkcase.py HiveLexer.g4`, that does not solve all cases,
  look for `KW_` tokens with rules including `|` or between `'`, and modify manually.
  * add an `?` after `identifier` under tag `subQuerySource` in `FromClauseParser.g4`
```
subQuerySource
    : LPAREN queryStatementExpression RPAREN KW_AS? identifier?
```
  * add our replace patten string as valid identifier in `HiveLexer.g4` under
rule `Identifier` :
```
Identifier
    : (Letter | Digit | '_')+
    /** Add this line: to Identifier rule: */
    | (Letter | Digit | '_')* DOLLAR LCURLY (Letter | Digit | '_')* RCURLY (Letter | Digit | '_')*
...
```
  * add multi line comment skipper:
```
MULTI_LINE_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;
```
  * make the `AS` optional in a with clause, not present in some of our
statements: add a `?` after `KW_AS` in this line:
```
cteStatement
    : identifier KW_AS? LPAREN queryStatementExpression RPAREN
    ;
```
  * Add `left` and `right` as possible function identifier in
  `IdentifiersParser.g4` by appending to rule
  `sql11ReservedKeywordsUsedAsFunctionName`: `| KW_LEFT | KW_RIGHT`.

    2. add it to `functionIdentifier` rule:
```
functionIdentifier
    : identifier DOT identifier
    | identifier
    | nonReservedFunctions   // <<< add this line
    ;
```
  * Fix the `window_frame_boundary` rule in `SelectClauseParser.g4`, by adding
  this line to the rule: `| intervalExpression (KW_PRECEDING | KW_FOLLOWING )`
  * Fix the `castExpression` in `IdentifiersParsers.g4`, by replacing
  `primitiveType` with `(primitiveType | listType | structType | mapType | unionType)`

  * Dealing with badly named `end` columns and missing `end` in `case` in
  `IdentifiersParsers.g4`

    1. under `expression` rule, change `atmoExpression` line to:
```
    : atomExpression ((LSQUARE expression RSQUARE) | (DOT identifier) | (DOT nonReservedColumnName))*
```
    2. add at the end of file:
```
nonReservedColumnName
    : KW_END
    ;
```
    3: Change `KW_END` to `KW_END?` in rules `caseExpression` and
    `whenExpression`.
  * Fix `function_` rule in `IdentifiersParser.g4` by adding this line:
    `| expression (KW_AS identifier)? (COMMA expression (KW_AS identifier)?)*`
  * Allow identifiers for `IN` expressions by adding `| identifier` to
  `precedenceSimilarExpressionIn` rule.
  * At this point I am getting too many of these, here is a summary:
      * adding supprot for `PIVOT` view & reworked the lateral view
      * functionals
      * multicolumn output for cast
      * long as a type


3. Generate the Python files from `.g4` grammar. Don't want to depend on
external tool antlr, so generating them outside of build process:
```bash
antlr -no-listener -visitor -Dlanguage=Python3 HiveLexer.g4
antlr -no-listener -visitor -Dlanguage=Python3 HiveParser.g4
```
4. Remove temporary files: `rm *.tokens *.interp`
