

## Generating `.py` files from grammar files:

`ClickHouseLexer.g4` and `ClickHouseParser.g4` are from
ClickHouse project here on
[GitHub](https://github.com/ClickHouse/ClickHouse/tree/master/utils/antlr).
The Apache `LICENSE` is included in this directory, with some
small modifications:

  * Modify `ClickHouseLexer.g4` and add this to `IDENTIFIER`:
```
    | '$' '{' (LETTER | UNDERSCORE | DEC_DIGIT)* '}'
```
  * Modify `ClickHouseParser.g4` and remove all C++ code
  inserts - look for `locals` and comment out.
  * Various small rule tuneups in the parser.

From these grammar definitions, we generated `.py` files w/ antlr.
We don't have this in the bazel build as we don't want to
depend on the a random bazel antler wrapper. Here are the commands
to generate `.py` files:

```bash
antlr -no-listener -visitor -Dlanguage=Python3 ClickHouseLexer.g4
antlr -no-listener -visitor -Dlanguage=Python3 ClickHouseParser.g4
```

Remove temporary files: `rm *.tokens *.interp *.bkp`
