# Nuna: sql_tools

This repository contains Python libraries for helping developers
produce and maintain data analysis projects. In particular:

* `dataschema`: a library for defining data schemas using Python
dataclasses, that that can be easily converted between protobuffers,
Scala case classes, sql (ClickHouse) create table statements,
Parquet Arrow schemas and so on, from a central Python based
representation. Includes facilities to generate sample date and
compare schemas for validations.

* `sql_analyze`: a library for analyzing SQL statements. In particular
the raw SQL statments are parsed and converted to a Python based
data structure. From here, they can be converted to a data graph,
visualized, and information about the lineage of tables and columns
can be infered. Support for now SparkSql and ClickHouse dialects
for parsing.


## Requirements:

The project needs [Bazel](https://bazel.build/) for building, and
requires Python 3.7 or higher.

## Quick Demo:

To quickly check out the SQL visualizer included in this project,
run `./run_viewer.sh` from the top directory (Bazel required),
navigate to [http://localhost:8000/] and examine some SQL statements.
Here is an exmple analysis:

![Screenshot](sql_analyze/viewer/web/viewer.png)
