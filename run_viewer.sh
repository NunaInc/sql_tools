#!/usr/bin/env bash
#
# This utility starts the sql viewer on the example sql queries.
# Run from the top repo directory, the navigate to http://localhost:8000/
#

top_dir=$(pwd)
bazel run //sql_analyze/viewer:viewer \
      -- --root_dir ${top_dir}/sql_analyze/viewer/web \
      --port=8000 \
      --stocksql_dirs ${top_dir}/sql_analyze/viewer/examples
