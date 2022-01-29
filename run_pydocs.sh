#!/usr/bin/env bash
#
# Starts a local pydoc3 server to explore reference of our modules.
#

help() {
     cat << __EOF__

Starts a pydoc server to display information about dataschema and
sql_analyze modules.

${0} -p <port> -n

  -p <port> - starts the viewing server on provide port.
              Default: ${port}.
  -n - performs dry-run: prints the command only.
__EOF__
     exit;
}

port=7000
run=

while [[ $# -gt 0 ]]; do
    case ${1} in
        -p|--port)
            port="${2}"
            shift; shift
            ;;
        -n|--dry_run)
            run=echo
            shift
            ;;
        *)
            echo "Unknown option ${1}"
            help
            ;;
    esac
done

${run} bazel build //...
${run} cp -f bazel-bin/dataschema/Schema_pb2.py dataschema/Schema_pb2.py
${run} pydoc3 -p ${port}
