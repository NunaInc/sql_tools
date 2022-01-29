#!/usr/bin/env bash
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
set -e

function is_absolute {
  [[ "$1" = /* ]] || [[ "$1" =~ ^[a-zA-Z]:[/\\].* ]]
}

function real_path() {
  is_absolute "$1" && echo "$1" || echo "$PWD/${1#./}"
}

function prepare_src() {
    TMPDIR="${1%/}"
    mkdir -p "$TMPDIR/src/"
    echo $(date) : "=== Preparing sources in dir: ${TMPDIR}"
    RUNFILES=bazel-bin/package/build_pip.runfiles/nuna_schema
    for pkg in dataschema sql_analyze; do
        cp -LR ${RUNFILES}/${pkg} "${TMPDIR}/src/"
        touch ${TMPDIR}/src/${pkg}/__init__.py
    done
    cp package/setup.py "${TMPDIR}"
    cp requirements-dev.txt "${TMPDIR}"
}

function build_wheel() {
    TMPDIR="$1"
    DEST="$2"
    VERSION="$3"
    pushd ${TMPDIR} > /dev/null
    echo "${VERSION}" > "${TMPDIR}/VERSION"
    echo $(date) : "=== Building wheel ${VERSION}"
    python3 setup.py bdist_wheel ${PKG_NAME_FLAG}
    mkdir -p ${DEST}
    cp dist/* ${DEST}
    popd > /dev/null
    echo $(date) : "=== Output wheel file is in: ${DEST}"
}


function main() {
    SRCDIR="$(mktemp -d -t tmp.NUNA_SCHEMA)"
    VERSION="${1}"
    DSTDIR="$(real_path $2)"
    if [ "${VERSION}" == "" ]; then
        VERSION=$(git tag -l  | tail -n 1)
    fi
    if [ "${VERSION}" == "" ]; then
        VERSION=$(git log --oneline -1 | cut -d ' ' -f 1)
    fi
    prepare_src "${SRCDIR}"
    build_wheel "${SRCDIR}" "${DSTDIR}" "${VERSION}"
}

main "$@"
