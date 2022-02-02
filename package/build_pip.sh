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
    PKGDIR="${1%/}"
    mkdir -p "$PKGDIR/src/"
    echo $(date) : "=== Preparing sources in dir: ${PKGDIR}"
    RUNFILES=bazel-bin/package/build_pip.runfiles/nuna_sql_tools
    for pkg in dataschema sql_analyze; do
        cp -LR ${RUNFILES}/${pkg} "${PKGDIR}/src/"
        touch ${PKGDIR}/src/${pkg}/__init__.py
    done
    cp package/setup.py "${PKGDIR}"
    cp package/MANIFEST.in "${PKGDIR}"
    cp requirements.txt "${PKGDIR}"
    cp README.md "${PKGDIR}"
    cp LICENSE "${PKGDIR}"
}

function build_wheel() {
    PKGDIR="$1"
    DSTDIR="$2"
    VERSION="$3"
    pushd ${PKGDIR} > /dev/null
    rm -f nuna_sql_tools-*.whl
    echo "${VERSION}" > VERSION
    echo $(date) : "=== Building wheel ${VERSION}"
    python3 -m pip wheel -w ${PKGDIR} -e ${PKGDIR} --no-deps -vvv
    mkdir -p ${DSTDIR}
    whl=$(ls nuna_sql_tools-*.whl)
    cp ${whl} ${DSTDIR}
    popd > /dev/null
    echo $(date) : "=== Output wheel file is in: ${DSTDIR}${whl}"
}


function main() {
    PKGDIR="$(mktemp -d -t tmp.NUNA_SCHEMA)"    VERSION="${1}"
    PKGDIR="${2}"
    if [ "${PKGDIR}" == "" ]; then
        PKGDIR="$(mktemp -d -t tmp.NUNA_SCHEMA)"
    fi
    DSTDIR="$(real_path $3)"
    if [ "${VERSION}" == "" ]; then
        VERSION=$(git tag -l  | tail -n 1)
    fi
    if [ "${VERSION}" == "" ]; then
        commit_hash=$(git log --oneline -1 | cut -d ' ' -f 1)
        version=$(printf "%d" "0x${commit_hash}")
        VERSION="0.0.dev${version}"
    fi
    prepare_src "${PKGDIR}"
    build_wheel "${PKGDIR}" "${DSTDIR}" "${VERSION}"
}

main "$@"
