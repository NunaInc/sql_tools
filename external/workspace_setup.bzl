load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@rules_cc//cc:repositories.bzl", "rules_cc_dependencies")
load(
    "@rules_proto//proto:repositories.bzl",
    "rules_proto_dependencies",
    "rules_proto_toolchains",
)
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
load("@rules_python//python:pip.bzl", "pip_install")
load("@bazel_pylint//:load.bzl", "bazel_pylint_load_workspace")
load("@bazel_pylint//:setup.bzl", "bazel_pylint_setup_workspace")

def nuna_sql_tools_setup_workspace():
    rules_cc_dependencies()
    rules_proto_dependencies()
    rules_proto_toolchains()
    protobuf_deps()
    bazel_pylint_load_workspace()
    bazel_pylint_setup_workspace()
    pip_install(
        name = "nuna_sql_tools_pip_deps",
        requirements = "@nuna_sql_tools//:requirements-dev.txt",
        timeout = 1200,
    )
