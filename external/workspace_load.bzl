load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def nuna_sql_tools_load_workspace():
    # Bazel utility library
    http_archive(
        name = "bazel_skylib",
        sha256 = "97e70364e9249702246c0e9444bccdc4b847bed1eb03c5a3ece4f83dfe6abc44",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.0.2/bazel-skylib-1.0.2.tar.gz",
            "https://github.com/bazelbuild/bazel-skylib/releases/download/1.0.2/bazel-skylib-1.0.2.tar.gz",
        ],
    )

    ## CC rules
    http_archive(
        name = "rules_cc",
        sha256 = "9d48151ea71b3e225adfb6867e6d2c7d0dce46cbdc8710d9a9a628574dfd40a0",
        strip_prefix = "rules_cc-818289e5613731ae410efb54218a4077fb9dbb03",
        urls = ["https://github.com/bazelbuild/rules_cc/archive/818289e5613731ae410efb54218a4077fb9dbb03.tar.gz"],
    )

    ## Proto rules
    http_archive(
        name = "rules_proto",
        sha256 = "a4382f78723af788f0bc19fd4c8411f44ffe0a72723670a34692ffad56ada3ac",
        strip_prefix = "rules_proto-f7a30f6f80006b591fa7c437fe5a951eb10bcbcf",
        urls = ["https://github.com/bazelbuild/rules_proto/archive/f7a30f6f80006b591fa7c437fe5a951eb10bcbcf.zip"],
    )

    ## Google protobuf
    http_archive(
        name = "com_google_protobuf",
        sha256 = "7b8d3ac3d6591ce9d25f90faba80da78d0ef620fda711702367f61a40ba98429",
        strip_prefix = "protobuf-3.19.0",
        urls = ["https://github.com/protocolbuffers/protobuf/releases/download/v3.19.0/protobuf-all-3.19.0.tar.gz"],
    )

    ## Python rules
    http_archive(
        name = "rules_python",
        sha256 = "934c9ceb552e84577b0faf1e5a2f0450314985b4d8712b2b70717dc679fdc01b",
        url = "https://github.com/bazelbuild/rules_python/releases/download/0.3.0/rules_python-0.3.0.tar.gz",
    )

    ## Pylinted targets
    http_archive(
        name = "bazel_pylint",
        sha256 = "ce8f886c113734a2a4d8c3c700d1aa666bccf9898a07ab8595304dbc26a26500",
        strip_prefix = "bazel_pylint-0.0.6",
        urls = ["https://github.com/NunaInc/bazel_pylint/archive/refs/tags/0.0.6.tar.gz"],
    )
