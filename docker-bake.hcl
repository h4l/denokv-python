PROTOC_VERSION = "22.0"

group "default" {
    targets = ["test", "test_package", "lint-all"]
}

group "lint-all" {
    targets = ["lint", "lint-protobuf"]
}

function "get_py_image_tag" {
  params = [py_version]
  // Don't use slim build for rc because wheels may need compiling, and slim
  // does not have gcc, etc.
  result = (
    py_version == "latest"
    ? "slim"
    : can(regex("^.*-rc$", py_version))
    ? py_version
    : "${py_version}-slim"
  )
}

py_versions = ["3.9", "3.10", "3.11", "3.12", "3.13-rc"]

target "test" {
    name = "test_py${replace(py, ".", "")}"
    matrix = {
        py = py_versions,
    }
    args = {
        PYTHON_VER = get_py_image_tag(py)
    }
    target = "test"
    no-cache-filter = ["test"]
    output = ["type=cacheonly"]
}

target "test_package" {
    name = "test_package_py${replace(py, ".", "")}"
    matrix = {
        py = py_versions,
    }
    args = {
        PYTHON_VER = get_py_image_tag(py)
    }
    target = "test-package"
    no-cache-filter = ["test-package"]
    output = ["type=cacheonly"]
}

target "_lint" {
    args = {
        PYTHON_VER = get_py_image_tag("latest")
    }
    no-cache-filter = ["lint-setup"]
    output = ["type=cacheonly"]
}

target "lint" {
    inherits = ["_lint"]
    name = "lint-${lint_type}"
    matrix = {
        lint_type = ["check", "format", "mypy"],
    }
    target = "lint-${lint_type}"
}

target "lint-protobuf" {
    inherits = ["_lint"]
    args = {
        PROTOC_VERSION = PROTOC_VERSION
    }
    contexts = {
        generated-protobuf = "target:protobuf"
    }
    target = "lint-protobuf"
}

target "protobuf" {
    args = {
        PYTHON_VER = get_py_image_tag("latest")
        PROTOC_VERSION = PROTOC_VERSION
    }
    contexts = {
        // https://github.com/denoland/denokv/commits/main/proto/schema/datapath.proto
        denokv-repo = "https://github.com/denoland/denokv.git#e6a50cfe7ea5e66b9bf68da3a0731c72122ff17f" # Dec 21, 2023
    }
    target = "datapath-protobuf-python"
    no-cache-filter = ["build-datapath-protobuf-python"]
    output = ["type=local,dest=build/protobuf_${PROTOC_VERSION}"]
}
