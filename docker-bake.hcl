group "default" {
    targets = ["test", "test_package", "lint"]
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

py_versions = ["3.12", "3.13-rc"]
// TODO: support these versions
// py_versions = ["3.9", "3.10", "3.11", "3.12", "3.13-rc"]

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

target "lint" {
    name = "lint-${lint_type}"
    matrix = {
        lint_type = ["check", "format", "mypy"],
    }
    args = {
        PYTHON_VER = get_py_image_tag("latest")
    }
    target = "lint-${lint_type}"
    no-cache-filter = ["lint-setup"]
    output = ["type=cacheonly"]
}
