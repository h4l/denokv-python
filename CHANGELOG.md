# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added

- Support closing `Kv` client's `session` via:
  ([#20](https://github.com/h4l/denokv-python/pull/20))
  - `Kv.aclose()`
  - async context manager
  - At interpreter exit / garbage collection via `Kv.create_finalizer()`
  - Automatically when an interactive console exists:
    - `Kv` objects created by `open_kv()` from an interactive console/REPL
      automatically close at exit.
    - The `open_kv()` function has a `finalize` option that controls this.

[unreleased]: https://github.com/h4l/denokv-python/commits/main/
