"""Set the pyproject.toml version to `-devN` where N is a unique number."""

import re
import sys
from subprocess import PIPE
from subprocess import run

DEV_PATTERN = r"(?<=-)dev(?=$|[+-])"


def main() -> None:
    poetry_version = run(
        ["poetry", "version", "--short"], shell=False, stdout=PIPE, encoding="utf-8"
    )
    poetry_version.check_returncode()
    raw_version = poetry_version.stdout.strip()

    dev_match = re.search(DEV_PATTERN, raw_version)
    if not dev_match:
        print(
            f"pyproject.toml version is not plain dev version: {raw_version!r}",
            file=sys.stderr,
        )
        sys.exit(1)
    if "+" in raw_version:
        print(
            f"pyproject.toml version contains a '+' build identifier: {raw_version!r}"
        )
        sys.exit(1)

    dev_number_proc = run(
        ["git", "rev-list", "--count", "HEAD"],
        shell=False,
        stdout=PIPE,
        encoding="utf-8",
    )
    dev_number_proc.check_returncode()
    dev_number = int(dev_number_proc.stdout.strip())

    short_sha_proc = run(
        ["git", "rev-parse", "--short", "HEAD"],
        shell=False,
        stdout=PIPE,
        encoding="utf-8",
    )
    short_sha_proc.check_returncode()
    short_sha = short_sha_proc.stdout.strip()

    ver_prefix = raw_version[: dev_match.start()]
    ver_suffix = raw_version[dev_match.end() :]

    this_version = f"{ver_prefix}dev{dev_number:d}{ver_suffix}+{short_sha}"

    run(["poetry", "version", this_version], shell=False).check_returncode()


if __name__ == "__main__":
    main()
