#!/usr/bin/env python3
"""Wait until every file in a local dist directory is visible on PyPI."""

import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("package")
    parser.add_argument("version")
    parser.add_argument("dist_dir", type=Path)
    args = parser.parse_args()

    expected = {path.name for path in args.dist_dir.iterdir() if path.is_file()}
    if not expected:
        print(f"No files found in {args.dist_dir}.")
        return 1

    url = f"https://pypi.org/pypi/{args.package}/{args.version}/json"
    missing = expected
    for attempt in range(30):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                actual = {file["filename"] for file in json.load(response)["urls"]}
        except (OSError, ValueError, KeyError, TypeError) as error:
            print(f"Could not read {args.package} {args.version} from PyPI: {error}.")
            actual = set()
        missing = expected - actual
        if not missing:
            print(f"All {len(expected)} files of {args.package} {args.version} are on PyPI.")
            return 0
        print(f"Still missing on PyPI: {sorted(missing)}")
        if attempt < 29:
            time.sleep(10)

    print(f"{args.package} {args.version} did not become fully available on PyPI.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
