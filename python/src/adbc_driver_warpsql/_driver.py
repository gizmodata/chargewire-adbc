"""Locate the bundled Go Warp SQL ADBC driver shared library."""

from __future__ import annotations

import functools
import os
import pathlib
import sys

#: Environment variable naming a driver library to use instead of the bundled one.
DRIVER_LIB_ENV = "WARP_SQL_ADBC_DRIVER_LIB"


@functools.cache
def _driver_path() -> str:
    """Return the path to libadbc_driver_warpsql for this platform.

    Search order:
      1. the ``WARP_SQL_ADBC_DRIVER_LIB`` environment variable
      2. the shared library bundled inside this package (wheel installs)
      3. the repository's build output ``go/build/`` (development checkouts)
    """
    if env := os.environ.get(DRIVER_LIB_ENV):
        return env

    if sys.platform == "darwin":
        ext = "dylib"
    elif os.name == "nt":
        ext = "dll"
    else:
        ext = "so"

    pkg_dir = pathlib.Path(__file__).resolve().parent
    for directory in (pkg_dir, pkg_dir.parent.parent.parent / "go" / "build"):
        lib = directory / f"libadbc_driver_warpsql.{ext}"
        if lib.exists():
            return str(lib)

    raise RuntimeError(
        "libadbc_driver_warpsql shared library not found. Install a platform wheel of "
        f"adbc-driver-warpsql, set {DRIVER_LIB_ENV}, or build it with `make -C go lib`."
    )
