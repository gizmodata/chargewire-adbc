#!/usr/bin/env bash
# Build the driver's C shared library (go/build/libadbc_driver_warpsql.<ext>).
#
# On Linux the C toolchain is `zig cc` targeting glibc 2.17, so the library runs on any glibc >= 2.17
# (RHEL/Oracle Linux 7 and 8, Amazon Linux, Debian 10+, ...); built with a CI runner's own gcc it
# would need that runner's much newer glibc.
set -euo pipefail
cd "$(dirname "$0")/../go"

if [ "$(go env GOOS)" = "linux" ]; then
  case "$(go env GOARCH)" in
    amd64) TARGET=x86_64-linux-gnu.2.17 ;;
    arm64) TARGET=aarch64-linux-gnu.2.17 ;;
    *) echo "unsupported GOARCH $(go env GOARCH)" >&2; exit 1 ;;
  esac
  command -v zig >/dev/null || { echo "zig is required to build the Linux library" >&2; exit 1; }
  export CC="zig cc -target $TARGET"
fi

make lib
ls -la build/
