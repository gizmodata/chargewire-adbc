#!/usr/bin/env bash
# Build the driver's C shared library (go/build/libadbc_driver_warpsql.<ext>) the way CI does.
# (`make -C go lib` is the same build with the local C toolchain.)
#
# On Linux the C toolchain is `zig cc` targeting glibc 2.17, so the library runs on any glibc >= 2.17
# (RHEL/Oracle Linux 7 and 8, Amazon Linux, Debian 10+, ...); built with a CI runner's own gcc it
# would need that runner's much newer glibc.
set -euo pipefail
cd "$(dirname "$0")/../go"

case "$(go env GOOS)" in
  windows) EXT=dll ;;
  darwin)  EXT=dylib ;;
  *)       EXT=so ;;
esac

if [ "$(go env GOOS)" = "linux" ]; then
  case "$(go env GOARCH)" in
    amd64) TARGET=x86_64-linux-gnu.2.17 ;;
    arm64) TARGET=aarch64-linux-gnu.2.17 ;;
    *) echo "unsupported GOARCH $(go env GOARCH)" >&2; exit 1 ;;
  esac
  command -v zig >/dev/null || { echo "zig is required to build the Linux library" >&2; exit 1; }
  export CC="zig cc -target $TARGET"
fi

VERSION="${VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}"
CGO_ENABLED=1 go build -tags driverlib -buildmode=c-shared \
  -ldflags "-s -w -X github.com/adbc-drivers/driverbase-go/driverbase.infoDriverVersion=$VERSION" \
  -o "build/libadbc_driver_warpsql.$EXT" ./pkg/warpsql
ls -la build/
