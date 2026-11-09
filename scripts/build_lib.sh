#!/usr/bin/env bash
# Build the driver's C shared library (go/build/libadbc_driver_chargewire.<ext>) the way CI does.
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

# On macOS, the oldest macOS the library runs on (the wheel's tag says the same). It goes into the
# cgo flags, not only the environment: Go's build cache keys on those flags and not on
# MACOSX_DEPLOYMENT_TARGET, so a cached build for another target would otherwise be reused.
if [ "$(go env GOOS)" = "darwin" ] && [ -n "${MACOSX_DEPLOYMENT_TARGET:-}" ]; then
  export CGO_CFLAGS="${CGO_CFLAGS:--O2 -g} -mmacosx-version-min=$MACOSX_DEPLOYMENT_TARGET"
  export CGO_LDFLAGS="${CGO_LDFLAGS:--O2 -g} -mmacosx-version-min=$MACOSX_DEPLOYMENT_TARGET"
fi

# The library exports only the ADBC entrypoints (Adbc*), per the ADBC shared-library requirements
# (https://adbc-drivers.org/building-drivers/shared-library-requirements.html); without export lists
# it also exported every Go //export function (ChargewireDatabaseNew, ...) and the cgo glue. The
# lists live next to the package: exports_linux.map (version script), exports_darwin.txt (ld64
# exported-symbols list) and exports_windows.def. On Windows, Go's linker adds its own .def file
# (every //export function: ChargewireDatabaseNew, arrow-go's streamGetNext, ...) unless its
# -extldflags hold the exact word -Wl,--export-all-symbols. That flag must not reach the linker
# itself, though: LLVM lld (llvm-mingw, the Windows arm64 build) then exports every symbol whatever
# else is given. So `-L` comes first and makes the C compiler read it as a library directory: Go
# sees the word, the linker never does, and the DLL exports what the C code marks dllexport (the
# Adbc* wrappers in utils.c) plus our .def (AdbcDriverChargewireInit, a Go //export), with GNU ld
# and lld alike (--exclude-all-symbols keeps automatic exporting off either way). The flags can't go
# in `#cgo LDFLAGS` (Go refuses them), so they go through -extldflags. The external linker runs in a
# temporary directory: the paths are absolute, in the native form on Windows (D:/a/..., not Git
# Bash's /d/a/...). `go run ./internal/checkexports build/libadbc_driver_chargewire.<ext>` (in go/)
# checks the result.
EXPORTS_DIR=$PWD/pkg/chargewire
if command -v cygpath >/dev/null 2>&1; then
  EXPORTS_DIR=$(cygpath -m "$EXPORTS_DIR")
fi
case "$(go env GOOS)" in
  linux)   EXTLDFLAGS="-Wl,--version-script=$EXPORTS_DIR/exports_linux.map" ;;
  darwin)  EXTLDFLAGS="-Wl,-exported_symbols_list,$EXPORTS_DIR/exports_darwin.txt" ;;
  windows) EXTLDFLAGS="-L -Wl,--export-all-symbols -Wl,--exclude-all-symbols $EXPORTS_DIR/exports_windows.def" ;;
  *) echo "unsupported GOOS $(go env GOOS)" >&2; exit 1 ;;
esac

# The release tag the commit descends from (vX.Y.Z[-N-gHASH][-dirty]), never the Go module's
# go/vX.Y.Z tag on the same commit; a checkout without tags (CI's shallow clone) is v0.0.0-HASH, so
# GetInfo's driver version is always a version.
VERSION="${VERSION:-$(git describe --tags --match 'v[0-9]*' --dirty 2>/dev/null || echo "v0.0.0-$(git rev-parse --short HEAD 2>/dev/null || echo dev)")}"
CGO_ENABLED=1 go build -tags driverlib -buildmode=c-shared \
  -ldflags "-s -w -X github.com/adbc-drivers/driverbase-go/driverbase.infoDriverVersion=$VERSION -extldflags '$EXTLDFLAGS'" \
  -o "build/libadbc_driver_chargewire.$EXT" ./pkg/chargewire
ls -la build/
