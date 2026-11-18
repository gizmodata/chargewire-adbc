#!/usr/bin/env bash
# Check a release's files and write their SHA256SUMS (release.yml, before the GitHub Release).
#
#   scripts/release_files.sh <dist dir> <ref>    # e.g. dist v2.4.1
#
# <dist dir> holds what release.yml's build jobs uploaded. It must hold exactly six library
# archives, libadbc_driver_chargewire-<ref>-<platform>.tar.gz, each unpacking to one folder of the
# same name with the platform's library in it (<folder>/libadbc_driver_chargewire.<so|dylib|dll>,
# built for that platform: the layout the ADBC driver installer, github.com/gizmodata/adbc-install,
# unpacks), and five wheels. Then it writes <dist dir>/SHA256SUMS over those eleven files and
# verifies it.
set -euo pipefail

[ $# -eq 2 ] || { echo "usage: $0 <dist dir> <ref>" >&2; exit 2; }
lib=libadbc_driver_chargewire
ref=${2//\//-}  # a branch name may hold slashes (package_lib.sh does the same)
cd "$1"

fail() { echo "::error::$*" >&2; exit 1; }

n=$(find . -maxdepth 1 -name "$lib-*.tar.gz" | wc -l)
[ "$n" -eq 6 ] || fail "$n library archives, expected 6"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
for platform in linux_amd64 linux_arm64 macos_amd64 macos_arm64 windows_amd64 windows_arm64; do
  case "$platform" in
    linux_amd64) ext=so; kind='ELF 64-bit.*x86-64' ;;
    linux_arm64) ext=so; kind='ELF 64-bit.*(aarch64|arm64)' ;;
    macos_amd64) ext=dylib; kind='Mach-O 64-bit.*x86_64' ;;
    macos_arm64) ext=dylib; kind='Mach-O 64-bit.*arm64' ;;
    windows_amd64) ext=dll; kind='PE32\+.*DLL.*x86-64' ;;
    windows_arm64) ext=dll; kind='PE32\+.*DLL.*(aarch64|arm64)' ;;
  esac
  top="$lib-$ref-$platform"
  [ -f "$top.tar.gz" ] || fail "no $top.tar.gz"
  rm -rf "${tmp:?}"/*
  tar -xzf "$top.tar.gz" -C "$tmp"
  [ "$(ls -A "$tmp")" = "$top" ] || fail "$top.tar.gz does not unpack to the one folder $top/: $(ls -A "$tmp" | tr '\n' ' ')"
  [ -f "$tmp/$top/$lib.$ext" ] && [ -s "$tmp/$top/$lib.$ext" ] || fail "$top.tar.gz has no $top/$lib.$ext"
  what=$(file -b "$tmp/$top/$lib.$ext")
  grep -Eiq -- "$kind" <<<"$what" || fail "$top/$lib.$ext is not a $platform library: $what"
  echo "$top.tar.gz: $top/$lib.$ext ($what)"
done

n=$(find . -maxdepth 1 -name '*.whl' | wc -l)
[ "$n" -eq 5 ] || fail "$n wheels, expected 5"

sha256sum -- "$lib"-*.tar.gz *.whl > SHA256SUMS
sha256sum -c SHA256SUMS
[ "$(wc -l < SHA256SUMS)" -eq 11 ] || fail "SHA256SUMS has $(wc -l < SHA256SUMS) lines, expected 11"
cat SHA256SUMS
