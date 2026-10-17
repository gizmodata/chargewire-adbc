#!/usr/bin/env bash
# Fail if the Linux library needs a glibc newer than 2.17.
set -euo pipefail
LIB="${1:?usage: check_glibc.sh <libadbc_driver_warpsql.so>}"
MAX=$(objdump -T "$LIB" | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -V | tail -1)
echo "highest glibc symbol version: ${MAX:-none}"
if [ -n "$MAX" ] && [ "$(printf '%s\n2.17\n' "$MAX" | sort -V | tail -1)" != "2.17" ]; then
  echo "::error::$LIB needs glibc $MAX (> 2.17)" >&2
  exit 1
fi
