#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
: "${VERUS:=verus}"

OUT="${TMPDIR:-/tmp}/holt-verus-verify-$$"
mkdir -p "$OUT"
trap 'rm -rf "$OUT"' EXIT INT TERM

(cd "$OUT" && "$VERUS" "$ROOT/src/lib.rs" --crate-type=lib)
