#!/usr/bin/env bash
# Cross-version Parquet storage check (#753).
#
# Builds cmd/duckdbcompat against the current DuckDB driver (go.mod) and the
# previous one (scripts/duckdbcompat/previous.mod), then runs each build on
# the other's files:
#
#   upgrade   the current engine reads a file the previous one wrote
#   rollback  the previous engine reads a file the current one wrote
#   compact   the current engine rewrites both into one file (compaction's
#             union_by_name read + COPY), and both engines read the result
#
# Every file is written with Forma's production COPY options
# (internal/parquetformat) and checked row for row against the probe's
# fixture. Set DUCKDB_COMPAT_KEEP=1 to keep the work directory.
#
# See docs/duckdb-upgrades.md for when to run it and how to move the pair.

set -Eeuo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

export GOCACHE="${GOCACHE:-$repo_root/.gocache}"
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.26.0+auto}"
export GOFLAGS="${GOFLAGS:--buildvcs=false}"

work=$(mktemp -d "${TMPDIR:-/tmp}/duckdb-storage-compat.XXXXXX")
cleanup() {
	if [ "${DUCKDB_COMPAT_KEEP:-0}" = "1" ]; then
		echo "[duckdb-storage-compat] kept $work"
	else
		rm -rf "$work"
	fi
}
trap cleanup EXIT

step() {
	printf '\n[duckdb-storage-compat] %s\n' "$*"
}

step "building the probe against go.mod (current driver)"
go build -o "$work/current" ./cmd/duckdbcompat
step "building the probe against scripts/duckdbcompat/previous.mod (previous driver)"
go build -mod=readonly -modfile=scripts/duckdbcompat/previous.mod -o "$work/previous" ./cmd/duckdbcompat

"$work/current" version
"$work/previous" version

step "write: each engine writes the fixture"
"$work/previous" write "$work/previous.parquet"
"$work/current" write "$work/current.parquet"

step "upgrade: the current engine reads the previous engine's file"
"$work/current" verify "$work/previous.parquet" 1

step "rollback: the previous engine reads the current engine's file"
"$work/previous" verify "$work/current.parquet" 1

step "compact: the current engine rewrites both files into one"
"$work/current" merge "$work/merged.parquet" "$work/previous.parquet" "$work/current.parquet"
"$work/previous" verify "$work/merged.parquet" 2
"$work/current" verify "$work/merged.parquet" 2

step "passed: files written by either engine read back unchanged in the other"
