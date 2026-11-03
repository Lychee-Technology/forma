#!/usr/bin/env bash
# EXPERIMENTAL: reproduces every measurement of the DSQL typed call-site spike
# (docs/spikes/dsql-typed-callsite-spike.md). Run from the forma checkout that
# holds the spike branch. Needs only the Go toolchain and the module cache;
# the dsql-lint steps reuse the PR #801 gate's pinned download.
set -euo pipefail
ROOT=$(git rev-parse --show-toplevel)
OUT=${OUT:-$(mktemp -d)}
export GOFLAGS=-buildvcs=false GOTOOLCHAIN=go1.26.0+auto
SPIKE=$ROOT/internal/dsqllint/spike
PKGS="./cmd/tools ./internal ./internal/cdc ./internal/declaredindex ./internal/federated ./internal/widthaudit"
echo "outputs: $OUT"

# 0. Baseline: PR #801 scanner timing and counts.
(cd "$ROOT" && go test -tags dsqlspike -count=1 -run TestSpikeBaselineScan -v ./internal/dsqllint | grep BASELINE)

# 1. Build the prototype (separate module; never part of forma's build).
(cd "$SPIKE" && GOFLAGS=-mod=mod go vet ./... && GOFLAGS=-mod=mod go test ./sites ./evidence &&
  GOFLAGS=-mod=mod go build -o "$OUT/sqlsites" ./cmd/sqlsites &&
  GOFLAGS=-mod=mod go build -o "$OUT/attribute" ./cmd/attribute &&
  GOFLAGS=-mod=mod go build -o "$OUT/evidence" ./cmd/evidence)

# 2. Static inventory of the shipped closure, both wrapper modes.
"$OUT/sqlsites" -dir "$ROOT" -mode verbatim -out "$OUT/sites.json"
"$OUT/sqlsites" -dir "$ROOT" -mode taint -out "$OUT/sites-taint.json"
"$OUT/sqlsites" -dir "$ROOT" -patterns ./internal/dsqllint/spikefix -tags dsqlspike -out "$OUT/sites-fix.json" >/dev/null

# 3. Runtime capture: real corpus and synthetic fixture, three build configs.
for cfg in default noinline race; do
  case $cfg in default) X="";; noinline) X="-gcflags=all=-l";; race) X="-race";; esac
  mkdir -p "$OUT/real-$cfg" "$OUT/fix-$cfg"
  (cd "$ROOT" && SPIKE_ATTR_OUT="$OUT/real-$cfg" go test -count=1 $X -run '^TestDSQLCorpus' $PKGS >/dev/null)
  (cd "$ROOT" && SPIKE_ATTR_OUT="$OUT/fix-$cfg" go test -tags dsqlspike -count=1 $X ./internal/dsqllint/spikefix >/dev/null)
  "$OUT/attribute" -sites "$OUT/sites.json" -records "$OUT/real-$cfg/records.jsonl" -root "$ROOT" -out "$OUT/attr-real-$cfg.json" | tail -1
  "$OUT/attribute" -sites "$OUT/sites-fix.json" -records "$OUT/fix-$cfg/records.jsonl" -root "$ROOT" -out "$OUT/attr-fix-$cfg.json" | tail -1
done

# 4. Evidence join against the sample manifest (regenerate it if site IDs moved).
"$OUT/evidence" -sites "$OUT/sites.json" -outcomes "$OUT/attr-real-default.json" \
  -manifest "$ROOT/docs/spikes/dsql-typed-callsite-data/manifest-sample.txt" || echo "join reported problems (expected if the tree differs from the spike commit)"

# 5. Mutation tests in a disposable worktree (reset after every case).
MUT=$(mktemp -d)/mut
git -C "$ROOT" worktree add --detach "$MUT" HEAD >/dev/null
(cd "$SPIKE" && SPIKE_MUT_DIR="$MUT" SPIKE_MUT_OUT="$OUT/mutations.json" GOFLAGS=-mod=mod go test ./sites -run 'TestMutations$' -count=1 -timeout 60m -v | grep mutation_test)
(cd "$SPIKE" && SPIKE_MUT_DIR="$MUT" SPIKE_MUT_OUT="$OUT/mutations-pr.json" GOFLAGS=-mod=mod go test ./sites -run TestMutationsPRScanner -count=1 -timeout 60m -v | grep mutation_pr_test)
git -C "$ROOT" worktree remove --force "$MUT"

# 6. DuckDB parser as an emptiness oracle (inputs: corpus kept by the PR gate).
echo "see docs/spikes/dsql-typed-callsite-spike.md section 11 for the parse-oracle run"
