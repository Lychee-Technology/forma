#!/usr/bin/env bash
# The MiniStack strict DSQL integration lane (#739): make test-dsql-ministack.
#
#   run   start a pinned MiniStack, create a cluster with a SQL data plane,
#         prove the strict proxy answers, run the aurora-dsql suites through
#         it, check their skips against the ledger, and tear everything down
#   down  remove what an interrupted run left behind
#
# Every failure is a non-zero exit, including a failed cleanup. Nothing here
# skips: a run that cannot reach the proxy fails. CI runs this same script.
#
# Set MINISTACK_DSQL_PROJECT to name the container and network, DSQL_BASE_PORT
# for the SQL port on 127.0.0.1, DSQL_DOCKER_NETWORK for another name for the
# network, which the run creates and removes and so must not already exist.
# Logs go to .artifacts/dsql-ministack/. See docs/dsql-ministack.md, which also
# says what a pass here does not establish.

set -Eeuo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

export GOCACHE="${GOCACHE:-$repo_root/.gocache}"
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.26.0+auto}"
export GOFLAGS="${GOFLAGS:--buildvcs=false}"

# Both images are pinned by digest. To move them, see docs/dsql-ministack.md:
# MiniStack's proxy rules change between releases.
MINISTACK_IMAGE="ministackorg/ministack:1.5.38@sha256:c2f82812bec8eb1145a5f09b9937e51da5e796ad2240bcb6e2ae955ee5ee3930"
BACKEND_IMAGE="postgres:16-alpine@sha256:aa6cb33cf4c19c765cefcf48d96176258800870a9a1e930ad126200f9d825139"

project="${MINISTACK_DSQL_PROJECT:-forma-dsql-ministack}"
network="${DSQL_DOCKER_NETWORK:-$project}"
sql_port="${DSQL_BASE_PORT:-25432}"
artifacts="$repo_root/.artifacts/dsql-ministack"
skip_ledger="$repo_root/scripts/dsql_ministack_expected_skips.txt"
dsql_host="dsql.us-east-1.amazonaws.com"

log() { echo "dsql-ministack: $*"; }
die() {
	echo "dsql-ministack: FAILED: $*" >&2
	exit 1
}

# gateway_url answers the published address of MiniStack's API, or fails when
# its container is not running.
gateway_url() {
	local published
	published=$(docker port "$project" 4566/tcp 2>/dev/null | head -n 1) || return 1
	[ -n "$published" ] || return 1
	echo "http://$published"
}

api() {
	local path=$1
	shift
	curl -sS -m 30 -H "Host: $dsql_host" -H 'Content-Type: application/json' "$@" "$(gateway_url)$path"
}

# json_string reads a string member of MiniStack's flat JSON answers, so the
# lane needs neither jq nor a Python on the host.
json_string() {
	sed -nE 's/.*"'"$1"'": *"([^"]*)".*/\1/p'
}

docker_socket() {
	case "${DOCKER_HOST:-}" in
	"") echo /var/run/docker.sock ;;
	unix://*) echo "${DOCKER_HOST#unix://}" ;;
	*) die "MiniStack starts its PostgreSQL backend through a mounted socket, and DOCKER_HOST=$DOCKER_HOST is not a local unix socket" ;;
	esac
}

preflight() {
	local tool
	for tool in docker curl go; do
		command -v "$tool" >/dev/null || die "$tool is not installed"
	done
	docker info >/dev/null 2>&1 || die "no access to a Docker daemon ('docker info' failed): MiniStack needs one for the cluster's PostgreSQL backend"
	local socket
	socket=$(docker_socket)
	[ -S "$socket" ] || die "$socket is not a socket: MiniStack cannot start the cluster's PostgreSQL backend without it, and would answer with a metadata-only cluster"
}

# Pulled up front so a failed pull is reported as one. MiniStack would turn a
# backend image it cannot get into a metadata-only cluster.
pull_images() {
	log "pulling the pinned images"
	docker pull --quiet "$MINISTACK_IMAGE" >/dev/null || die "cannot pull $MINISTACK_IMAGE"
	docker pull --quiet "$BACKEND_IMAGE" >/dev/null || die "cannot pull $BACKEND_IMAGE"
}

# claim_network creates the lane's network. Docker refuses a second network of
# one name, so this is what makes the run the owner of what teardown removes:
# a run that loses here, to an active or interrupted one, must not tear it down.
claim_network() {
	docker network create "$network" >/dev/null ||
		die "cannot create Docker network $network: another run is active, or one was interrupted; 'make test-dsql-ministack-down' removes what it left"
}

start_ministack() {
	# The SQL port is published on loopback because the backend is a
	# passwordless superuser behind a proxy that accepts any credentials.
	docker run -d --name "$project" --network "$network" \
		-v "$(docker_socket):/var/run/docker.sock" \
		-e DSQL_STRICT=1 \
		-e "DSQL_BASE_PORT=$sql_port" \
		-e "DSQL_PG_IMAGE=$BACKEND_IMAGE" \
		-e "DOCKER_NETWORK=$network" \
		-p "127.0.0.1:$sql_port:$sql_port" \
		-p 127.0.0.1::4566 \
		"$MINISTACK_IMAGE" >/dev/null || die "cannot start MiniStack; is port $sql_port free? (DSQL_BASE_PORT moves it)"

	local url attempt
	for attempt in $(seq 1 60); do
		if url=$(gateway_url) && curl -fsS -m 5 "$url/_ministack/health" >/dev/null 2>&1; then
			log "MiniStack is up at $url"
			return
		fi
		sleep 1
	done
	die "MiniStack's API did not answer within 60s"
}

# create_cluster creates the lane's cluster and sets cluster_id and dsn. ACTIVE
# only says the control plane finished: MiniStack also reports it for a cluster
# whose backend failed to start, so the backend is checked here and the SQL path
# by the gate that follows.
create_cluster() {
	local created status endpoint port attempt
	# Deletion protection, on by default as on AWS, would block the teardown.
	created=$(api /cluster -X POST -d '{"deletionProtectionEnabled":false}') || die "CreateCluster failed"
	cluster_id=$(echo "$created" | json_string identifier)
	[ -n "$cluster_id" ] || die "CreateCluster answered without an identifier: $created"
	for attempt in $(seq 1 120); do
		created=$(api "/cluster/$cluster_id") || die "GetCluster failed"
		status=$(echo "$created" | json_string status)
		[ "$status" = ACTIVE ] && break
		sleep 1
	done
	[ "$status" = ACTIVE ] || die "cluster $cluster_id is still $status after 120s"

	docker container inspect "ministack-dsql-$cluster_id" >/dev/null 2>&1 ||
		die "cluster $cluster_id is ACTIVE with no backend container: MiniStack degraded it to metadata-only (see $artifacts/ministack.log)"

	endpoint=$(echo "$created" | json_string endpoint)
	port=${endpoint##*:}
	[ "$port" = "$sql_port" ] || die "cluster $cluster_id reports endpoint '$endpoint', but only port $sql_port is published"
	# No password: the proxy accepts any credentials, and none is needed.
	dsn="postgres://admin@127.0.0.1:$port/postgres?sslmode=disable"
	log "cluster $cluster_id reports endpoint $endpoint"
}

# pin_isolation gives the backend DSQL's one isolation level. PostgreSQL's
# default is READ COMMITTED, where a guard-row conflict waits and then commits
# rather than failing with 40001. The proxy sends no client startup parameter
# to the backend, so this is set on the database. The gate asserts it took.
pin_isolation() {
	docker exec "ministack-dsql-$cluster_id" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -q \
		-c "ALTER DATABASE postgres SET default_transaction_isolation TO 'repeatable read'" ||
		die "cannot set the backend's isolation level"
}

failed_suites=()

# run_suite runs one suite and records a failure without stopping, so a run
# shows every failing suite. floor is the fewest passes that count as having
# run it: a -run pattern that stops matching passes with no tests at all.
run_suite() {
	local name=$1 floor=$2
	shift 2
	local suite_log="$artifacts/$name.log" rc passed failed skipped
	log "suite $name: go test $*"
	set +e
	go test -count=1 -v "$@" 2>&1 | tee "$suite_log" | grep -E --line-buffered '^ *--- (FAIL|SKIP)|^(ok|FAIL|panic)'
	rc=${PIPESTATUS[0]}
	set -e
	passed=$(grep -cE '^ *--- PASS' "$suite_log" || true)
	failed=$(grep -cE '^ *--- FAIL' "$suite_log" || true)
	skipped=$(grep -cE '^ *--- SKIP' "$suite_log" || true)
	printf '%-14s pass=%-5s fail=%-5s skip=%-5s\n' "$name" "$passed" "$failed" "$skipped" >>"$artifacts/summary.txt"
	if [ "$rc" -ne 0 ]; then
		failed_suites+=("$name")
	elif [ "$passed" -lt "$floor" ]; then
		echo "dsql-ministack: suite $name passed only $passed tests, fewer than its floor of $floor: it stopped running what it should" >&2
		failed_suites+=("$name")
	fi
}

# check_skips fails on a skip no ledger line covers, on a line that covers no
# skip, and on a line without a reason: a skip is a decision, never a drift.
check_skips() {
	local actual pattern reason unexpected rc=0
	actual=$(cat "$artifacts"/*.log | sed -nE 's/^ *--- SKIP: (.*) \([0-9.]+s\)$/\1/p' | sort -u)
	local anchored="$artifacts/skip-patterns.txt"
	: >"$anchored"
	while IFS=$'\t' read -r pattern reason; do
		case "$pattern" in '' | '#'*) continue ;; esac
		if [ -z "$reason" ]; then
			echo "dsql-ministack: $skip_ledger gives no reason for: $pattern" >&2
			rc=1
		fi
		if ! grep -qE "^($pattern)\$" <<<"$actual"; then
			echo "dsql-ministack: $skip_ledger covers no skip any more; remove: $pattern" >&2
			rc=1
		fi
		echo "^($pattern)\$" >>"$anchored"
	done <"$skip_ledger"
	unexpected=$(grep -vE -f "$anchored" <<<"$actual" || true)
	if [ -n "$unexpected" ]; then
		echo "dsql-ministack: skips $skip_ledger does not cover:" >&2
		echo "$unexpected" >&2
		rc=1
	fi
	[ "$rc" -eq 0 ] || failed_suites+=("skip-ledger")
}

run_suites() {
	# Nothing may fall back to an integration PostgreSQL unnoticed.
	unset DATABASE_URL DB_HOST DB_PORT DB_USER DB_PASSWORD DB_NAME DB_SSL_MODE
	export FORMA_DSQL_TARGET=ministack FORMA_DSQL_DSN="$dsn"
	export FORMA_COORDINATION_DSN="$dsn" FORMA_COORDINATION_BACKEND=dsql

	: >"$artifacts/summary.txt"
	run_suite gate 1 -timeout=3m ./internal/testdb -run '^TestMiniStackStrictProxy$'
	if [ ${#failed_suites[@]} -ne 0 ]; then
		die "the strict SQL proxy is not reachable: no suite ran, see $artifacts/gate.log"
	fi
	run_suite entity 8 -timeout=5m ./internal -run '^TestDSQL(Engine|ClusterProbe)'
	run_suite cdc 8 -timeout=5m ./internal/cdc -run '^(TestLeaseSchemaLock|TestMarkInChunksRetriesARealConflict)'
	run_suite coordination 90 -timeout=20m ./internal/coordinationtest/
	check_skips

	cat "$artifacts/summary.txt"
	if [ ${#failed_suites[@]} -ne 0 ]; then
		die "failing: ${failed_suites[*]}; logs in $artifacts"
	fi
	log "passed on MiniStack's strict proxy. This is an emulator result, not Aurora DSQL's: see docs/dsql-ministack.md"
}

collect_logs() {
	mkdir -p "$artifacts"
	docker logs "$project" >"$artifacts/ministack.log" 2>&1 || true
	local backend
	for backend in $(docker ps -aq --filter label=ministack=dsql --filter "network=$network"); do
		docker logs "$backend" >"$artifacts/backend-$backend.log" 2>&1 || true
	done
}

# delete_clusters deletes every cluster through MiniStack's API and waits for
# it to remove their backends, so a MiniStack that cannot clean up is reported.
delete_clusters() {
	local failed=0 id attempt
	gateway_url >/dev/null 2>&1 || return 0
	for id in $(api /cluster 2>/dev/null | grep -oE '"identifier": *"[^"]*"' | sed -E 's/.*"([^"]*)"$/\1/'); do
		if ! api "/cluster/$id" -X DELETE -o /dev/null -f; then
			echo "dsql-ministack: DeleteCluster failed for $id" >&2
			failed=1
			continue
		fi
		for attempt in $(seq 1 15); do
			docker container inspect "ministack-dsql-$id" >/dev/null 2>&1 || break
			sleep 1
		done
		if docker container inspect "ministack-dsql-$id" >/dev/null 2>&1; then
			echo "dsql-ministack: MiniStack left the backend of deleted cluster $id behind" >&2
			failed=1
		fi
	done
	return "$failed"
}

# teardown removes the lane's container, network and backends, and only those:
# backends are found by MiniStack's label on this lane's own network. It tries
# every step and fails if any failed, or if anything is left.
teardown() {
	local failed=0 backends
	delete_clusters || failed=1
	backends=$(docker ps -aq --filter label=ministack=dsql --filter "network=$network")
	if [ -n "$backends" ]; then
		# shellcheck disable=SC2086 # one container ID per word
		docker rm -f -v $backends >/dev/null || failed=1
	fi
	if docker container inspect "$project" >/dev/null 2>&1; then
		docker rm -f -v "$project" >/dev/null || failed=1
	fi
	if docker network inspect "$network" >/dev/null 2>&1; then
		docker network rm "$network" >/dev/null || failed=1
	fi
	if docker container inspect "$project" >/dev/null 2>&1 || docker network inspect "$network" >/dev/null 2>&1; then
		failed=1
	fi
	if [ "$failed" -ne 0 ]; then
		echo "dsql-ministack: FAILED: cleanup was incomplete; check 'docker ps -a' and 'docker network ls', then run 'make test-dsql-ministack-down'" >&2
		return 1
	fi
	log "torn down"
}

on_exit() {
	local rc=$?
	trap - EXIT
	collect_logs
	teardown || rc=1
	exit "$rc"
}

run() {
	preflight
	rm -rf "$artifacts"
	mkdir -p "$artifacts"
	pull_images
	claim_network
	trap on_exit EXIT
	trap 'exit 130' INT
	trap 'exit 143' TERM
	start_ministack
	create_cluster
	pin_isolation
	run_suites
}

case "${1:-}" in
run) run ;;
down)
	docker info >/dev/null 2>&1 || die "no access to a Docker daemon ('docker info' failed)"
	teardown
	;;
*)
	echo "usage: $0 run|down" >&2
	exit 2
	;;
esac
