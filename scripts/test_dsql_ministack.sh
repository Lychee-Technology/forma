#!/usr/bin/env bash
# The MiniStack strict DSQL integration lane (#739): make test-dsql-ministack.
#
#   run   start a pinned MiniStack, create a cluster with a SQL data plane,
#         prove the strict proxy answers, run the aurora-dsql suites through
#         it (the generation-ledger upgrade suite with its parquet tier on
#         MiniStack's S3), check their skips against the skip ledger, and
#         tear everything down
#   down  remove what an interrupted run left behind
#
# Every failure is a non-zero exit, including a failed cleanup. Nothing here
# skips: a run that cannot reach the proxy fails. CI runs this same script.
#
# Set MINISTACK_DSQL_PROJECT to name the container and network, DSQL_BASE_PORT
# for the SQL port on 127.0.0.1, DSQL_DOCKER_NETWORK for another name for the
# network. The run creates and removes both, and so neither may already exist.
# Logs go to .artifacts/dsql-ministack/<project>/. See docs/dsql-ministack.md,
# which also says what a pass here does not establish.

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
artifacts="$repo_root/.artifacts/dsql-ministack/$project"
skip_ledger="$repo_root/scripts/dsql_ministack_expected_skips.txt"
dsql_host="dsql.us-east-1.amazonaws.com"
owner_label="forma.dsql-ministack.project"

# What this invocation may touch. A run records the IDs Docker gave it when it
# created each one, and works through those IDs alone, never a name. Whatever
# stands under the lane's names without having been created here is not the
# lane's: another run's, or something unrelated.
owned_container=""
owned_network=""
artifacts_claimed=0

log() { echo "dsql-ministack: $*"; }
die() {
	echo "dsql-ministack: FAILED: $*" >&2
	exit 1
}

# gateway_url answers the published address of MiniStack's API, or fails when
# its container is not running.
gateway_url() {
	local published
	published=$(docker port "$owned_container" 4566/tcp 2>/dev/null | head -n 1) || return 1
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
# one name, so a run that loses here, to an active or interrupted one, or to an
# unrelated network, owns nothing and so removes nothing.
claim_network() {
	owned_network=$(docker network create --label "$owner_label=$project" "$network") && [ -n "$owned_network" ] ||
		die "cannot create Docker network $network: another run is active, or one was interrupted; 'make test-dsql-ministack-down' removes what it left"
}

# claim_container creates MiniStack's container without starting it. Docker
# refuses a second container of one name, so this is the claim on the project,
# which two runs with different networks would otherwise share. Creating it apart
# from starting it means a start that fails, on an occupied port, leaves a
# container the run knows is its own, and one that loses here has touched nothing.
claim_container() {
	# The SQL port is published on loopback because the backend is a
	# passwordless superuser behind a proxy that accepts any credentials.
	owned_container=$(docker create --name "$project" --label "$owner_label=$project" --network "$network" \
		-v "$(docker_socket):/var/run/docker.sock" \
		-e DSQL_STRICT=1 \
		-e "DSQL_BASE_PORT=$sql_port" \
		-e "DSQL_PG_IMAGE=$BACKEND_IMAGE" \
		-e "DOCKER_NETWORK=$network" \
		-p "127.0.0.1:$sql_port:$sql_port" \
		-p 127.0.0.1::4566 \
		"$MINISTACK_IMAGE") && [ -n "$owned_container" ] ||
		die "cannot create container $project, and an existing one stays as it is: another run is active, or one was interrupted; 'make test-dsql-ministack-down' removes what it left"
}

# claim_artifacts clears the project's logs. Only the run holding the project's
# container name gets here, so what it clears is no other run's, and a run with
# another project has another directory.
claim_artifacts() {
	rm -rf "$artifacts" && mkdir -p "$artifacts" || die "cannot prepare $artifacts"
	artifacts_claimed=1
}

start_ministack() {
	docker start "$owned_container" >/dev/null || die "cannot start MiniStack; is port $sql_port free? (DSQL_BASE_PORT moves it)"

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
	local created status endpoint port attempt backend
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

	backend=$(container_named "ministack-dsql-$cluster_id") || die "cannot list containers to find the backend of cluster $cluster_id"
	[ -n "$backend" ] ||
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

# count_in_log prints how many lines of a log match. grep's 1 means none and
# still prints 0. Anything else means the log could not be read, which must not
# read as a count: a blank one would pass the floor below.
count_in_log() {
	local count rc=0
	count=$(grep -cE "$1" "$2") || rc=$?
	[ "$rc" -le 1 ] && [[ "$count" =~ ^[0-9]+$ ]] || return 1
	echo "$count"
}

# run_suite runs one suite and records a failure without stopping, so a run
# shows every failing suite. floor is the fewest passes that count as having
# run it: a -run pattern that stops matching passes with no tests at all.
run_suite() {
	local name=$1 floor=$2
	shift 2
	local suite_log="$artifacts/$name.log" passed failed skipped
	local status
	log "suite $name: go test $*"
	set +e
	go test -count=1 -v "$@" 2>&1 | tee "$suite_log" | grep -E --line-buffered '^ *--- (FAIL|SKIP)|^(ok|FAIL|panic)'
	status=("${PIPESTATUS[@]}")
	set -e
	if [ "${status[1]}" -ne 0 ] || [ "${status[2]}" -gt 1 ]; then
		echo "dsql-ministack: suite $name: its output could not be written to $suite_log" >&2
		failed_suites+=("$name")
		return 0
	fi
	if ! passed=$(count_in_log '^ *--- PASS' "$suite_log") ||
		! failed=$(count_in_log '^ *--- FAIL' "$suite_log") ||
		! skipped=$(count_in_log '^ *--- SKIP' "$suite_log"); then
		echo "dsql-ministack: suite $name: cannot read its results from $suite_log" >&2
		failed_suites+=("$name")
		return 0
	fi
	if ! printf '%-14s pass=%-5s fail=%-5s skip=%-5s\n' "$name" "$passed" "$failed" "$skipped" >>"$artifacts/summary.txt"; then
		echo "dsql-ministack: suite $name: cannot write to $artifacts/summary.txt" >&2
		failed_suites+=("$name")
		return 0
	fi
	if [ "${status[0]}" -ne 0 ]; then
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
	# The ledger suite's parquet tier lives on MiniStack's own S3, behind the
	# same API the cluster was created through.
	FORMA_DSQL_S3_ENDPOINT=$(gateway_url) || die "MiniStack's API has no published address for the ledger suite's S3"
	export FORMA_DSQL_S3_ENDPOINT

	: >"$artifacts/summary.txt"
	run_suite gate 1 -timeout=3m ./internal/testdb -run '^TestMiniStackStrictProxy$'
	if [ ${#failed_suites[@]} -ne 0 ]; then
		die "the strict SQL proxy is not reachable: no suite ran, see $artifacts/gate.log"
	fi
	run_suite entity 8 -timeout=5m ./internal -run '^TestDSQL(Engine|ClusterProbe)'
	run_suite cdc 8 -timeout=5m ./internal/cdc -run '^(TestLeaseSchemaLock|TestMarkInChunksRetriesARealConflict)'
	run_suite ledger 6 -timeout=10m ./cmd/tools -run '^TestLedgerUpgrade'
	run_suite coordination 90 -timeout=20m ./internal/coordinationtest/
	check_skips

	cat "$artifacts/summary.txt"
	if [ ${#failed_suites[@]} -ne 0 ]; then
		die "failing: ${failed_suites[*]}; logs in $artifacts"
	fi
	log "passed on MiniStack's strict proxy. This is an emulator result, not Aurora DSQL's: see docs/dsql-ministack.md"
}

# backends_on_network lists the backends on the lane's network, by the network's
# name: the filter misses a container that never started when given its ID. Only
# called with the network owned, so that the name is this run's network.
backends_on_network() {
	docker ps -aq --no-trunc --filter label=ministack=dsql --filter "network=$network"
}

# collect_logs writes to the artifacts only if this run claimed them: a run that
# lost its claim would be writing over the logs of the one that holds them.
collect_logs() {
	[ "$artifacts_claimed" -eq 1 ] || return 0
	local failed=0 backend backends
	mkdir -p "$artifacts" || return 1
	docker logs "$owned_container" >"$artifacts/ministack.log" 2>&1 || failed=1
	if backends=$(backends_on_network); then
		for backend in $backends; do
			docker logs "$backend" >"$artifacts/backend-$backend.log" 2>&1 || failed=1
		done
	else
		failed=1
	fi
	[ "$failed" -eq 0 ] || echo "dsql-ministack: could not collect every container's log" >&2
	return "$failed"
}

# container_named and network_named print "<id> <owner label>" for the resource
# of exactly that name, nothing when there is none, and fail when Docker cannot
# answer. A listing tells those apart; 'docker inspect' fails alike for both.
container_named() {
	docker ps -a --no-trunc --filter "name=$1" --format "{{.ID}} {{.Names}} {{.Label \"$owner_label\"}}" |
		awk -v name="$1" '$2 == name { print $1, $3 }'
}

network_named() {
	docker network ls --no-trunc --filter "name=$1" --format "{{.ID}} {{.Name}} {{.Label \"$owner_label\"}}" |
		awk -v name="$1" '$2 == name { print $1, $3 }'
}

# wait_backend_gone waits for MiniStack to remove a deleted cluster's backend.
wait_backend_gone() {
	local backend attempt
	for attempt in $(seq 1 15); do
		if ! backend=$(container_named "ministack-dsql-$1"); then
			echo "dsql-ministack: cannot tell whether deleted cluster $1 left its backend behind" >&2
			return 1
		fi
		[ -n "$backend" ] || return 0
		sleep 1
	done
	echo "dsql-ministack: MiniStack left the backend of deleted cluster $1 behind" >&2
	return 1
}

# delete_clusters deletes every cluster through MiniStack's API and waits for
# it to remove their backends, so a MiniStack that cannot clean up is reported.
# It only runs against the container this run owns. A container with no published
# API has none to call: remove_backends handles what it left, and reports Docker
# being unavailable, which also lands here.
delete_clusters() {
	local failed=0 clusters id
	gateway_url >/dev/null 2>&1 || return 0
	if ! clusters=$(api /cluster 2>/dev/null); then
		echo "dsql-ministack: cannot list clusters through MiniStack's API" >&2
		return 1
	fi
	for id in $(echo "$clusters" | grep -oE '"identifier": *"[^"]*"' | sed -E 's/.*"([^"]*)"$/\1/'); do
		if ! api "/cluster/$id" -X DELETE -o /dev/null -f; then
			echo "dsql-ministack: DeleteCluster failed for $id" >&2
			failed=1
			continue
		fi
		wait_backend_gone "$id" || failed=1
	done
	return "$failed"
}

remove_backends() {
	local backends
	if ! backends=$(backends_on_network); then
		echo "dsql-ministack: cannot list the backends on network $network" >&2
		return 1
	fi
	[ -n "$backends" ] || return 0
	# shellcheck disable=SC2086 # one container ID per word
	docker rm -f -v $backends >/dev/null
}

# remove_container and remove_network take an ID and answer whether it is gone:
# an ID Docker lists as absent is. An unanswered listing proves nothing, so they
# still try the removal, and fail unless a listing then confirms it.
remove_container() {
	local left
	if left=$(docker ps -aq --no-trunc --filter "id=$1") && [ -z "$left" ]; then
		return 0
	fi
	docker rm -f -v "$1" >/dev/null || return 1
	left=$(docker ps -aq --no-trunc --filter "id=$1") || return 1
	[ -z "$left" ]
}

remove_network() {
	local left
	if left=$(docker network ls -q --no-trunc --filter "id=$1") && [ -z "$left" ]; then
		return 0
	fi
	docker network rm "$1" >/dev/null || return 1
	left=$(docker network ls -q --no-trunc --filter "id=$1") || return 1
	[ -z "$left" ]
}

# teardown removes what this invocation owns and nothing else. Its callers run
# it in an OR-list, where errexit does not apply, so every step checks its own
# result: each step is tried, and any that failed or could not be confirmed
# fails the whole.
teardown() {
	local failed=0
	[ -n "$owned_container$owned_network" ] || return 0
	if [ -n "$owned_container" ]; then
		delete_clusters || failed=1
	fi
	if [ -n "$owned_network" ]; then
		remove_backends || failed=1
	fi
	if [ -n "$owned_container" ]; then
		remove_container "$owned_container" || failed=1
	fi
	if [ -n "$owned_network" ]; then
		remove_network "$owned_network" || failed=1
	fi
	if [ "$failed" -ne 0 ]; then
		echo "dsql-ministack: FAILED: cleanup was incomplete or could not be confirmed; check 'docker ps -a' and 'docker network ls', then run 'make test-dsql-ministack-down'" >&2
		return 1
	fi
	log "torn down"
}

on_exit() {
	local rc=$?
	trap - EXIT
	collect_logs || rc=1
	teardown || rc=1
	exit "$rc"
}

run() {
	preflight
	pull_images
	trap on_exit EXIT
	trap 'exit 130' INT
	trap 'exit 143' TERM
	claim_network
	claim_container
	claim_artifacts
	start_ministack
	create_cluster
	pin_isolation
	run_suites
}

# adopt prints the ID of a resource 'down' found, if the lane created it for this
# project. The label says so, and an interrupted run's IDs are gone with it.
adopt() {
	local kind=$1 name=$2 found=$3 id label
	[ -n "$found" ] || return 0
	read -r id label <<<"$found"
	if [ "$label" != "$project" ]; then
		echo "dsql-ministack: $kind $name is not this lane's (no $owner_label=$project label); leaving it as it is" >&2
		return 1
	fi
	echo "$id"
}

down() {
	local found failed=0
	docker info >/dev/null 2>&1 || die "no access to a Docker daemon ('docker info' failed)"
	found=$(container_named "$project") || die "cannot list containers"
	owned_container=$(adopt container "$project" "$found") || failed=1
	found=$(network_named "$network") || die "cannot list networks"
	owned_network=$(adopt network "$network" "$found") || failed=1
	teardown || failed=1
	return "$failed"
}

# The project names a path below as well as a container, so it has to be a name
# Docker would accept before anything is created or cleared under it.
[[ "$project" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]+$ ]] || die "MINISTACK_DSQL_PROJECT '$project' is not a valid container name"

case "${1:-}" in
run) run ;;
down) down ;;
*)
	echo "usage: $0 run|down" >&2
	exit 2
	;;
esac
