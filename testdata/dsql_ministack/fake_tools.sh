#!/usr/bin/env bash
# Stand-ins for docker, curl and go, for dsql_ministack_lane_test.go: symlinked
# under each name, they keep Docker's state as files in $FAKE_STATE, so the lane
# can be run against name collisions, overlapping runs and a Docker that fails,
# with no daemon. They follow what the real CLI does where the lane depends on it:
# names are unique, listings exit 0 when they find nothing, and a dead daemon fails
# every call. The lane's normal path against real Docker is `make test-dsql-ministack`.
#
#   $FAKE_STATE/fail/<key>   makes a call fail: docker-all, docker-<subcommand>
#                            (docker-network-rm), docker-ps-backends, curl-delete
#   $FAKE_STATE/hooks/<suite> runs once, halfway through that suite's fake go test
#   $FAKE_STATE/calls.log    one line per call
set -euo pipefail

state=$FAKE_STATE
owner_label="forma.dsql-ministack.project"
mkdir -p "$state/containers" "$state/networks" "$state/clusters" "$state/fail" "$state/hooks"
echo "$(basename "$0") $*" >>"$state/calls.log"

next_id() {
	local seq=0
	[ -f "$state/seq" ] && seq=$(cat "$state/seq")
	seq=$((seq + 1))
	echo "$seq" >"$state/seq"
	printf '%064x\n' "$seq"
}

# resolve_container prints the ID of a container given by ID or name.
resolve_container() {
	local dir
	for dir in "$state"/containers/*/; do
		[ -d "$dir" ] || continue
		if [ "$(basename "$dir")" = "$1" ] || [ "$(cat "$dir/name")" = "$1" ]; then
			basename "$dir"
			return 0
		fi
	done
	echo "Error: No such container: $1" >&2
	return 1
}

owner_of() {
	sed -n "s/^$owner_label=//p" "$1/labels"
}

docker_create() {
	local name="" network="" sql_port="" id
	local labels=()
	while [ $# -gt 1 ]; do
		case "$1" in
		--name) name=$2 ; shift 2 ;;
		--label) labels+=("$2") ; shift 2 ;;
		--network) network=$2 ; shift 2 ;;
		-e)
			case "$2" in DSQL_BASE_PORT=*) sql_port=${2#*=} ;; esac
			shift 2
			;;
		-v | -p) shift 2 ;;
		*) echo "fake docker create: unknown argument $1" >&2 ; exit 125 ;;
		esac
	done
	if [ -n "$name" ] && resolve_container "$name" >/dev/null 2>&1; then
		echo "Error: Conflict. The container name \"/$name\" is already in use" >&2
		exit 125
	fi
	id=$(next_id)
	mkdir -p "$state/containers/$id"
	echo "$name" >"$state/containers/$id/name"
	echo "$network" >"$state/containers/$id/network"
	echo "$sql_port" >"$state/containers/$id/sql_port"
	printf '%s\n' ${labels[@]+"${labels[@]}"} >"$state/containers/$id/labels"
	echo "$id"
}

docker_ps() {
	local quiet=0 format=0 id_filter="" name_filter="" label_filter="" network_filter="" dir id
	while [ $# -gt 0 ]; do
		case "$1" in
		-aq) quiet=1 ; shift ;;
		-a | --no-trunc) shift ;;
		--format) format=1 ; shift 2 ;;
		--filter)
			case "$2" in
			id=*) id_filter=${2#*=} ;;
			name=*) name_filter=${2#*=} ;;
			label=*)
				label_filter=${2#*=}
				if [ -e "$state/fail/docker-ps-backends" ]; then
					echo "Cannot connect to the Docker daemon (fake: docker-ps-backends)" >&2
					exit 1
				fi
				;;
			network=*) network_filter=${2#*=} ;;
			esac
			shift 2
			;;
		*) echo "fake docker ps: unknown argument $1" >&2 ; exit 125 ;;
		esac
	done
	for dir in "$state"/containers/*/; do
		[ -d "$dir" ] || continue
		id=$(basename "$dir")
		[ -z "$id_filter" ] || [ "$id" = "$id_filter" ] || continue
		[ -z "$name_filter" ] || grep -q "$name_filter" "$dir/name" || continue
		[ -z "$label_filter" ] || grep -qx "$label_filter" "$dir/labels" || continue
		[ -z "$network_filter" ] || [ "$(cat "$dir/network")" = "$network_filter" ] || continue
		if [ "$quiet" -eq 1 ]; then
			echo "$id"
		elif [ "$format" -eq 1 ]; then
			echo "$id $(cat "$dir/name") $(owner_of "$dir")"
		fi
	done
}

docker_network() {
	local sub=$1 dir id
	shift
	case "$sub" in
	create)
		local label=""
		if [ "$1" = --label ]; then
			label=$2
			shift 2
		fi
		for dir in "$state"/networks/*/; do
			if [ -d "$dir" ] && [ "$(cat "$dir/name")" = "$1" ]; then
				echo "Error response from daemon: network with name $1 already exists" >&2
				exit 1
			fi
		done
		id=$(next_id)
		mkdir -p "$state/networks/$id"
		echo "$1" >"$state/networks/$id/name"
		echo "$label" >"$state/networks/$id/labels"
		echo "$id"
		;;
	ls)
		local quiet=0 id_filter="" name_filter=""
		while [ $# -gt 0 ]; do
			case "$1" in
			-q) quiet=1 ; shift ;;
			--no-trunc) shift ;;
			--format) shift 2 ;;
			--filter)
				case "$2" in
				id=*) id_filter=${2#*=} ;;
				name=*) name_filter=${2#*=} ;;
				esac
				shift 2
				;;
			*) echo "fake docker network ls: unknown argument $1" >&2 ; exit 125 ;;
			esac
		done
		for dir in "$state"/networks/*/; do
			[ -d "$dir" ] || continue
			id=$(basename "$dir")
			[ -z "$id_filter" ] || [ "$id" = "$id_filter" ] || continue
			[ -z "$name_filter" ] || grep -q "$name_filter" "$dir/name" || continue
			if [ "$quiet" -eq 1 ]; then
				echo "$id"
			else
				echo "$id $(cat "$dir/name") $(owner_of "$dir")"
			fi
		done
		;;
	rm)
		[ -d "$state/networks/$1" ] || { echo "Error: network $1 not found" >&2; exit 1; }
		for dir in "$state"/containers/*/; do
			if [ -d "$dir" ] && [ "$(cat "$dir/network")" = "$(cat "$state/networks/$1/name")" ]; then
				echo "Error response from daemon: network $1 has active endpoints" >&2
				exit 1
			fi
		done
		rm -rf "${state:?}/networks/$1"
		;;
	*) echo "fake docker network: unknown subcommand $sub" >&2 ; exit 125 ;;
	esac
}

fake_docker() {
	local sub=${1:-} key id
	key="docker-$sub"
	[ "$sub" != network ] || key="docker-network-${2:-}"
	if [ -e "$state/fail/docker-all" ] || [ -e "$state/fail/$key" ]; then
		echo "Cannot connect to the Docker daemon (fake: $key)" >&2
		exit 1
	fi
	shift
	case "$sub" in
	info | pull | exec) ;;
	create) docker_create "$@" ;;
	start)
		id=$(resolve_container "$1")
		touch "$state/containers/$id/running"
		echo "$id"
		;;
	port)
		id=$(resolve_container "$1")
		if [ ! -e "$state/containers/$id/running" ]; then
			echo "no public port '$2' published for $1" >&2
			exit 1
		fi
		echo "127.0.0.1:$((40000 + 16#${id: -8}))"
		;;
	logs)
		id=$(resolve_container "$1")
		echo "log of $id"
		;;
	ps) docker_ps "$@" ;;
	network) docker_network "$@" ;;
	rm)
		shift 2 # -f -v
		for key in "$@"; do
			id=$(resolve_container "$key")
			rm -rf "${state:?}/containers/$id"
		done
		;;
	*) echo "fake docker: unknown subcommand $sub" >&2 ; exit 125 ;;
	esac
}

cluster_json() {
	echo "{\"identifier\": \"$1\", \"status\": \"ACTIVE\", \"endpoint\": \"$1.dsql:$2\"}"
}

# fake_curl answers as the MiniStack whose API port the URL names.
fake_curl() {
	local method=GET url="" cluster="" port path container sql_port network
	while [ $# -gt 0 ]; do
		case "$1" in
		-X) method=$2 ; shift 2 ;;
		-m | -H | -d | -o) shift 2 ;;
		-*) shift ;;
		*) url=$1 ; shift ;;
		esac
	done
	path=/${url#http://*/}
	port=${url#http://*:}
	port=${port%%/*}
	[ "$path" != /_ministack/health ] || { echo ok; return 0; }
	[ "$method" != DELETE ] || [ ! -e "$state/fail/curl-delete" ] || exit 22
	container=$(printf '%064x' $((port - 40000)))
	[ -d "$state/containers/$container" ] || { echo "curl: (7) connection refused" >&2; exit 7; }
	sql_port=$(cat "$state/containers/$container/sql_port")
	network=$(cat "$state/containers/$container/network")
	mkdir -p "$state/clusters/$port"
	case "$method $path" in
	"POST /cluster")
		cluster="c$port"
		touch "$state/clusters/$port/$cluster"
		docker create --name "ministack-dsql-$cluster" --label ministack=dsql --network "$network" >/dev/null
		docker start "ministack-dsql-$cluster" >/dev/null
		cluster_json "$cluster" "$sql_port"
		;;
	"GET /cluster")
		for cluster in "$state/clusters/$port"/*; do
			[ -e "$cluster" ] || continue
			cluster_json "$(basename "$cluster")" "$sql_port"
		done
		;;
	"GET /cluster/"*) cluster_json "${path#/cluster/}" "$sql_port" ;;
	"DELETE /cluster/"*)
		cluster=${path#/cluster/}
		rm -f "$state/clusters/$port/$cluster"
		docker rm -f -v "ministack-dsql-$cluster" >/dev/null
		;;
	*) echo "fake curl: unknown request $method $path" >&2 ; exit 22 ;;
	esac
}

# fake_go prints a suite's results, with its hook in the middle of them, as a run
# overlapping this one would land.
fake_go() {
	local suite passes i hook arg
	for arg in "$@"; do
		case "$arg" in
		./internal/testdb) suite=gate passes=1 ;;
		./internal) suite=entity passes=10 ;;
		./internal/cdc) suite=cdc passes=9 ;;
		./cmd/tools) suite=ledger passes=6 ;;
		./internal/coordinationtest/) suite=coordination passes=96 ;;
		esac
	done
	for ((i = 1; i <= passes; i++)); do
		echo "--- PASS: Test$i (0.00s)"
		if [ "$i" -eq $((passes / 2 + 1)) ]; then
			hook="$state/hooks/$suite"
			if [ -e "$hook" ]; then
				mv "$hook" "$hook.ran"
				bash "$hook.ran" >/dev/null
			fi
		fi
	done
	[ "$suite" != cdc ] || echo "--- SKIP: TestLedgered (0.00s)"
	echo "ok  	$suite	0.001s"
}

case "$(basename "$0")" in
docker) fake_docker "$@" ;;
curl) fake_curl "$@" ;;
go) fake_go "$@" ;;
*) echo "fake tools: unknown name $0" >&2 ; exit 125 ;;
esac
