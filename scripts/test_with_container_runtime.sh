#!/usr/bin/env bash

set -Eeuo pipefail

podman_service_pid=""

log() {
	printf '[test-with-container-runtime] %s\n' "$*"
}

die() {
	printf '[test-with-container-runtime] error: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	if [ -n "$podman_service_pid" ]; then
		kill "$podman_service_pid" 2>/dev/null || true
	fi
}

command_available() {
	command -v "$1" >/dev/null 2>&1
}

docker_info_succeeds() {
	command_available docker || return 1
	if command_available timeout; then
		timeout 10s docker info >/dev/null 2>&1
		return
	fi
	docker info >/dev/null 2>&1
}

docker_command_is_podman() {
	command_available docker || return 1
	local docker_path
	docker_path="$(command -v docker)"
	case "$(readlink -f "$docker_path" 2>/dev/null || printf '%s' "$docker_path")" in
		*podman*) return 0 ;;
		*) return 1 ;;
	esac
}

wait_for_socket() {
	local socket="$1"
	for _ in $(seq 1 40); do
		if [ -S "$socket" ]; then
			return 0
		fi
		sleep 0.25
	done
	return 1
}

configure_podman() {
	command_available podman || die "Podman was selected but the podman command is unavailable"

	local runtime_dir socket service_log
	runtime_dir="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
	socket="$runtime_dir/podman/podman.sock"
	service_log="${TMPDIR:-/tmp}/forma-podman-service.log"
	export DOCKER_HOST="unix://$socket"

	if [ ! -S "$socket" ]; then
		if command_available systemctl && systemctl --user start podman.socket >/dev/null 2>&1; then
			:
		else
			mkdir -p "$(dirname "$socket")"
			log "starting rootless Podman API service"
			podman system service --time=0 "$DOCKER_HOST" >"$service_log" 2>&1 &
			podman_service_pid=$!
		fi
	fi

	wait_for_socket "$socket" || die "Podman socket did not become ready: $socket"
	# Rootless Podman cannot run the Ryuk reaper against its API socket reliably.
	export TESTCONTAINERS_RYUK_DISABLED=true
	log "using Podman socket: $DOCKER_HOST"
	log "TESTCONTAINERS_RYUK_DISABLED=true"
}

trap cleanup EXIT

runtime=""
if [ -n "${DOCKER_HOST:-}" ]; then
	case "$DOCKER_HOST" in
		*podman.sock*) runtime="podman" ;;
		*) runtime="docker" ;;
	esac
elif docker_command_is_podman; then
	runtime="podman"
elif docker_info_succeeds; then
	runtime="docker"
elif command_available podman; then
	runtime="podman"
elif command_available docker; then
	runtime="docker"
else
	die "neither a usable Docker daemon nor Podman was found"
fi

if [ "$runtime" = "podman" ]; then
	configure_podman
else
	log "using Docker"
fi

make test
