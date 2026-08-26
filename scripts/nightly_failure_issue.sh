#!/usr/bin/env bash
# Files or updates the labeled failure issue for a nightly e2e job (#434,
# #410). One definition shared by both nightly jobs so the mechanics — label
# idempotency, first-open-issue selection, run URL construction — cannot
# diverge. The label keys the update lookup, so each job MUST pass its own
# label: with a shared label one suite's failures get appended to the other
# suite's open issue and the per-suite signal is buried (#410 review).
#
# Inputs (env): GH_TOKEN, GITHUB_REPOSITORY, GITHUB_SERVER_URL, GITHUB_RUN_ID
# Arguments: <label> <label-description> <issue-title> <comment-prefix> <body-prefix> <ref>
# The run URL and "(refs <ref>)" are appended to the comment/body here.
set -Eeuo pipefail

label="$1"
label_desc="$2"
title="$3"
comment_prefix="$4"
body_prefix="$5"
ref="$6"

gh label create "$label" --repo "$GITHUB_REPOSITORY" \
	--color D93F0B --description "$label_desc" --force
run_url="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
existing=$(gh issue list --repo "$GITHUB_REPOSITORY" --label "$label" \
	--state open --json number --jq '.[0].number // empty')
if [ -n "$existing" ]; then
	gh issue comment "$existing" --repo "$GITHUB_REPOSITORY" \
		--body "$comment_prefix: $run_url"
else
	gh issue create --repo "$GITHUB_REPOSITORY" --label "$label" \
		--title "$title" \
		--body "$body_prefix: $run_url (refs $ref)"
fi
