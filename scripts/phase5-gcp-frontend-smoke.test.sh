#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
temp_root=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-public-smoke-test.XXXXXX")
trap 'rm -rf "$temp_root"' EXIT
mkdir -p "$temp_root/bin"

cat >"$temp_root/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
body_file=
headers_file=
url=
while [[ $# -gt 0 ]]; do
    case "$1" in
        -sS|-s|-S) shift ;;
        --connect-timeout|--max-time|-w) shift 2 ;;
        -o) body_file=$2; shift 2 ;;
        -D) headers_file=$2; shift 2 ;;
        *) url=$1; shift ;;
    esac
done
printf '%s\n' "$url" >>"${SMOKE_CURL_LOG:?}"
if [[ "${SMOKE_SCENARIO:-healthy}" == timeout ]]; then exit 28; fi
case "$url" in
    */api/v1/auth/csrf)
        status=${SMOKE_CSRF_STATUS:-200}
        printf 'HTTP/1.1 %s\n' "$status" >"$headers_file"
        [[ "$status" == 200 ]] && printf '%s\n' 'Set-Cookie: XSRF-TOKEN=redacted; Path=/' >>"$headers_file"
        printf '%s\n' '{"headerName":"X-XSRF-TOKEN"}' >"$body_file"
        ;;
    */api/v1/jobs)
        status=${SMOKE_JOBS_STATUS:-401}
        printf 'HTTP/1.1 %s\nContent-Type: %s\n' "$status" "${SMOKE_JOBS_CONTENT_TYPE:-application/problem+json}" >"$headers_file"
        printf '%s\n' "${SMOKE_JOBS_BODY:-{\"detail\":\"protected\"}}" >"$body_file"
        ;;
    */login)
        status=${SMOKE_LOGIN_STATUS:-200}
        printf 'HTTP/1.1 %s\n' "$status" >"$headers_file"
        printf '%s\n' "${SMOKE_LOGIN_BODY:-<title>ReplicaDB Control Plane</title>}" >"$body_file"
        ;;
    *)
        status=${SMOKE_ROOT_STATUS:-200}
        printf 'HTTP/1.1 %s\n' "$status" >"$headers_file"
        printf '%s\n' "${SMOKE_ROOT_BODY:-<title>ReplicaDB Control Plane</title>}" >"$body_file"
        ;;
esac
printf '%s' "$status"
EOF
chmod 755 "$temp_root/bin/curl"
cat >"$temp_root/bin/gcloud" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${SMOKE_GCLOUD_LOG:?}"
printf '%s\n' 'https://api.example.invalid'
EOF
chmod 755 "$temp_root/bin/gcloud"

export PATH="$temp_root/bin:$PATH"
export SMOKE_CURL_LOG="$temp_root/curl.log" SMOKE_GCLOUD_LOG="$temp_root/gcloud.log"

run_smoke() {
    if [[ "${SMOKE_USE_URL:-true}" == true ]]; then
        bash "$script_dir/phase5-gcp-frontend-smoke.sh" --url https://api.example.invalid "$@"
    else
        bash "$script_dir/phase5-gcp-frontend-smoke.sh" "$@"
    fi
}

run_smoke >/dev/null
[[ "$(wc -l <"$SMOKE_CURL_LOG")" -eq 4 ]]
if [[ -s "$SMOKE_GCLOUD_LOG" ]]; then exit 1; fi

expect_failure() {
    if "$@" >"$temp_root/output" 2>&1; then
        printf 'expected smoke failure but command passed\n' >&2
        exit 1
    fi
}

SMOKE_ROOT_STATUS=404 expect_failure run_smoke
SMOKE_ROOT_STATUS=200 SMOKE_ROOT_BODY='{"password":"should-not-print"}' expect_failure run_smoke
SMOKE_CSRF_STATUS=503 expect_failure run_smoke
SMOKE_JOBS_STATUS=200 expect_failure run_smoke
SMOKE_JOBS_CONTENT_TYPE=application/json expect_failure run_smoke
SMOKE_SCENARIO=timeout expect_failure run_smoke
if rg -q 'should-not-print|password|XSRF-TOKEN=redacted|protected' "$temp_root/output"; then
    printf 'smoke output leaked response content\n' >&2
    exit 1
fi

unset SMOKE_ROOT_STATUS SMOKE_ROOT_BODY SMOKE_CSRF_STATUS SMOKE_JOBS_STATUS SMOKE_JOBS_CONTENT_TYPE SMOKE_SCENARIO
export SMOKE_USE_URL=false
run_smoke --project test-project --region europe-west4 --service replicadb-api >/dev/null
rg -q 'replicadb-api.*test-project.*europe-west4' "$SMOKE_GCLOUD_LOG"

printf 'public frontend smoke tests passed\n'
