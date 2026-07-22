#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/coinpilot-ops-test.XXXXXX")"
cleanup() {
  rm -rf "${TMP_ROOT}"
}
trap cleanup EXIT INT TERM

SHELL_FILES=(
  "${REPO_ROOT}/scripts/mac-studio"
  "${REPO_ROOT}/scripts/mac-studio-daily-scorecard"
  "${REPO_ROOT}/scripts/coinpilot-service.sh"
  "${REPO_ROOT}/scripts/mac-studio-keychain"
  "${REPO_ROOT}/scripts/mac-studio-backup"
  "${REPO_ROOT}/scripts/mac-studio-retention"
  "${REPO_ROOT}/scripts/test-daily-scorecard-ops.sh"
  "${REPO_ROOT}/scripts/test-mac-studio-ops.sh"
)

bash -n "${SHELL_FILES[@]}"
echo "OK bash syntax"
if command -v shellcheck >/dev/null 2>&1; then
  shellcheck "${SHELL_FILES[@]}"
  echo "OK shellcheck"
fi

grep -Fq 'scripts/coinpilot_hourly_notifier.py' \
  "${REPO_ROOT}/scripts/mac-studio"
grep -Fq 'coinpilot-hourly-notifier' \
  "${REPO_ROOT}/scripts/coinpilot-service.sh"
grep -Fq 'COINPILOT_SLACK_SUMMARY_SECONDS' \
  "${REPO_ROOT}/scripts/mac-studio"
grep -Fq 'COINPILOT_SLACK_SUMMARY_GRACE_SECONDS' \
  "${REPO_ROOT}/scripts/mac-studio"
echo "OK hourly Slack summary helper wiring"

grep -Fq 'scripts/coinpilot_bounded_shadow.py' \
  "${REPO_ROOT}/scripts/mac-studio"
grep -Fq 'coinpilot-bounded-shadow' \
  "${REPO_ROOT}/scripts/coinpilot-service.sh"
BOUNDED_HOME="${TMP_ROOT}/bounded-service"
mkdir -p \
  "${BOUNDED_HOME}/bin" \
  "${BOUNDED_HOME}/config" \
  "${BOUNDED_HOME}/state" \
  "${BOUNDED_HOME}/venv/bin"
touch "${BOUNDED_HOME}/config/config.toml"
cat > "${BOUNDED_HOME}/venv/bin/python" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-}" == "-" ]]; then
  printf '%s\n' "${TEST_SHADOW_MODEL_VERSION:-diagnostic-bounded-v1}"
  exit 0
fi
printf '%s\n' "$*" > "${TEST_BOUNDED_ARGS:?}"
SH
cat > "${BOUNDED_HOME}/venv/bin/coinpilot" <<'SH'
#!/usr/bin/env bash
exit 64
SH
cat > "${BOUNDED_HOME}/bin/coinpilot-bounded-shadow" <<'SH'
#!/usr/bin/env bash
exit 64
SH
chmod 755 \
  "${BOUNDED_HOME}/venv/bin/python" \
  "${BOUNDED_HOME}/venv/bin/coinpilot" \
  "${BOUNDED_HOME}/bin/coinpilot-bounded-shadow"
printf '%s\n' \
  'COINPILOT_BOUNDED_SHADOW=1' \
  'COINPILOT_SHADOW_COOLDOWN_SECONDS=3600' \
  'COINPILOT_SHADOW_MAX_ROUND_TRIPS_PER_DAY=24' \
  'COINPILOT_SHADOW_RESERVE_FULL_ORDER_LOSS=1' \
  'COINPILOT_SHADOW_EXECUTION_SPREAD_RECHECK=1' \
  > "${BOUNDED_HOME}/config/runtime.env"
TEST_BOUNDED_ARGS="${TMP_ROOT}/bounded.args" \
  COINPILOT_HOME="${BOUNDED_HOME}" \
  "${REPO_ROOT}/scripts/coinpilot-service.sh" shadow
grep -Fq \
  "${BOUNDED_HOME}/bin/coinpilot-bounded-shadow --config ${BOUNDED_HOME}/config/config.toml" \
  "${TMP_ROOT}/bounded.args"
grep -Fq -- '--max-round-trips-per-day 24' "${TMP_ROOT}/bounded.args"
grep -Fq -- '--execution-spread-recheck 1' "${TMP_ROOT}/bounded.args"
grep -Fq 'COINPILOT_CODE_VERSION' \
  "${REPO_ROOT}/scripts/coinpilot-service.sh"
grep -Fq 'SERVICE_WRAPPER_SHA' \
  "${REPO_ROOT}/scripts/coinpilot-service.sh"
echo "OK bounded diagnostic shadow helper wiring"

printf '%s\n' \
  'COINPILOT_BOUNDED_SHADOW=0' \
  > "${BOUNDED_HOME}/config/runtime.env"
if COINPILOT_HOME="${BOUNDED_HOME}" \
    TEST_BOUNDED_ARGS="${TMP_ROOT}/bounded-disabled.args" \
    "${REPO_ROOT}/scripts/coinpilot-service.sh" shadow \
    > "${TMP_ROOT}/bounded-disabled.log" 2>&1; then
  echo "error: bounded model ran after its helper was disabled" >&2
  exit 1
fi
grep -Fq 'refusing bounded model without bounded shadow helper' \
  "${TMP_ROOT}/bounded-disabled.log"
printf '%s\n' \
  'COINPILOT_BOUNDED_SHADOW=invalid' \
  > "${BOUNDED_HOME}/config/runtime.env"
if COINPILOT_HOME="${BOUNDED_HOME}" \
    TEST_BOUNDED_ARGS="${TMP_ROOT}/bounded-invalid.args" \
    "${REPO_ROOT}/scripts/coinpilot-service.sh" shadow \
    > "${TMP_ROOT}/bounded-invalid.log" 2>&1; then
  echo "error: invalid bounded flag was accepted" >&2
  exit 1
fi
grep -Fq 'invalid COINPILOT_BOUNDED_SHADOW value' \
  "${TMP_ROOT}/bounded-invalid.log"
grep -Fq 'coinpilot-bounded-shadow' "${REPO_ROOT}/scripts/mac-studio"
echo "OK bounded diagnostic profile fails closed"

grep -Fq 'COINPILOT_ENABLE_PAPER' "${REPO_ROOT}/scripts/mac-studio"
grep -Fq 'paper --loop' "${REPO_ROOT}/scripts/coinpilot-service.sh"
grep -Fq 'coinpilot-c2.db' "${REPO_ROOT}/scripts/mac-studio-backup"
grep -Fq 'coinpilot-c2-config' "${REPO_ROOT}/scripts/mac-studio"
echo "OK disabled forward-paper service and backup wiring"

PAPER_SERVICE_HOME="${TMP_ROOT}/paper-service"
mkdir -p \
  "${PAPER_SERVICE_HOME}/config" \
  "${PAPER_SERVICE_HOME}/venv/bin"
touch \
  "${PAPER_SERVICE_HOME}/config/config.toml" \
  "${PAPER_SERVICE_HOME}/config/paper.toml" \
  "${PAPER_SERVICE_HOME}/config/paper.manifest.json"
mkdir -p "${PAPER_SERVICE_HOME}/bin"
cat > "${PAPER_SERVICE_HOME}/venv/bin/python" <<'SH'
#!/usr/bin/env bash
case "${1:-}" in
  -)
    printf '%s\n' "${COINPILOT_HOME:?}/venv/lib/site-packages/coinpilot"
    ;;
  */coinpilot-c2-config)
    printf '%s\n' "$*" > "${TEST_GUARD_ARGS:?}"
    exit "${TEST_GUARD_EXIT:-0}"
    ;;
  *)
    exit 64
    ;;
esac
SH
cat > "${PAPER_SERVICE_HOME}/venv/bin/coinpilot" <<'SH'
#!/usr/bin/env bash
printf '%s|%s|%s\n' \
  "${COINPILOT_MODE:-}" "${COINPILOT_LIVE_TRADING:-}" "$*" \
  > "${TEST_PAPER_ARGS:?}"
SH
cat > "${PAPER_SERVICE_HOME}/bin/coinpilot-c2-config" <<'SH'
#!/usr/bin/env bash
exit 64
SH
chmod 755 \
  "${PAPER_SERVICE_HOME}/venv/bin/python" \
  "${PAPER_SERVICE_HOME}/venv/bin/coinpilot" \
  "${PAPER_SERVICE_HOME}/bin/coinpilot-c2-config"
printf '%s\n' \
  'COINPILOT_ENABLE_PAPER=1' \
  'COINPILOT_WEB_HOST=127.0.0.1' \
  'COINPILOT_WEB_PORT=8765' \
  > "${PAPER_SERVICE_HOME}/config/runtime.env"
TEST_PAPER_ARGS="${TMP_ROOT}/paper-service.args" \
  TEST_GUARD_ARGS="${TMP_ROOT}/paper-guard.args" \
  COINPILOT_HOME="${PAPER_SERVICE_HOME}" \
  "${REPO_ROOT}/scripts/coinpilot-service.sh" paper
grep -Fq \
  "paper|0|--config ${PAPER_SERVICE_HOME}/config/paper.toml paper --loop" \
  "${TMP_ROOT}/paper-service.args"
grep -Fq \
  "verify-installed --config ${PAPER_SERVICE_HOME}/config/paper.toml" \
  "${TMP_ROOT}/paper-guard.args"
grep -Fq -- \
  "--manifest ${PAPER_SERVICE_HOME}/config/paper.manifest.json" \
  "${TMP_ROOT}/paper-guard.args"
rm -f "${TMP_ROOT}/paper-service.args"
if TEST_PAPER_ARGS="${TMP_ROOT}/paper-service.args" \
    TEST_GUARD_ARGS="${TMP_ROOT}/paper-guard-drift.args" \
    TEST_GUARD_EXIT=2 \
    COINPILOT_HOME="${PAPER_SERVICE_HOME}" \
    "${REPO_ROOT}/scripts/coinpilot-service.sh" paper \
    > "${TMP_ROOT}/paper-guard-drift.log" 2>&1; then
  echo "error: paper wrapper ran after its immutable guard rejected drift" >&2
  exit 1
fi
[[ ! -e "${TMP_ROOT}/paper-service.args" ]]
grep -Fq 'verify-installed' "${TMP_ROOT}/paper-guard-drift.args"
echo "OK every direct paper start passes the immutable runtime guard"
printf '%s\n' 'COINPILOT_ENABLE_PAPER=0' \
  > "${PAPER_SERVICE_HOME}/config/runtime.env"
if COINPILOT_HOME="${PAPER_SERVICE_HOME}" \
    "${REPO_ROOT}/scripts/coinpilot-service.sh" paper \
    > "${TMP_ROOT}/paper-disabled.log" 2>&1; then
  echo "error: disabled paper service was allowed to run" >&2
  exit 1
fi
grep -Fq 'paper service is disabled' "${TMP_ROOT}/paper-disabled.log"
echo "OK paper wrapper is simulation-only and fails closed when disabled"

FAKE_LAUNCHCTL_BIN="${TMP_ROOT}/fake-launchctl-bin"
FAKE_STATUS_HOME="${TMP_ROOT}/paper-loaded-not-running"
mkdir -p "${FAKE_LAUNCHCTL_BIN}" "${FAKE_STATUS_HOME}/config"
cat > "${FAKE_LAUNCHCTL_BIN}/launchctl" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-}" == "print" ]]; then
  case "${2:-}" in
    gui/[0-9]*/*|user/[0-9]*/*)
      printf '%s\n' 'state = waiting'
      ;;
  esac
  exit 0
fi
exit 1
SH
chmod 755 "${FAKE_LAUNCHCTL_BIN}/launchctl"
printf '%s\n' \
  'COINPILOT_ENABLE_PAPER=1' \
  'COINPILOT_ENABLE_WEB=0' \
  > "${FAKE_STATUS_HOME}/config/runtime.env"
PATH="${FAKE_LAUNCHCTL_BIN}:/usr/bin:/bin" \
  HOME="${TMP_ROOT}/paper-status-home" \
  "${REPO_ROOT}/scripts/mac-studio" status \
  --home "${FAKE_STATUS_HOME}" > "${TMP_ROOT}/paper-not-running.log"
grep -Fq 'paper                loaded-not-running' \
  "${TMP_ROOT}/paper-not-running.log"
grep -Fq 'paper account        unavailable' \
  "${TMP_ROOT}/paper-not-running.log"
if grep -Fq 'paper account        ready' "${TMP_ROOT}/paper-not-running.log"; then
  echo "error: non-running launchd paper job was reported ready" >&2
  exit 1
fi
echo "OK loaded paper job without a running PID is never ready"

printf '%s\n' \
  'COINPILOT_ENABLE_SHADOW=0' \
  'COINPILOT_ENABLE_PAPER=0' \
  'COINPILOT_ENABLE_NOTIFIER=0' \
  'COINPILOT_ENABLE_WEB=0' \
  'COINPILOT_ENABLE_WATCHDOG=0' \
  > "${FAKE_STATUS_HOME}/config/runtime.env"
PATH="${FAKE_LAUNCHCTL_BIN}:/usr/bin:/bin" \
  HOME="${TMP_ROOT}/paper-status-home" \
  "${REPO_ROOT}/scripts/mac-studio" status \
  --home "${FAKE_STATUS_HOME}" > "${TMP_ROOT}/disabled-loaded-status.log"
grep -Fq 'paper                loaded-config-disabled' \
  "${TMP_ROOT}/disabled-loaded-status.log"
PATH="${FAKE_LAUNCHCTL_BIN}:/usr/bin:/bin" \
  HOME="${TMP_ROOT}/paper-status-home" \
  "${REPO_ROOT}/scripts/mac-studio" doctor \
  --home "${FAKE_STATUS_HOME}" > "${TMP_ROOT}/disabled-loaded-doctor.log" 2>&1 || true
grep -Fq 'FAIL  launchd loaded despite config-disabled: paper' \
  "${TMP_ROOT}/disabled-loaded-doctor.log"
echo "OK configured-disabled loaded launchd drift is explicit"

RESTART_RACE_BIN="${TMP_ROOT}/restart-race-bin"
RESTART_RACE_HOME="${TMP_ROOT}/restart-race-home"
RESTART_RACE_APP="${TMP_ROOT}/restart-race-app"
RESTART_RACE_LOG="${TMP_ROOT}/restart-race.log"
RESTART_RACE_STATE="${TMP_ROOT}/restart-race.state"
RESTART_RACE_EARLY="${TMP_ROOT}/restart-race.early"
RESTART_OVERRIDE_STATE="${TMP_ROOT}/restart-race.override"
mkdir -p \
  "${RESTART_RACE_BIN}" \
  "${RESTART_RACE_APP}/config" \
  "${RESTART_RACE_HOME}/Library/LaunchAgents"
touch "${RESTART_RACE_HOME}/Library/LaunchAgents/dev.coinpilot.shadow.plist"
printf '%s\n' loaded > "${RESTART_RACE_STATE}"
cat > "${RESTART_RACE_BIN}/uname" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-}" == "-s" ]]; then
  printf '%s\n' Darwin
  exit 0
fi
exit 64
SH
cat > "${RESTART_RACE_BIN}/launchctl" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "${TEST_LAUNCHCTL_LOG:?}"
case "${1:-}" in
  print-disabled)
    if [[ "${TEST_PRINT_DISABLED_FAIL:-0}" -eq 1 ]]; then
      exit 72
    fi
    label="${TEST_OVERRIDE_LABEL:-dev.coinpilot.shadow}"
    printf '%s\n' 'disabled services = {'
    printf '\t"%s-extra" => disabled\n' "${label}"
    if [[ -e "${TEST_LAUNCHCTL_OVERRIDE_STATE:?}" ]]; then
      printf '\t"%s" => %s\n' \
        "${label}" "$(cat "${TEST_LAUNCHCTL_OVERRIDE_STATE}")"
      if [[ "${TEST_OVERRIDE_DUPLICATE:-0}" -eq 1 ]]; then
        printf '\t%s => enabled;\r\n' "${label}"
      fi
    fi
    printf '%s\n' '}'
    ;;
  print)
    case "${2:-}" in
      */dev.coinpilot.shadow)
        state="$(cat "${TEST_LAUNCHCTL_STATE:?}" 2>/dev/null || true)"
        case "${state}" in
          loaded) exit 0 ;;
          stopping:*)
            remaining="${state#stopping:}"
            if [[ "${remaining}" -gt 1 ]]; then
              printf 'stopping:%s\n' "$((remaining - 1))" \
                > "${TEST_LAUNCHCTL_STATE}"
              exit 0
            fi
            rm -f "${TEST_LAUNCHCTL_STATE}"
            exit 1
            ;;
          *) exit 1 ;;
        esac
        ;;
      */dev.coinpilot.btc.shadow) exit 1 ;;
      *) exit 0 ;;
    esac
    ;;
  bootout)
    if [[ "${TEST_BOOTOUT_FAIL:-0}" -eq 1 ]]; then
      printf '%s\n' 'injected bootout failure' >&2
      exit 71
    fi
    printf '%s\n' 'stopping:2' > "${TEST_LAUNCHCTL_STATE:?}"
    exit 0
    ;;
  bootstrap)
    if [[ -e "${TEST_LAUNCHCTL_STATE:?}" ]]; then
      touch "${TEST_LAUNCHCTL_EARLY:?}"
      exit 0
    fi
    printf '%s\n' loaded > "${TEST_LAUNCHCTL_STATE}"
    exit 0
    ;;
  enable)
    printf '%s\n' enabled > "${TEST_LAUNCHCTL_OVERRIDE_STATE:?}"
    ;;
  disable)
    if [[ "${TEST_DISABLE_FAIL:-0}" -eq 1 ]]; then
      printf '%s\n' 'injected disable failure' >&2
      exit 70
    fi
    printf '%s\n' disabled > "${TEST_LAUNCHCTL_OVERRIDE_STATE:?}"
    ;;
  *) exit 64 ;;
esac
SH
chmod 755 \
  "${RESTART_RACE_BIN}/uname" \
  "${RESTART_RACE_BIN}/launchctl"
PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
  HOME="${RESTART_RACE_HOME}" \
  TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
  TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
  TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
  "${REPO_ROOT}/scripts/mac-studio" restart shadow \
  --home "${RESTART_RACE_APP}" --apply
if [[ "$(grep -c '^bootout ' "${RESTART_RACE_LOG}")" -ne 1 ]]; then
  echo "error: restart issued more than one launchd bootout" >&2
  exit 1
fi
grep -Fq 'bootstrap ' "${RESTART_RACE_LOG}"
grep -Fq "enable gui/$(id -u)/dev.coinpilot.shadow" \
  "${RESTART_RACE_LOG}"
[[ "$(cat "${RESTART_OVERRIDE_STATE}")" == "enabled" ]]
if [[ -e "${RESTART_RACE_EARLY}" ]]; then
  echo "error: restart bootstrapped while the old job was still unloading" >&2
  exit 1
fi
if [[ "$(cat "${RESTART_RACE_STATE}")" != "loaded" ]]; then
  echo "error: restart bootstrapped before the old job fully unloaded" >&2
  exit 1
fi
expected_restart_probe="print gui/$(id -u)/dev.coinpilot.shadow"
if [[ "$(tail -n 1 "${RESTART_RACE_LOG}")" != "${expected_restart_probe}" ]]; then
  echo "error: restart did not verify the bootstrapped launchd job" >&2
  exit 1
fi
echo "OK restart uses one bootout/bootstrap cycle"

rm -f "${RESTART_RACE_LOG}"
printf '%s\n' loaded > "${RESTART_RACE_STATE}"
PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
  HOME="${RESTART_RACE_HOME}" \
  TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
  TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
  TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
  "${REPO_ROOT}/scripts/mac-studio" stop shadow \
  --home "${RESTART_RACE_APP}" --apply
grep -Fq "disable gui/$(id -u)/dev.coinpilot.shadow" \
  "${RESTART_RACE_LOG}"
grep -Fq "bootout gui/$(id -u)/dev.coinpilot.shadow" \
  "${RESTART_RACE_LOG}"
[[ "$(cat "${RESTART_OVERRIDE_STATE}")" == "disabled" ]]
if [[ -e "${RESTART_RACE_STATE}" ]]; then
  echo "error: stop left the launchd job loaded" >&2
  exit 1
fi

rm -f "${RESTART_RACE_LOG}"
PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
  HOME="${RESTART_RACE_HOME}" \
  TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
  TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
  TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
  "${REPO_ROOT}/scripts/mac-studio" start shadow \
  --home "${RESTART_RACE_APP}" --apply
grep -Fq "enable gui/$(id -u)/dev.coinpilot.shadow" \
  "${RESTART_RACE_LOG}"
grep -Fq 'bootstrap ' "${RESTART_RACE_LOG}"
[[ "$(cat "${RESTART_OVERRIDE_STATE}")" == "enabled" ]]
if grep -Fq "disable gui/$(id -u)/dev.coinpilot.shadow" \
    "${RESTART_RACE_LOG}"; then
  echo "error: start left a persistent launchd disable override" >&2
  exit 1
fi
echo "OK stop disables persistently and start enables before bootstrap"

rm -f "${RESTART_RACE_LOG}"
printf '%s\n' loaded > "${RESTART_RACE_STATE}"
if PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
    HOME="${RESTART_RACE_HOME}" \
    TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
    TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
    TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
    TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
    TEST_DISABLE_FAIL=1 \
    "${REPO_ROOT}/scripts/mac-studio" stop shadow \
    --home "${RESTART_RACE_APP}" --apply \
    > "${TMP_ROOT}/disable-failure-stop.log" 2>&1; then
  echo "error: stop ignored a persistent disable failure" >&2
  exit 1
fi
grep -Fq "bootout gui/$(id -u)/dev.coinpilot.shadow" \
  "${RESTART_RACE_LOG}"
[[ ! -e "${RESTART_RACE_STATE}" ]]
grep -Fq 'launchctl disable failed for shadow: injected disable failure' \
  "${TMP_ROOT}/disable-failure-stop.log"
echo "OK disable failure still attempts and completes bootout"

rm -f "${RESTART_RACE_LOG}"
printf '%s\n' loaded > "${RESTART_RACE_STATE}"
if PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
    HOME="${RESTART_RACE_HOME}" \
    TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
    TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
    TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
    TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
    TEST_DISABLE_FAIL=1 \
    TEST_BOOTOUT_FAIL=1 \
    "${REPO_ROOT}/scripts/mac-studio" stop shadow \
    --home "${RESTART_RACE_APP}" --apply \
    > "${TMP_ROOT}/combined-stop-failure.log" 2>&1; then
  echo "error: stop ignored combined disable and bootout failures" >&2
  exit 1
fi
grep -Fq 'launchctl disable failed for shadow: injected disable failure' \
  "${TMP_ROOT}/combined-stop-failure.log"
grep -Fq 'launchctl bootout failed for shadow: injected bootout failure' \
  "${TMP_ROOT}/combined-stop-failure.log"
rm -f "${RESTART_RACE_STATE}"
echo "OK stop aggregates disable and bootout failures"

printf '%s\n' \
  'COINPILOT_ENABLE_SHADOW=0' \
  'COINPILOT_ENABLE_PAPER=0' \
  'COINPILOT_ENABLE_NOTIFIER=0' \
  'COINPILOT_ENABLE_WEB=0' \
  'COINPILOT_ENABLE_WATCHDOG=0' \
  > "${RESTART_RACE_APP}/config/runtime.env"
printf '%s\n' disabled > "${RESTART_OVERRIDE_STATE}"
PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
  HOME="${RESTART_RACE_HOME}" \
  TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
  TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
  TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
  "${REPO_ROOT}/scripts/mac-studio" status shadow \
  --home "${RESTART_RACE_APP}" > "${TMP_ROOT}/override-disabled-status.log"
grep -Fq 'shadow               not-loaded' \
  "${TMP_ROOT}/override-disabled-status.log"
PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
  HOME="${RESTART_RACE_HOME}" \
  TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
  TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
  TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
  "${REPO_ROOT}/scripts/mac-studio" doctor \
  --home "${RESTART_RACE_APP}" > "${TMP_ROOT}/override-disabled-doctor.log" 2>&1 || true
grep -Fq \
  'OK    launchd explicitly disabled: shadow (reason=config-disabled)' \
  "${TMP_ROOT}/override-disabled-doctor.log"

printf '%s\n' enabled > "${RESTART_OVERRIDE_STATE}"
PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
  HOME="${RESTART_RACE_HOME}" \
  TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
  TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
  TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
  "${REPO_ROOT}/scripts/mac-studio" status \
  --home "${RESTART_RACE_APP}" > "${TMP_ROOT}/override-enabled-status.log"
grep -Fq 'shadow               not-loaded-config-disabled-override-enabled' \
  "${TMP_ROOT}/override-enabled-status.log"
if PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
    HOME="${RESTART_RACE_HOME}" \
    TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
    TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
    TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
    TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
    "${REPO_ROOT}/scripts/mac-studio" doctor \
    --home "${RESTART_RACE_APP}" \
    > "${TMP_ROOT}/override-enabled-doctor.log" 2>&1; then
  echo "error: doctor accepted enabled override for config-disabled shadow" >&2
  exit 1
fi
grep -Fq \
  'FAIL  launchd disable override drift: shadow (reason=config-disabled, override=enabled)' \
  "${TMP_ROOT}/override-enabled-doctor.log"

for supported_disabled_value in true 1; do
  printf '%s\n' "${supported_disabled_value}" > "${RESTART_OVERRIDE_STATE}"
  PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
    HOME="${RESTART_RACE_HOME}" \
    TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
    TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
    TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
    TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
    "${REPO_ROOT}/scripts/mac-studio" status \
    --home "${RESTART_RACE_APP}" \
    > "${TMP_ROOT}/override-${supported_disabled_value}-status.log"
  grep -Fq 'shadow               not-loaded' \
    "${TMP_ROOT}/override-${supported_disabled_value}-status.log"
done

for drift_case in false 0 malformed; do
  printf '%s\n' "${drift_case}" > "${RESTART_OVERRIDE_STATE}"
  PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
    HOME="${RESTART_RACE_HOME}" \
    TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
    TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
    TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
    TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
    "${REPO_ROOT}/scripts/mac-studio" status \
    --home "${RESTART_RACE_APP}" \
    > "${TMP_ROOT}/override-${drift_case}-status.log"
done
grep -Fq 'shadow               not-loaded-config-disabled-override-enabled' \
  "${TMP_ROOT}/override-false-status.log"
grep -Fq 'shadow               not-loaded-config-disabled-override-enabled' \
  "${TMP_ROOT}/override-0-status.log"
grep -Fq 'shadow               not-loaded-config-disabled-override-unknown' \
  "${TMP_ROOT}/override-malformed-status.log"

printf '%s\n' disabled > "${RESTART_OVERRIDE_STATE}"
PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
  HOME="${RESTART_RACE_HOME}" \
  TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
  TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
  TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
  TEST_OVERRIDE_DUPLICATE=1 \
  "${REPO_ROOT}/scripts/mac-studio" status \
  --home "${RESTART_RACE_APP}" > "${TMP_ROOT}/override-duplicate-status.log"
grep -Fq 'shadow               not-loaded-config-disabled-override-unknown' \
  "${TMP_ROOT}/override-duplicate-status.log"

rm -f "${RESTART_OVERRIDE_STATE}"
PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
  HOME="${RESTART_RACE_HOME}" \
  TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
  TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
  TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
  "${REPO_ROOT}/scripts/mac-studio" status \
  --home "${RESTART_RACE_APP}" > "${TMP_ROOT}/override-unset-status.log"
grep -Fq 'shadow               not-loaded-config-disabled-override-unset' \
  "${TMP_ROOT}/override-unset-status.log"
if PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
    HOME="${RESTART_RACE_HOME}" \
    TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
    TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
    TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
    TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
    "${REPO_ROOT}/scripts/mac-studio" doctor \
    --home "${RESTART_RACE_APP}" \
    > "${TMP_ROOT}/override-unset-doctor.log" 2>&1; then
  echo "error: doctor accepted unset override for config-disabled shadow" >&2
  exit 1
fi
grep -Fq \
  'FAIL  launchd disable override drift: shadow (reason=config-disabled, override=unset)' \
  "${TMP_ROOT}/override-unset-doctor.log"
PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
  HOME="${RESTART_RACE_HOME}" \
  TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
  TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
  TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
  TEST_PRINT_DISABLED_FAIL=1 \
  "${REPO_ROOT}/scripts/mac-studio" status \
  --home "${RESTART_RACE_APP}" > "${TMP_ROOT}/override-query-failure-status.log"
grep -Fq 'shadow               not-loaded-config-disabled-override-unknown' \
  "${TMP_ROOT}/override-query-failure-status.log"

printf '%s\n' disabled > "${RESTART_OVERRIDE_STATE}"
PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
  HOME="${RESTART_RACE_HOME}" \
  TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
  TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
  TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
  TEST_OVERRIDE_LABEL='dev.coinpilot.btc.shadow' \
  "${REPO_ROOT}/scripts/mac-studio" status --instance btc \
  --home "${RESTART_RACE_APP}" > "${TMP_ROOT}/named-override-status.log"
grep -Fq 'shadow               not-loaded' "${TMP_ROOT}/named-override-status.log"
rm -f "${RESTART_OVERRIDE_STATE}"
PATH="${RESTART_RACE_BIN}:/usr/bin:/bin" \
  HOME="${RESTART_RACE_HOME}" \
  TEST_LAUNCHCTL_LOG="${RESTART_RACE_LOG}" \
  TEST_LAUNCHCTL_STATE="${RESTART_RACE_STATE}" \
  TEST_LAUNCHCTL_EARLY="${RESTART_RACE_EARLY}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${RESTART_OVERRIDE_STATE}" \
  TEST_OVERRIDE_LABEL='dev.coinpilot.btc.shadow' \
  "${REPO_ROOT}/scripts/mac-studio" status --instance btc \
  --home "${RESTART_RACE_APP}" > "${TMP_ROOT}/named-adjacent-only-status.log"
grep -Fq 'shadow               not-loaded-config-disabled-override-unset' \
  "${TMP_ROOT}/named-adjacent-only-status.log"
echo "OK print-disabled parser handles actual values and exact instance labels"

NOTIFIER_LAUNCHCTL_BIN="${TMP_ROOT}/notifier-launchctl-bin"
NOTIFIER_LAUNCHCTL_APP="${TMP_ROOT}/notifier-launchctl-app"
NOTIFIER_LAUNCHCTL_LOG="${TMP_ROOT}/notifier-launchctl.log"
NOTIFIER_LAUNCHCTL_STATE="${TMP_ROOT}/notifier-launchctl.state"
NOTIFIER_OVERRIDE_STATE="${TMP_ROOT}/notifier-launchctl.override"
NOTIFIER_SECURITY_LOG="${TMP_ROOT}/notifier-security.log"
mkdir -p "${NOTIFIER_LAUNCHCTL_BIN}" "${NOTIFIER_LAUNCHCTL_APP}/config"
cat > "${NOTIFIER_LAUNCHCTL_BIN}/uname" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-}" == "-s" ]]; then
  printf '%s\n' Darwin
  exit 0
fi
exit 64
SH
cat > "${NOTIFIER_LAUNCHCTL_BIN}/launchctl" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "${TEST_LAUNCHCTL_LOG:?}"
case "${1:-}" in
  print-disabled)
    printf '%s\n' 'disabled services = {'
    if [[ -e "${TEST_LAUNCHCTL_OVERRIDE_STATE:?}" ]]; then
      printf '\t"dev.coinpilot.notifier" => %s\n' \
        "$(cat "${TEST_LAUNCHCTL_OVERRIDE_STATE}")"
    fi
    printf '%s\n' '}'
    ;;
  print)
    case "${2:-}" in
      */dev.coinpilot.notifier)
        [[ -e "${TEST_LAUNCHCTL_STATE:?}" ]]
        ;;
      gui/[0-9]*|user/[0-9]*) exit 0 ;;
      *) exit 1 ;;
    esac
    ;;
  disable)
    if [[ "${TEST_DISABLE_FAIL:-0}" -eq 1 ]]; then
      printf '%s\n' 'injected notifier disable failure' >&2
      exit 70
    fi
    printf '%s\n' disabled > "${TEST_LAUNCHCTL_OVERRIDE_STATE:?}"
    ;;
  enable)
    printf '%s\n' enabled > "${TEST_LAUNCHCTL_OVERRIDE_STATE:?}"
    ;;
  bootout)
    rm -f "${TEST_LAUNCHCTL_STATE:?}"
    exit 0
    ;;
  bootstrap) exit 64 ;;
  *) exit 64 ;;
esac
SH
cat > "${NOTIFIER_LAUNCHCTL_BIN}/security" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "${TEST_SECURITY_LOG:?}"
case "${1:-}" in
  find-generic-password)
    [[ "${TEST_SECURITY_PRESENT:-0}" -eq 1 ]]
    ;;
  delete-generic-password) exit 0 ;;
  *) exit 64 ;;
esac
SH
chmod 755 \
  "${NOTIFIER_LAUNCHCTL_BIN}/uname" \
  "${NOTIFIER_LAUNCHCTL_BIN}/launchctl" \
  "${NOTIFIER_LAUNCHCTL_BIN}/security"
printf '%s\n' 'COINPILOT_ENABLE_NOTIFIER=1' \
  > "${NOTIFIER_LAUNCHCTL_APP}/config/runtime.env"
touch "${NOTIFIER_LAUNCHCTL_STATE}"
PATH="${NOTIFIER_LAUNCHCTL_BIN}:/usr/bin:/bin" \
  HOME="${TMP_ROOT}/notifier-launchctl-home" \
  TEST_LAUNCHCTL_LOG="${NOTIFIER_LAUNCHCTL_LOG}" \
  TEST_LAUNCHCTL_STATE="${NOTIFIER_LAUNCHCTL_STATE}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${NOTIFIER_OVERRIDE_STATE}" \
  TEST_SECURITY_LOG="${NOTIFIER_SECURITY_LOG}" \
  TEST_SECURITY_PRESENT=0 \
  "${REPO_ROOT}/scripts/mac-studio" start notifier \
  --home "${NOTIFIER_LAUNCHCTL_APP}" --apply \
  > "${TMP_ROOT}/notifier-missing-keychain.log" 2>&1
grep -Fq "disable gui/$(id -u)/dev.coinpilot.notifier" \
  "${NOTIFIER_LAUNCHCTL_LOG}"
grep -Fq "bootout gui/$(id -u)/dev.coinpilot.notifier" \
  "${NOTIFIER_LAUNCHCTL_LOG}"
[[ "$(cat "${NOTIFIER_OVERRIDE_STATE}")" == "disabled" ]]
if grep -Eq '^(enable|bootstrap) ' "${NOTIFIER_LAUNCHCTL_LOG}"; then
  echo "error: notifier without a Keychain item was enabled or bootstrapped" >&2
  exit 1
fi
echo "OK missing notifier Keychain item disables and unloads launchd"

PATH="${NOTIFIER_LAUNCHCTL_BIN}:/usr/bin:/bin" \
  HOME="${TMP_ROOT}/notifier-launchctl-home" \
  TEST_LAUNCHCTL_LOG="${NOTIFIER_LAUNCHCTL_LOG}" \
  TEST_LAUNCHCTL_STATE="${NOTIFIER_LAUNCHCTL_STATE}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${NOTIFIER_OVERRIDE_STATE}" \
  TEST_SECURITY_LOG="${NOTIFIER_SECURITY_LOG}" \
  TEST_SECURITY_PRESENT=0 \
  "${REPO_ROOT}/scripts/mac-studio" status \
  --home "${NOTIFIER_LAUNCHCTL_APP}" \
  > "${TMP_ROOT}/notifier-disabled-override-status.log"
grep -Fq 'notifier             not-loaded' \
  "${TMP_ROOT}/notifier-disabled-override-status.log"
printf '%s\n' enabled > "${NOTIFIER_OVERRIDE_STATE}"
PATH="${NOTIFIER_LAUNCHCTL_BIN}:/usr/bin:/bin" \
  HOME="${TMP_ROOT}/notifier-launchctl-home" \
  TEST_LAUNCHCTL_LOG="${NOTIFIER_LAUNCHCTL_LOG}" \
  TEST_LAUNCHCTL_STATE="${NOTIFIER_LAUNCHCTL_STATE}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${NOTIFIER_OVERRIDE_STATE}" \
  TEST_SECURITY_LOG="${NOTIFIER_SECURITY_LOG}" \
  TEST_SECURITY_PRESENT=0 \
  "${REPO_ROOT}/scripts/mac-studio" status \
  --home "${NOTIFIER_LAUNCHCTL_APP}" \
  > "${TMP_ROOT}/notifier-enabled-override-status.log"
grep -Fq 'notifier             not-loaded-missing-keychain-override-enabled' \
  "${TMP_ROOT}/notifier-enabled-override-status.log"
if PATH="${NOTIFIER_LAUNCHCTL_BIN}:/usr/bin:/bin" \
    HOME="${TMP_ROOT}/notifier-launchctl-home" \
    TEST_LAUNCHCTL_LOG="${NOTIFIER_LAUNCHCTL_LOG}" \
    TEST_LAUNCHCTL_STATE="${NOTIFIER_LAUNCHCTL_STATE}" \
    TEST_LAUNCHCTL_OVERRIDE_STATE="${NOTIFIER_OVERRIDE_STATE}" \
    TEST_SECURITY_LOG="${NOTIFIER_SECURITY_LOG}" \
    TEST_SECURITY_PRESENT=0 \
    "${REPO_ROOT}/scripts/mac-studio" doctor \
    --home "${NOTIFIER_LAUNCHCTL_APP}" \
    > "${TMP_ROOT}/notifier-enabled-override-doctor.log" 2>&1; then
  echo "error: doctor accepted enabled notifier override without Keychain" >&2
  exit 1
fi
grep -Fq \
  'FAIL  launchd disable override drift: notifier (reason=missing-keychain, override=enabled)' \
  "${TMP_ROOT}/notifier-enabled-override-doctor.log"
echo "OK missing-Keychain notifier requires an explicit disable override"

rm -f "${NOTIFIER_LAUNCHCTL_LOG}" "${NOTIFIER_SECURITY_LOG}"
printf '%s\n' 'COINPILOT_ENABLE_NOTIFIER=0' \
  > "${NOTIFIER_LAUNCHCTL_APP}/config/runtime.env"
touch "${NOTIFIER_LAUNCHCTL_STATE}"
PATH="${NOTIFIER_LAUNCHCTL_BIN}:/usr/bin:/bin" \
  HOME="${TMP_ROOT}/notifier-launchctl-home" \
  TEST_LAUNCHCTL_LOG="${NOTIFIER_LAUNCHCTL_LOG}" \
  TEST_LAUNCHCTL_STATE="${NOTIFIER_LAUNCHCTL_STATE}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${NOTIFIER_OVERRIDE_STATE}" \
  TEST_SECURITY_LOG="${NOTIFIER_SECURITY_LOG}" \
  TEST_SECURITY_PRESENT=1 \
  "${REPO_ROOT}/scripts/mac-studio" start notifier \
  --home "${NOTIFIER_LAUNCHCTL_APP}" --apply
grep -Fq "disable gui/$(id -u)/dev.coinpilot.notifier" \
  "${NOTIFIER_LAUNCHCTL_LOG}"
if grep -Eq '^(enable|bootstrap) ' "${NOTIFIER_LAUNCHCTL_LOG}"; then
  echo "error: configured-disabled notifier was enabled or bootstrapped" >&2
  exit 1
fi
if grep -Fq 'bootstrap_service notifier' "${REPO_ROOT}/scripts/mac-studio"; then
  echo "error: Slack set bypasses configured notifier state" >&2
  exit 1
fi
grep -Fq 'start_one notifier' "${REPO_ROOT}/scripts/mac-studio"
echo "OK Slack set reconciliation honors configured-disabled notifier state"

rm -f "${NOTIFIER_LAUNCHCTL_LOG}" "${NOTIFIER_SECURITY_LOG}"
touch "${NOTIFIER_LAUNCHCTL_STATE}"
PATH="${NOTIFIER_LAUNCHCTL_BIN}:/usr/bin:/bin" \
  HOME="${TMP_ROOT}/notifier-launchctl-home" \
  TEST_LAUNCHCTL_LOG="${NOTIFIER_LAUNCHCTL_LOG}" \
  TEST_LAUNCHCTL_STATE="${NOTIFIER_LAUNCHCTL_STATE}" \
  TEST_LAUNCHCTL_OVERRIDE_STATE="${NOTIFIER_OVERRIDE_STATE}" \
  TEST_SECURITY_LOG="${NOTIFIER_SECURITY_LOG}" \
  TEST_SECURITY_PRESENT=1 \
  "${REPO_ROOT}/scripts/mac-studio" slack delete \
  --home "${NOTIFIER_LAUNCHCTL_APP}" --apply
grep -Fq 'delete-generic-password' "${NOTIFIER_SECURITY_LOG}"
grep -Fq "disable gui/$(id -u)/dev.coinpilot.notifier" \
  "${NOTIFIER_LAUNCHCTL_LOG}"
grep -Fq "bootout gui/$(id -u)/dev.coinpilot.notifier" \
  "${NOTIFIER_LAUNCHCTL_LOG}"
echo "OK Slack delete persistently disables and unloads notifier"

rm -f "${NOTIFIER_LAUNCHCTL_LOG}" "${NOTIFIER_SECURITY_LOG}"
touch "${NOTIFIER_LAUNCHCTL_STATE}"
printf '%s\n' enabled > "${NOTIFIER_OVERRIDE_STATE}"
if PATH="${NOTIFIER_LAUNCHCTL_BIN}:/usr/bin:/bin" \
    HOME="${TMP_ROOT}/notifier-launchctl-home" \
    TEST_LAUNCHCTL_LOG="${NOTIFIER_LAUNCHCTL_LOG}" \
    TEST_LAUNCHCTL_STATE="${NOTIFIER_LAUNCHCTL_STATE}" \
    TEST_LAUNCHCTL_OVERRIDE_STATE="${NOTIFIER_OVERRIDE_STATE}" \
    TEST_SECURITY_LOG="${NOTIFIER_SECURITY_LOG}" \
    TEST_SECURITY_PRESENT=1 \
    TEST_DISABLE_FAIL=1 \
    "${REPO_ROOT}/scripts/mac-studio" slack delete \
    --home "${NOTIFIER_LAUNCHCTL_APP}" --apply \
    > "${TMP_ROOT}/slack-delete-disable-failure.log" 2>&1; then
  echo "error: Slack delete ignored notifier disable failure" >&2
  exit 1
fi
grep -Fq 'delete-generic-password' "${NOTIFIER_SECURITY_LOG}"
grep -Fq "bootout gui/$(id -u)/dev.coinpilot.notifier" \
  "${NOTIFIER_LAUNCHCTL_LOG}"
[[ ! -e "${NOTIFIER_LAUNCHCTL_STATE}" ]]
grep -Fq 'launchctl disable failed for notifier: injected notifier disable failure' \
  "${TMP_ROOT}/slack-delete-disable-failure.log"
echo "OK Slack delete bootouts notifier even when disable fails"

RECONCILE_BIN="${TMP_ROOT}/reconcile-bin"
RECONCILE_HOME="${TMP_ROOT}/reconcile-home"
RECONCILE_APP="${TMP_ROOT}/reconcile-app"
RECONCILE_LOG="${TMP_ROOT}/reconcile-launchctl.log"
RECONCILE_STATE_DIR="${TMP_ROOT}/reconcile-state"
mkdir -p \
  "${RECONCILE_BIN}" \
  "${RECONCILE_HOME}/Library/LaunchAgents" \
  "${RECONCILE_APP}/config" \
  "${RECONCILE_APP}/venv/bin" \
  "${RECONCILE_STATE_DIR}"
touch "${RECONCILE_HOME}/Library/LaunchAgents/dev.coinpilot.shadow.plist"
for stale_label in \
  dev.coinpilot.paper \
  dev.coinpilot.notifier \
  dev.coinpilot.web \
  dev.coinpilot.external-watchdog; do
  touch "${RECONCILE_STATE_DIR}/${stale_label}"
done
printf '%s\n' \
  'COINPILOT_ENABLE_SHADOW=1' \
  'COINPILOT_ENABLE_PAPER=0' \
  'COINPILOT_ENABLE_NOTIFIER=0' \
  'COINPILOT_ENABLE_WEB=0' \
  'COINPILOT_ENABLE_WATCHDOG=0' \
  > "${RECONCILE_APP}/config/runtime.env"
cat > "${RECONCILE_BIN}/uname" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-}" == "-s" ]]; then
  printf '%s\n' Darwin
  exit 0
fi
exit 64
SH
cat > "${RECONCILE_APP}/venv/bin/coinpilot" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-}" == "--help" ]]; then
  printf '%s\n' 'shadow-run shadow-notify shadow-web shadow-watchdog paper status'
  exit 0
fi
exit 64
SH
cat > "${RECONCILE_BIN}/launchctl" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "${TEST_LAUNCHCTL_LOG:?}"
case "${1:-}" in
  print)
    case "${2:-}" in
      "gui/$(id -u)"|"user/$(id -u)") exit 0 ;;
    esac
    label="${2##*/}"
    [[ -e "${TEST_LAUNCHCTL_STATE_DIR:?}/${label}" ]]
    ;;
  disable)
    label="${2##*/}"
    if [[ "${TEST_DISABLE_FAIL_LABEL:-}" == "${label}" ]]; then
      printf '%s\n' "injected disable failure for ${label}" >&2
      exit 70
    fi
    ;;
  enable) exit 0 ;;
  bootout)
    label="${2##*/}"
    rm -f "${TEST_LAUNCHCTL_STATE_DIR:?}/${label}"
    ;;
  bootstrap)
    printf '%s\n' 'injected shadow bootstrap failure' >&2
    exit 73
    ;;
  *) exit 64 ;;
esac
SH
chmod 755 \
  "${RECONCILE_BIN}/uname" \
  "${RECONCILE_BIN}/launchctl" \
  "${RECONCILE_APP}/venv/bin/coinpilot"
if PATH="${RECONCILE_BIN}:/usr/bin:/bin" \
    HOME="${RECONCILE_HOME}" \
    TEST_LAUNCHCTL_LOG="${RECONCILE_LOG}" \
    TEST_LAUNCHCTL_STATE_DIR="${RECONCILE_STATE_DIR}" \
    "${REPO_ROOT}/scripts/mac-studio" start all \
    --home "${RECONCILE_APP}" --apply \
    > "${TMP_ROOT}/reconcile-bootstrap-failure.log" 2>&1; then
  echo "error: injected enabled-service bootstrap failure was ignored" >&2
  exit 1
fi
first_bootstrap_line="$(grep -n '^bootstrap ' "${RECONCILE_LOG}" | head -n 1 | cut -d: -f1)"
for stale_label in \
  dev.coinpilot.paper \
  dev.coinpilot.notifier \
  dev.coinpilot.web \
  dev.coinpilot.external-watchdog; do
  bootout_line="$(
    grep -n "^bootout gui/$(id -u)/${stale_label}$" "${RECONCILE_LOG}" |
      head -n 1 | cut -d: -f1
  )"
  [[ -n "${bootout_line}" && "${bootout_line}" -lt "${first_bootstrap_line}" ]]
  [[ ! -e "${RECONCILE_STATE_DIR}/${stale_label}" ]]
done
grep -Fq 'launchctl bootstrap failed for shadow' \
  "${TMP_ROOT}/reconcile-bootstrap-failure.log"
echo "OK disabled reconciliation completes before enabled bootstrap failure"

rm -f "${RECONCILE_LOG}"
for loaded_label in \
  dev.coinpilot.shadow \
  dev.coinpilot.paper \
  dev.coinpilot.notifier \
  dev.coinpilot.web \
  dev.coinpilot.external-watchdog \
  dev.coinpilot.backup \
  dev.coinpilot.retention; do
  touch "${RECONCILE_STATE_DIR}/${loaded_label}"
done
if PATH="${RECONCILE_BIN}:/usr/bin:/bin" \
    HOME="${RECONCILE_HOME}" \
    TEST_LAUNCHCTL_LOG="${RECONCILE_LOG}" \
    TEST_LAUNCHCTL_STATE_DIR="${RECONCILE_STATE_DIR}" \
    TEST_DISABLE_FAIL_LABEL='dev.coinpilot.shadow' \
    "${REPO_ROOT}/scripts/mac-studio" stop all \
    --home "${RECONCILE_APP}" --apply \
    > "${TMP_ROOT}/stop-all-first-failure.log" 2>&1; then
  echo "error: stop all ignored an injected shadow failure" >&2
  exit 1
fi
grep -Fq "disable gui/$(id -u)/dev.coinpilot.shadow" "${RECONCILE_LOG}"
grep -Fq "bootout gui/$(id -u)/dev.coinpilot.shadow" "${RECONCILE_LOG}"
grep -Fq "disable gui/$(id -u)/dev.coinpilot.notifier" "${RECONCILE_LOG}"
grep -Fq "bootout gui/$(id -u)/dev.coinpilot.notifier" "${RECONCILE_LOG}"
[[ ! -e "${RECONCILE_STATE_DIR}/dev.coinpilot.notifier" ]]
grep -Fq \
  'launchctl disable failed for shadow: injected disable failure for dev.coinpilot.shadow' \
  "${TMP_ROOT}/stop-all-first-failure.log"
echo "OK stop all continues through notifier after first-service failure"

rm -f "${RECONCILE_LOG}"
for loaded_label in \
  dev.coinpilot.shadow \
  dev.coinpilot.paper \
  dev.coinpilot.notifier \
  dev.coinpilot.web \
  dev.coinpilot.external-watchdog \
  dev.coinpilot.backup \
  dev.coinpilot.retention; do
  touch "${RECONCILE_STATE_DIR}/${loaded_label}"
done
if PATH="${RECONCILE_BIN}:/usr/bin:/bin" \
    HOME="${RECONCILE_HOME}" \
    TEST_LAUNCHCTL_LOG="${RECONCILE_LOG}" \
    TEST_LAUNCHCTL_STATE_DIR="${RECONCILE_STATE_DIR}" \
    TEST_DISABLE_FAIL_LABEL='dev.coinpilot.shadow' \
    "${REPO_ROOT}/scripts/mac-studio" install \
    --home "${RECONCILE_APP}" --no-start --apply \
    > "${TMP_ROOT}/no-start-first-failure.log" 2>&1; then
  echo "error: --no-start ignored an injected shadow stop failure" >&2
  exit 1
fi
grep -Fq "bootout gui/$(id -u)/dev.coinpilot.notifier" "${RECONCILE_LOG}"
[[ ! -e "${RECONCILE_STATE_DIR}/dev.coinpilot.notifier" ]]
if grep -Fq 'Creating/updating isolated Python environment' \
    "${TMP_ROOT}/no-start-first-failure.log"; then
  echo "error: --no-start mutated the install before stop reconciliation" >&2
  exit 1
fi
echo "OK --no-start reconciles every service before install mutation"

rm -f "${RECONCILE_LOG}"
for loaded_label in \
  dev.coinpilot.shadow \
  dev.coinpilot.paper \
  dev.coinpilot.notifier \
  dev.coinpilot.web \
  dev.coinpilot.external-watchdog \
  dev.coinpilot.backup \
  dev.coinpilot.retention; do
  touch "${RECONCILE_STATE_DIR}/${loaded_label}"
done
if PATH="${RECONCILE_BIN}:/usr/bin:/bin" \
    HOME="${RECONCILE_HOME}" \
    TEST_LAUNCHCTL_LOG="${RECONCILE_LOG}" \
    TEST_LAUNCHCTL_STATE_DIR="${RECONCILE_STATE_DIR}" \
    TEST_DISABLE_FAIL_LABEL='dev.coinpilot.shadow' \
    "${REPO_ROOT}/scripts/mac-studio" uninstall \
    --home "${RECONCILE_APP}" --apply \
    > "${TMP_ROOT}/uninstall-first-failure.log" 2>&1; then
  echo "error: uninstall ignored an injected shadow stop failure" >&2
  exit 1
fi
grep -Fq "bootout gui/$(id -u)/dev.coinpilot.notifier" "${RECONCILE_LOG}"
[[ ! -e "${RECONCILE_STATE_DIR}/dev.coinpilot.notifier" ]]
grep -Fq 'failed to stop launchd services' \
  "${TMP_ROOT}/uninstall-first-failure.log"
echo "OK uninstall reconciles every service before reporting failures"

PYTHON_DETECTION_BIN="${TMP_ROOT}/python-detection-bin"
PYTHON_FORMULA_PREFIX="${TMP_ROOT}/homebrew/opt/python@3.12"
mkdir -p "${PYTHON_DETECTION_BIN}" "${PYTHON_FORMULA_PREFIX}/bin"
cat > "${PYTHON_DETECTION_BIN}/python3" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-}" == "--version" ]]; then
  echo "Python 3.9.6"
fi
exit 1
SH
cat > "${PYTHON_DETECTION_BIN}/python3.12" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-}" == "--version" ]]; then
  echo "Python 3.9.6"
fi
exit 1
SH
cat > "${PYTHON_DETECTION_BIN}/brew" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-}" == "--prefix" && "${2:-}" == "python@3.12" ]]; then
  printf '%s\n' "${TEST_BREW_PYTHON_PREFIX:?}"
  exit 0
fi
exit 1
SH
cat > "${PYTHON_FORMULA_PREFIX}/bin/python3.12" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-}" == "-c" ]]; then
  exit 0
fi
if [[ "${1:-}" == "--version" ]]; then
  echo "Python 3.12.99"
  exit 0
fi
exit 1
SH
chmod 755 \
  "${PYTHON_DETECTION_BIN}/python3" \
  "${PYTHON_DETECTION_BIN}/python3.12" \
  "${PYTHON_DETECTION_BIN}/brew" \
  "${PYTHON_FORMULA_PREFIX}/bin/python3.12"
PATH="${PYTHON_DETECTION_BIN}:/usr/bin:/bin" \
  TEST_BREW_PYTHON_PREFIX="${PYTHON_FORMULA_PREFIX}" \
  HOME="${TMP_ROOT}/python-detection-home" \
  "${REPO_ROOT}/scripts/mac-studio" doctor \
  --home "${TMP_ROOT}/python-detection-app" \
  > "${TMP_ROOT}/python-detection.log" 2>&1 || true
grep -Fq \
  "OK    Python >= 3.11: ${PYTHON_FORMULA_PREFIX}/bin/python3.12" \
  "${TMP_ROOT}/python-detection.log"
echo "OK Homebrew versioned Python detection"

python3 - "${REPO_ROOT}/scripts/mac-studio" <<'PY'
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
match = re.search(
    r"^install_homebrew_python\(\) \{\n(?P<body>.*?)^\}",
    text,
    flags=re.MULTILINE | re.DOTALL,
)
if match is None:
    raise SystemExit("missing install_homebrew_python helper")
body = match.group("body")
if "(umask 022; brew install python@3.12)" not in body:
    raise SystemExit("Homebrew install must run with umask 022 in a subshell")
if "run_cmd brew install python@3.12" in text:
    raise SystemExit("Homebrew install must not inherit Coinpilot's umask 077")
print("OK Homebrew install uses isolated umask 022")
PY

python3 - "${REPO_ROOT}/ops/launchd" <<'PY'
import html
import pathlib
import plistlib
import sys

directory = pathlib.Path(sys.argv[1])
values = {
    "LABEL": "dev.coinpilot.test",
    "APP_HOME": "/Users/test/Library/Application Support/Coinpilot",
    "REPO_ROOT": "/Users/test/src/ai-coin-trading",
    "CONFIG_PATH": "/Users/test/Library/Application Support/Coinpilot/config/config.toml",
    "RUNTIME_ENV": "/Users/test/Library/Application Support/Coinpilot/config/runtime.env",
    "LOG_DIR": "/Users/test/Library/Logs/Coinpilot",
    "SERVICE_EXECUTABLE": "/Users/test/Library/Application Support/Coinpilot/bin/coinpilot-service",
    "BACKUP_EXECUTABLE": "/Users/test/Library/Application Support/Coinpilot/bin/coinpilot-backup",
    "RETENTION_EXECUTABLE": "/Users/test/Library/Application Support/Coinpilot/bin/coinpilot-retention",
    "KEYCHAIN_SERVICE": "coinpilot-slack-webhook",
    "KEYCHAIN_ACCOUNT": "coinpilot-shadow",
}
templates = sorted(directory.glob("*.plist.in"))
if len(templates) != 7:
    raise SystemExit(f"expected 7 launchd templates, found {len(templates)}")
for template in templates:
    text = template.read_text(encoding="utf-8")
    for key, value in values.items():
        text = text.replace(f"@@{key}@@", html.escape(value, quote=True))
    if "@@" in text:
        raise SystemExit(f"unresolved placeholder in {template}")
    parsed = plistlib.loads(text.encode("utf-8"))
    if template.name in {
        "dev.coinpilot.shadow.plist.in",
        "dev.coinpilot.notifier.plist.in",
    }:
        if parsed.get("KeepAlive") is not True:
            raise SystemExit(f"{template} must restart after every unexpected exit")
        if int(parsed.get("ThrottleInterval", 0)) < 30:
            raise SystemExit(f"{template} needs a bounded restart throttle")
    if template.name == "dev.coinpilot.paper.plist.in":
        if parsed.get("KeepAlive") != {"SuccessfulExit": False}:
            raise SystemExit(f"{template} must restart only after an unexpected exit")
        if int(parsed.get("ThrottleInterval", 0)) < 60:
            raise SystemExit(f"{template} needs a conservative restart throttle")
        arguments = parsed.get("ProgramArguments", [])
        if arguments[-1:] != ["paper"]:
            raise SystemExit(f"{template} must use the paper service wrapper")
print("OK rendered launchd plist syntax")
PY

HOME="${TMP_ROOT}/home" \
  "${REPO_ROOT}/scripts/mac-studio" bootstrap \
  --home "${TMP_ROOT}/app" > "${TMP_ROOT}/dry-run.log"
grep -q 'Dry-run only' "${TMP_ROOT}/dry-run.log"
if [[ -e "${TMP_ROOT}/app" || -e "${TMP_ROOT}/home" ]]; then
  echo "error: dry-run created application files" >&2
  exit 1
fi
echo "OK bootstrap dry-run is non-mutating"

DISABLED_INSTALL_APP="${TMP_ROOT}/disabled-install-app"
mkdir -p "${DISABLED_INSTALL_APP}/config"
printf '%s\n' \
  'COINPILOT_ENABLE_SHADOW=0' \
  'COINPILOT_ENABLE_PAPER=0' \
  'COINPILOT_ENABLE_NOTIFIER=0' \
  'COINPILOT_ENABLE_WEB=0' \
  'COINPILOT_ENABLE_WATCHDOG=0' \
  > "${DISABLED_INSTALL_APP}/config/runtime.env"
HOME="${TMP_ROOT}/disabled-install-home" \
  "${REPO_ROOT}/scripts/mac-studio" install \
  --home "${DISABLED_INSTALL_APP}" \
  > "${TMP_ROOT}/disabled-install.log"
for disabled_service in shadow paper notifier web external-watchdog; do
  grep -Eq \
    "launchctl disable (gui|user)/$(id -u)/dev\\.coinpilot\\.${disabled_service}" \
    "${TMP_ROOT}/disabled-install.log"
done
echo "OK install persistently disables configured-disabled services"

NO_START_APP="${TMP_ROOT}/no-start-app"
mkdir -p "${NO_START_APP}/config"
printf '%s\n' \
  'COINPILOT_ENABLE_SHADOW=1' \
  'COINPILOT_ENABLE_PAPER=0' \
  'COINPILOT_ENABLE_NOTIFIER=1' \
  'COINPILOT_ENABLE_WEB=1' \
  'COINPILOT_ENABLE_WATCHDOG=1' \
  > "${NO_START_APP}/config/runtime.env"
HOME="${TMP_ROOT}/no-start-home" \
  "${REPO_ROOT}/scripts/mac-studio" install \
  --home "${NO_START_APP}" --no-start \
  > "${TMP_ROOT}/no-start-install.log"
for disabled_service in \
  shadow paper notifier web external-watchdog backup retention; do
  disable_count="$(
    grep -Ec \
      "launchctl disable (gui|user)/$(id -u)/dev\\.coinpilot\\.${disabled_service}" \
      "${TMP_ROOT}/no-start-install.log"
  )"
  bootout_count="$(
    grep -Ec \
      "launchctl bootout (gui|user)/$(id -u)/dev\\.coinpilot\\.${disabled_service}" \
      "${TMP_ROOT}/no-start-install.log"
  )"
  [[ "${disable_count}" -eq 1 && "${bootout_count}" -eq 1 ]]
done
if grep -Fq 'launchctl enable ' "${TMP_ROOT}/no-start-install.log" ||
    grep -Fq 'launchctl bootstrap ' "${TMP_ROOT}/no-start-install.log"; then
  echo "error: --no-start enabled or bootstrapped a launchd job" >&2
  exit 1
fi
echo "OK --no-start persistently disables every installed service"

NAMED_TEST_HOME="${TMP_ROOT}/named-home"
NAMED_APP_HOME="${NAMED_TEST_HOME}/Library/Application Support/Coinpilot/instances/btc"
HOME="${NAMED_TEST_HOME}" \
  "${REPO_ROOT}/scripts/mac-studio" bootstrap \
  --instance btc \
  --market KRW-BTC \
  --initial-cash 5000000 \
  --order-quote 125000 \
  --shadow-mode diagnostic \
  --web-port 8766 \
  --no-start > "${TMP_ROOT}/named-dry-run.log"
grep -Fq "Coinpilot/instances/btc" "${TMP_ROOT}/named-dry-run.log"
grep -Fq "dev.coinpilot.btc.shadow.plist" "${TMP_ROOT}/named-dry-run.log"
grep -Fq -- "--force-reinstall" "${TMP_ROOT}/named-dry-run.log"
if grep -Fq -- "--editable" "${TMP_ROOT}/named-dry-run.log"; then
  echo "error: named instance dry-run used an editable package install" >&2
  exit 1
fi
if [[ -e "${NAMED_APP_HOME}" ]]; then
  echo "error: named dry-run created application files" >&2
  exit 1
fi
echo "OK named instance dry-run is isolated and non-editable"

VALIDATOR_ROOT="$(cd "${TMP_ROOT}" && pwd -P)/named-shadow-paths"
VALIDATOR_PYTHON="$(command -v python3.12 || command -v python3)"

write_named_validator_config() {
  local app_home="$1"
  local shadow_database="$2"
  mkdir -p \
    "${app_home}/config" \
    "${app_home}/data/raw" \
    "${app_home}/backups" \
    "${app_home}/logs"
  cat > "${app_home}/config/config.toml" <<EOF
[data]
market = "KRW-BTC"
database_path = "${app_home}/data/coinpilot.db"

[risk]
initial_cash = 5000000

[shadow]
mode = "diagnostic"
database_path = "${shadow_database}"
archive_root = "${app_home}/data/raw"
initial_cash = 5000000
order_quote = 25000

[operations]
bind_host = "127.0.0.1"
port = 18766
backup_dir = "${app_home}/backups"
EOF
  cat > "${app_home}/config/runtime.env" <<EOF
COINPILOT_WEB_HOST=127.0.0.1
COINPILOT_WEB_PORT=18766
COINPILOT_LOG_DIR=${app_home}/logs
COINPILOT_ENABLE_SHADOW=0
COINPILOT_ENABLE_PAPER=0
COINPILOT_ENABLE_NOTIFIER=0
COINPILOT_ENABLE_WEB=0
COINPILOT_ENABLE_WATCHDOG=0
EOF
}

run_named_validator_doctor() {
  local app_home="$1"
  local output="$2"
  COINPILOT_PYTHON="${VALIDATOR_PYTHON}" \
    HOME="${VALIDATOR_ROOT}/home" \
    "${REPO_ROOT}/scripts/mac-studio" doctor \
    --instance validator --home "${app_home}" > "${output}" 2>&1 || true
}

VERSIONED_APP="${VALIDATOR_ROOT}/versioned"
write_named_validator_config \
  "${VERSIONED_APP}" \
  "${VERSIONED_APP}/data/shadow-diagnostic-bounded-v1-a1b2c3d4e5f6.db"
run_named_validator_doctor \
  "${VERSIONED_APP}" "${VALIDATOR_ROOT}/versioned.log"
grep -Fq 'OK    named instance paths and capital boundaries are isolated' \
  "${VALIDATOR_ROOT}/versioned.log"

NESTED_APP="${VALIDATOR_ROOT}/nested"
write_named_validator_config \
  "${NESTED_APP}" "${NESTED_APP}/data/v2/shadow-v2.db"
run_named_validator_doctor "${NESTED_APP}" "${VALIDATOR_ROOT}/nested.log"
grep -Fq 'shadow.database_path must be directly inside' \
  "${VALIDATOR_ROOT}/nested.log"
grep -Fq 'FAIL  named instance configuration failed isolation validation' \
  "${VALIDATOR_ROOT}/nested.log"

OUTSIDE_APP="${VALIDATOR_ROOT}/outside-app"
OUTSIDE_DATABASE="${VALIDATOR_ROOT}/outside-data/shadow-v2.db"
write_named_validator_config "${OUTSIDE_APP}" "${OUTSIDE_DATABASE}"
run_named_validator_doctor "${OUTSIDE_APP}" "${VALIDATOR_ROOT}/outside.log"
grep -Fq 'shadow.database_path escapes instance home' \
  "${VALIDATOR_ROOT}/outside.log"
grep -Fq 'FAIL  named instance configuration failed isolation validation' \
  "${VALIDATOR_ROOT}/outside.log"

SYMLINK_APP="${VALIDATOR_ROOT}/symlink"
SYMLINK_DATABASE="${SYMLINK_APP}/data/shadow-v2.db"
write_named_validator_config "${SYMLINK_APP}" "${SYMLINK_DATABASE}"
mkdir -p "${VALIDATOR_ROOT}/symlink-target"
ln -s \
  "${VALIDATOR_ROOT}/symlink-target/shadow-v2.db" \
  "${SYMLINK_DATABASE}"
run_named_validator_doctor "${SYMLINK_APP}" "${VALIDATOR_ROOT}/symlink.log"
grep -Fq 'symlinked managed path' "${VALIDATOR_ROOT}/symlink.log"
grep -Fq 'FAIL  named instance configuration failed isolation validation' \
  "${VALIDATOR_ROOT}/symlink.log"

HARDLINK_APP="${VALIDATOR_ROOT}/hardlink"
HARDLINK_DATABASE="${HARDLINK_APP}/data/shadow-v2.db"
write_named_validator_config "${HARDLINK_APP}" "${HARDLINK_DATABASE}"
touch "${HARDLINK_APP}/data/coinpilot.db"
ln "${HARDLINK_APP}/data/coinpilot.db" "${HARDLINK_DATABASE}"
run_named_validator_doctor "${HARDLINK_APP}" "${VALIDATOR_ROOT}/hardlink.log"
grep -Fq 'shadow.database_path must not be hard-linked' \
  "${VALIDATOR_ROOT}/hardlink.log"
grep -Fq 'FAIL  named instance configuration failed isolation validation' \
  "${VALIDATOR_ROOT}/hardlink.log"

UNSAFE_APP="${VALIDATOR_ROOT}/unsafe-name"
write_named_validator_config \
  "${UNSAFE_APP}" "${UNSAFE_APP}/data/ledger-v2.db"
run_named_validator_doctor "${UNSAFE_APP}" "${VALIDATOR_ROOT}/unsafe-name.log"
grep -Fq \
  'shadow.database_path filename must be shadow.db or shadow-<version>.db' \
  "${VALIDATOR_ROOT}/unsafe-name.log"
grep -Fq 'FAIL  named instance configuration failed isolation validation' \
  "${VALIDATOR_ROOT}/unsafe-name.log"
echo "OK named shadow database version and path boundaries"

if HOME="${NAMED_TEST_HOME}" "${REPO_ROOT}/scripts/mac-studio" bootstrap \
    --instance btc --market KRW-BTC --initial-cash 5000000 \
    --order-quote 125000 --shadow-mode diagnostic --web-port 8766 \
    > "${TMP_ROOT}/named-autostart.log" 2>&1; then
  echo "error: new named diagnostic instance allowed automatic first start" >&2
  exit 1
fi
if HOME="${NAMED_TEST_HOME}" "${REPO_ROOT}/scripts/mac-studio" bootstrap \
    --instance "BTC_bad" --no-start > "${TMP_ROOT}/bad-instance.log" 2>&1; then
  echo "error: invalid instance slug was accepted" >&2
  exit 1
fi
if HOME="${NAMED_TEST_HOME}" "${REPO_ROOT}/scripts/mac-studio" bootstrap \
    --instance eth --market ETH --initial-cash 5000000 \
    --order-quote 125000 --shadow-mode diagnostic --web-port 8767 \
    --no-start > "${TMP_ROOT}/bad-market.log" 2>&1; then
  echo "error: invalid market was accepted" >&2
  exit 1
fi
if HOME="${NAMED_TEST_HOME}" "${REPO_ROOT}/scripts/mac-studio" bootstrap \
    --instance eth --market KRW-ETH --initial-cash 5000000 \
    --order-quote 5000001 --shadow-mode diagnostic --web-port 8767 \
    --no-start > "${TMP_ROOT}/bad-capital.log" 2>&1; then
  echo "error: order quote above initial cash was accepted" >&2
  exit 1
fi
if HOME="${NAMED_TEST_HOME}" "${REPO_ROOT}/scripts/mac-studio" bootstrap \
    --instance eth --market KRW-ETH --initial-cash 5000000 \
    --order-quote 125000 --shadow-mode live --web-port 8767 \
    --no-start > "${TMP_ROOT}/bad-mode.log" 2>&1; then
  echo "error: unsafe shadow mode was accepted" >&2
  exit 1
fi
echo "OK named instance inputs fail closed"

HOME="${TMP_ROOT}/home" "${REPO_ROOT}/scripts/mac-studio-backup" \
  --home "${TMP_ROOT}/app" > "${TMP_ROOT}/backup-dry-run.log"
grep -q 'dry-run' "${TMP_ROOT}/backup-dry-run.log"
HOME="${TMP_ROOT}/home" "${REPO_ROOT}/scripts/mac-studio-retention" \
  --home "${TMP_ROOT}/app" > "${TMP_ROOT}/retention-dry-run.log"
grep -q 'Dry-run only' "${TMP_ROOT}/retention-dry-run.log"
echo "OK maintenance dry-runs"

PAPER_BACKUP_HOME="${TMP_ROOT}/paper-backup-app"
mkdir -p \
  "${PAPER_BACKUP_HOME}/config" \
  "${PAPER_BACKUP_HOME}/data" \
  "${PAPER_BACKUP_HOME}/backups" \
  "${PAPER_BACKUP_HOME}/state" \
  "${PAPER_BACKUP_HOME}/venv/bin"
TEST_PYTHON_SOURCE="$(command -v python3.12 || command -v python3)"
TEST_PYTHON_REAL="$("${TEST_PYTHON_SOURCE}" -c \
  'import os, sys; print(os.path.realpath(sys.executable))')"
ln -s "${TEST_PYTHON_REAL}" "${PAPER_BACKUP_HOME}/venv/bin/python"
cat > "${PAPER_BACKUP_HOME}/config/config.toml" <<EOF
[shadow]
database_path = "${PAPER_BACKUP_HOME}/data/shadow.db"

[operations]
backup_dir = "${PAPER_BACKUP_HOME}/backups"
EOF
cat > "${PAPER_BACKUP_HOME}/config/paper.toml" <<EOF
[data]
database_path = "${PAPER_BACKUP_HOME}/data/coinpilot-c2.db"
EOF
printf '%s\n' 'COINPILOT_ENABLE_PAPER=1' \
  > "${PAPER_BACKUP_HOME}/config/runtime.env"
"${TEST_PYTHON_SOURCE}" - \
  "${PAPER_BACKUP_HOME}/data/shadow.db" \
  "${PAPER_BACKUP_HOME}/data/coinpilot-c2.db" <<'PY'
import sqlite3
import sys

for path in sys.argv[1:]:
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE evidence(value TEXT NOT NULL)")
    connection.execute("INSERT INTO evidence VALUES ('preserved')")
    connection.commit()
    connection.close()
PY
HOME="${TMP_ROOT}/paper-backup-home" \
  PYTHONPATH="${REPO_ROOT}/src" \
  "${REPO_ROOT}/scripts/mac-studio-backup" \
  --home "${PAPER_BACKUP_HOME}" --apply \
  > "${TMP_ROOT}/paper-backup.log"
test "$(find "${PAPER_BACKUP_HOME}/backups" -name 'coinpilot-*.sqlite' \
  ! -name 'coinpilot-paper-c2-*' | wc -l | tr -d '[:space:]')" -eq 1
test "$(find "${PAPER_BACKUP_HOME}/backups" -name 'coinpilot-paper-c2-*.sqlite' \
  | wc -l | tr -d '[:space:]')" -eq 1
test "$(find "${PAPER_BACKUP_HOME}/backups" -name '*.sha256' \
  | wc -l | tr -d '[:space:]')" -eq 2
grep -Fq 'Verified backup created' "${TMP_ROOT}/paper-backup.log"
echo "OK shadow and enabled paper databases receive verified online backups"

MANAGED_HOME="${TMP_ROOT}/managed-app"
MANAGED_LOG="${TMP_ROOT}/home/Library/Logs/Coinpilot"
mkdir -p "${MANAGED_HOME}/data/raw" "${MANAGED_HOME}/backups" "${MANAGED_LOG}"
for managed_dir in \
  "${MANAGED_HOME}/data/raw" "${MANAGED_HOME}/backups" "${MANAGED_LOG}"; do
  printf 'coinpilot-managed-v1\n' > "${managed_dir}/.coinpilot-managed"
done
mkdir -p \
  "${MANAGED_HOME}/data/raw/.audit-journal" \
  "${MANAGED_HOME}/data/raw/.audit-quarantine"
touch -t 202001010000 \
  "${MANAGED_HOME}/data/raw/old.jsonl.gz" \
  "${MANAGED_HOME}/data/raw/.audit-journal/recovery.audit.wal" \
  "${MANAGED_HOME}/data/raw/.audit-journal/recorder.lock" \
  "${MANAGED_HOME}/data/raw/.audit-quarantine/evidence.quarantined" \
  "${MANAGED_HOME}/backups/old.db" \
  "${MANAGED_LOG}/shadow.stdout.log.20200101.gz" \
  "${MANAGED_LOG}/shadow.stdout.log"
HOME="${TMP_ROOT}/home" "${REPO_ROOT}/scripts/mac-studio-retention" \
  --home "${MANAGED_HOME}" --apply > "${TMP_ROOT}/retention-apply.log"
[[ ! -e "${MANAGED_HOME}/data/raw/old.jsonl.gz" ]]
[[ -e "${MANAGED_HOME}/data/raw/.audit-journal/recovery.audit.wal" ]]
[[ -e "${MANAGED_HOME}/data/raw/.audit-journal/recorder.lock" ]]
[[ -e "${MANAGED_HOME}/data/raw/.audit-quarantine/evidence.quarantined" ]]
[[ ! -e "${MANAGED_HOME}/backups/old.db" ]]
[[ ! -e "${MANAGED_LOG}/shadow.stdout.log.20200101.gz" ]]
[[ -e "${MANAGED_LOG}/shadow.stdout.log" ]]
[[ -e "${MANAGED_LOG}/.coinpilot-managed" ]]

OUTSIDE="${TMP_ROOT}/outside"
mkdir -p "${OUTSIDE}"
printf 'coinpilot-managed-v1\n' > "${OUTSIDE}/.coinpilot-managed"
if HOME="${TMP_ROOT}/home" COINPILOT_LOG_DIR="${OUTSIDE}" \
    "${REPO_ROOT}/scripts/mac-studio-retention" \
    --home "${MANAGED_HOME}" > "${TMP_ROOT}/retention-outside.log" 2>&1; then
  echo "error: retention accepted an outside managed path" >&2
  exit 1
fi

UNMARKED_HOME="${TMP_ROOT}/unmarked-app"
mkdir -p "${UNMARKED_HOME}/data/raw"
if HOME="${TMP_ROOT}/isolated-home" \
    "${REPO_ROOT}/scripts/mac-studio-retention" \
    --home "${UNMARKED_HOME}" > "${TMP_ROOT}/retention-unmarked.log" 2>&1; then
  echo "error: retention accepted a directory without its managed sentinel" >&2
  exit 1
fi
echo "OK retention boundaries and sentinels"

NAMED_RETENTION_HOME="${TMP_ROOT}/named-retention"
NAMED_RETENTION_LOG="${NAMED_RETENTION_HOME}/logs"
SIBLING_LOG="${TMP_ROOT}/sibling-retention/logs"
mkdir -p \
  "${NAMED_RETENTION_HOME}/config" \
  "${NAMED_RETENTION_HOME}/data/raw" \
  "${NAMED_RETENTION_HOME}/backups" \
  "${NAMED_RETENTION_LOG}" \
  "${SIBLING_LOG}"
for managed_dir in \
  "${NAMED_RETENTION_HOME}/data/raw" \
  "${NAMED_RETENTION_HOME}/backups" \
  "${NAMED_RETENTION_LOG}"; do
  printf 'coinpilot-managed-v1\n' > "${managed_dir}/.coinpilot-managed"
done
printf '%s\n' \
  "COINPILOT_LOG_DIR=${NAMED_RETENTION_LOG}" \
  "COINPILOT_RETENTION_DAYS=90" \
  "COINPILOT_BACKUP_RETENTION_DAYS=35" \
  "COINPILOT_LOG_RETENTION_DAYS=1" \
  "COINPILOT_LOG_MAX_MB=100" \
  > "${NAMED_RETENTION_HOME}/config/runtime.env"
touch -t 202001010000 \
  "${NAMED_RETENTION_LOG}/shadow.stdout.log.20200101.gz" \
  "${SIBLING_LOG}/shadow.stdout.log.20200101.gz"
HOME="${TMP_ROOT}/retention-home" \
  "${REPO_ROOT}/scripts/mac-studio-retention" \
  --home "${NAMED_RETENTION_HOME}" --apply \
  > "${TMP_ROOT}/named-retention.log"
[[ ! -e "${NAMED_RETENTION_LOG}/shadow.stdout.log.20200101.gz" ]]
[[ -e "${SIBLING_LOG}/shadow.stdout.log.20200101.gz" ]]
echo "OK named retention is confined to its instance log directory"

python3 - "${REPO_ROOT}/scripts/mac-studio-keychain" <<'PY'
import pathlib
import sys

text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
logical = text.replace("\\\n", " ")
commands = [
    line.strip()
    for line in logical.splitlines()
    if "security add-generic-password" in line
]
if len(commands) != 1 or not commands[0].endswith("-w >/dev/null"):
    raise SystemExit("Keychain add must use Apple's final bare -w secure prompt")
if '-w "${WEBHOOK}"' in text or "export WEBHOOK" in text:
    raise SystemExit("Slack webhook must never enter argv or environment")
print("OK Keychain secret is absent from argv and environment")
PY

if grep -R -E 'hooks\.slack(-gov)?\.com/services/[A-Z0-9]{6,}/[A-Z0-9]{6,}/[A-Za-z0-9]{12,}' \
  "${REPO_ROOT}/ops" "${REPO_ROOT}/scripts" >/dev/null 2>&1; then
  echo "error: a Slack webhook-shaped value appears in tracked operations files" >&2
  exit 1
fi
echo "OK no Slack webhook-shaped value"

if grep -R -E '(^|[;&|])[[:space:]]*sudo[[:space:]]' \
  "${REPO_ROOT}/ops" "${REPO_ROOT}/scripts" >/dev/null 2>&1; then
  echo "error: operations scripts must not invoke sudo" >&2
  exit 1
fi
echo "OK no implicit sudo"

"${REPO_ROOT}/scripts/test-daily-scorecard-ops.sh"
