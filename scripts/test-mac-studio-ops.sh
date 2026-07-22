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
  "${REPO_ROOT}/scripts/coinpilot-service.sh"
  "${REPO_ROOT}/scripts/mac-studio-keychain"
  "${REPO_ROOT}/scripts/mac-studio-backup"
  "${REPO_ROOT}/scripts/mac-studio-retention"
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

RESTART_RACE_BIN="${TMP_ROOT}/restart-race-bin"
RESTART_RACE_HOME="${TMP_ROOT}/restart-race-home"
RESTART_RACE_APP="${TMP_ROOT}/restart-race-app"
RESTART_RACE_LOG="${TMP_ROOT}/restart-race.log"
RESTART_RACE_STATE="${TMP_ROOT}/restart-race.state"
RESTART_RACE_EARLY="${TMP_ROOT}/restart-race.early"
mkdir -p \
  "${RESTART_RACE_BIN}" \
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
      *) exit 0 ;;
    esac
    ;;
  bootout)
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
  enable) exit 0 ;;
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
  "${REPO_ROOT}/scripts/mac-studio" restart shadow \
  --home "${RESTART_RACE_APP}" --apply
if [[ "$(grep -c '^bootout ' "${RESTART_RACE_LOG}")" -ne 1 ]]; then
  echo "error: restart issued more than one launchd bootout" >&2
  exit 1
fi
grep -Fq 'bootstrap ' "${RESTART_RACE_LOG}"
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
