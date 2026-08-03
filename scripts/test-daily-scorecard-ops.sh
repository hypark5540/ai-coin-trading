#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/coinpilot-daily-ops.XXXXXX")"
TMP_ROOT="$(cd "${TMP_ROOT}" && pwd -P)"
cleanup() { rm -rf "${TMP_ROOT}"; }
trap cleanup EXIT INT TERM

MANAGER="${REPO_ROOT}/scripts/mac-studio-daily-scorecard"
REPORTER="${REPO_ROOT}/scripts/coinpilot_daily_scorecard.py"
TEMPLATE="${REPO_ROOT}/ops/reporting/dev.coinpilot.daily-scorecard.plist.in"

bash -n "${MANAGER}" "${REPO_ROOT}/scripts/test-daily-scorecard-ops.sh"
python3 -m py_compile "${REPORTER}"
echo "OK daily scorecard syntax"

grep -Fq 'D2_INSTANCES' "${REPORTER}"
grep -Fq 'C2_INSTANCES' "${REPORTER}"
grep -Fq 'mode=ro' "${REPORTER}"
grep -Fq 'PRAGMA query_only = ON' "${REPORTER}"
grep -Fq 'legacy_instances_excluded' "${REPORTER}"
grep -Fq 'trading_outbox_mutation' "${REPORTER}"
if grep -Eq 'sqlite3\.connect\([^\n]*mode=rw' "${REPORTER}"; then
  echo "error: reporter contains a writable source SQLite URI" >&2
  exit 1
fi
echo "OK daily reporter source ledgers are explicitly read-only"

python3 - "${TEMPLATE}" <<'PY'
import pathlib
import plistlib
import sys

text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
replacements = {
    "LABEL": "dev.coinpilot.daily-scorecard",
    "PYTHON_EXECUTABLE": "/usr/bin/python3",
    "REPORTER_EXECUTABLE": "/tmp/reporting/bin/reporter",
    "COINPILOT_ROOT": "/tmp/Coinpilot",
    "REPORT_HOME": "/tmp/reporting",
    "STATE_DIR": "/tmp/reporting/state",
    "ARTIFACT_DIR": "/tmp/reporting/reports",
    "LOG_DIR": "/tmp/reporting/logs",
    "KEYCHAIN_SERVICE": "coinpilot-slack-webhook",
    "KEYCHAIN_ACCOUNT": "coinpilot-shadow",
}
for key, value in replacements.items():
    text = text.replace(f"@@{key}@@", value)
if "@@" in text:
    raise SystemExit("unresolved plist placeholder")
plist = plistlib.loads(text.encode())
if plist["Label"] != "dev.coinpilot.daily-scorecard":
    raise SystemExit("unexpected reporting label")
if plist["StartCalendarInterval"] != {"Minute": 10}:
    raise SystemExit("report retry cadence must be hourly at :10")
if plist.get("RunAtLoad") is not True or "KeepAlive" in plist:
    raise SystemExit("reporter must be a RunAtLoad one-shot without KeepAlive")
arguments = plist["ProgramArguments"]
if "--schedule-hour" not in arguments or arguments[arguments.index("--schedule-hour") + 1] != "0":
    raise SystemExit("normal report deadline must be 00:10 KST")
if "--schedule-minute" not in arguments or arguments[arguments.index("--schedule-minute") + 1] != "10":
    raise SystemExit("normal report deadline must be 00:10 KST")
joined = " ".join(arguments)
if "ai-coin-trading" in joined or "webhook_url" in joined.lower():
    raise SystemExit("plist must contain neither repository paths nor secrets")
print("OK daily scorecard plist schedule and isolation")
PY

DRY_HOME="${TMP_ROOT}/dry-home"
mkdir -p "${DRY_HOME}"
HOME="${DRY_HOME}" \
  COINPILOT_ROOT="${DRY_HOME}/Library/Application Support/Coinpilot" \
  COINPILOT_REPORT_HOME="${DRY_HOME}/Library/Application Support/Coinpilot/reporting" \
  "${MANAGER}" install --dry-run > "${TMP_ROOT}/dry-run.log"
grep -Fq 'dev.coinpilot.daily-scorecard' "${TMP_ROOT}/dry-run.log"
if grep -Eq 'dev\.coinpilot\.((d2|c2)-(btc|eth|xrp|sol)|(btc|eth|xrp|sol))\.' \
    "${TMP_ROOT}/dry-run.log"; then
  echo "error: central reporter dry-run targeted a trading service" >&2
  exit 1
fi
echo "OK daily scorecard install dry-run is central-only"

if HOME="${DRY_HOME}" COINPILOT_ROOT="${TMP_ROOT}/escaped-root" \
    "${MANAGER}" status > "${TMP_ROOT}/bad-root.log" 2>&1; then
  echo "error: arbitrary COINPILOT_ROOT was accepted" >&2
  exit 1
fi
if HOME="${DRY_HOME}" COINPILOT_REPORT_HOME="${TMP_ROOT}/escaped-reporting" \
    "${MANAGER}" status > "${TMP_ROOT}/bad-report-home.log" 2>&1; then
  echo "error: arbitrary COINPILOT_REPORT_HOME was accepted" >&2
  exit 1
fi
if HOME="${DRY_HOME}" "${MANAGER}" status --root "${TMP_ROOT}/escaped-root" \
    > "${TMP_ROOT}/bad-root-option.log" 2>&1; then
  echo "error: --root override was accepted" >&2
  exit 1
fi
if HOME="${DRY_HOME}" "${MANAGER}" status --report-home "${TMP_ROOT}/escaped-reporting" \
    > "${TMP_ROOT}/bad-report-option.log" 2>&1; then
  echo "error: --report-home override was accepted" >&2
  exit 1
fi
grep -Fq 'is fixed' "${TMP_ROOT}/bad-root.log"
grep -Fq 'not configurable' "${TMP_ROOT}/bad-report-option.log"
echo "OK daily scorecard paths are fixed below HOME"

URLISH_SELECTOR='https://example.invalid/services/not-a-secret'
if HOME="${DRY_HOME}" "${MANAGER}" status \
    --keychain-service "${URLISH_SELECTOR}" \
    > "${TMP_ROOT}/bad-keychain-url.log" 2>&1; then
  echo "error: URL-shaped Keychain selector was accepted" >&2
  exit 1
fi
if grep -Fq "${URLISH_SELECTOR}" "${TMP_ROOT}/bad-keychain-url.log"; then
  echo "error: rejected Keychain selector was echoed" >&2
  exit 1
fi
if HOME="${DRY_HOME}" "${MANAGER}" status \
    --keychain-account $'bad\nselector' \
    > "${TMP_ROOT}/bad-keychain-newline.log" 2>&1; then
  echo "error: newline Keychain selector was accepted" >&2
  exit 1
fi
if HOME="${DRY_HOME}" COINPILOT_SLACK_KEYCHAIN_SERVICE='bad selector' \
    "${MANAGER}" status > "${TMP_ROOT}/bad-keychain-env.log" 2>&1; then
  echo "error: whitespace Keychain selector environment value was accepted" >&2
  exit 1
fi
grep -Fq 'must be a 1-128 character Keychain identifier' \
  "${TMP_ROOT}/bad-keychain-newline.log"
echo "OK Keychain selectors reject URL, whitespace, newline, and non-identifiers"

FAKE_BIN="${TMP_ROOT}/fake-bin"
APPLY_HOME="${TMP_ROOT}/apply-home"
LAUNCH_LOG="${TMP_ROOT}/launchctl.log"
SERVICE_STATE="${TMP_ROOT}/service.state"
OVERRIDE_STATE="${TMP_ROOT}/override.state"
BOOTSTRAP_COUNT="${TMP_ROOT}/bootstrap.count"
REAL_PYTHON="$(command -v python3.12 2>/dev/null || command -v python3)"
PYTHON_WRAPPER="${FAKE_BIN}/python3.12"
mkdir -p "${FAKE_BIN}" "${APPLY_HOME}"
printf 'unloaded\n' > "${SERVICE_STATE}"
printf 'unset\n' > "${OVERRIDE_STATE}"
printf '0\n' > "${BOOTSTRAP_COUNT}"
cat > "${FAKE_BIN}/uname" <<'SH'
#!/usr/bin/env bash
printf 'Darwin\n'
SH
cat > "${FAKE_BIN}/security" <<'SH'
#!/usr/bin/env bash
exit 0
SH
cat > "${PYTHON_WRAPPER}" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-}" == *"/reporting/bin/coinpilot-daily-scorecard" ]]; then
  for argument in "$@"; do
    case "${argument}" in
      --dry-run) exit 0 ;;
      --status)
        if [[ "${TEST_REPORTER_STATUS_HEALTHY:-0}" == "1" ]]; then
          printf '%s\n' \
            '{"healthy":true,"latest_due_delivered":true,"latest_due_report_date":"2026-07-22","missing_backlog_days":0,"unresolved_receipts":0}'
        else
          printf '%s\n' \
            '{"healthy":false,"latest_due_delivered":true,"latest_due_report_date":"2026-07-22","missing_backlog_days":1,"unresolved_receipts":0}'
        fi
        exit 0
        ;;
    esac
  done
fi
exec "${TEST_REAL_PYTHON:?}" "$@"
SH
cat > "${FAKE_BIN}/launchctl" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "${TEST_LAUNCH_LOG:?}"
case "${1:-}" in
  print)
    case "${2:-}" in
      */dev.coinpilot.daily-scorecard)
        [[ "$(< "${TEST_SERVICE_STATE:?}")" == "loaded" ]]
        ;;
      *) exit 0 ;;
    esac
    ;;
  disable)
    if [[ "${TEST_DISABLE_FAIL:-0}" == "1" ]]; then
      printf 'injected disable failure\n' >&2
      exit 1
    fi
    printf 'disabled\n' > "${TEST_OVERRIDE_STATE:?}"
    ;;
  enable)
    if [[ "${TEST_ENABLE_FAIL:-0}" == "1" ]]; then
      printf 'injected enable failure\n' >&2
      exit 1
    fi
    printf 'enabled\n' > "${TEST_OVERRIDE_STATE:?}"
    ;;
  bootout)
    if [[ "${TEST_BOOTOUT_FAIL:-0}" == "1" ]]; then
      printf 'injected bootout failure\n' >&2
      exit 1
    fi
    printf 'unloaded\n' > "${TEST_SERVICE_STATE:?}"
    ;;
  bootstrap)
    count="$(< "${TEST_BOOTSTRAP_COUNT:?}")"
    count=$((count + 1))
    printf '%s\n' "${count}" > "${TEST_BOOTSTRAP_COUNT:?}"
    if [[ "${count}" -le "${TEST_BOOTSTRAP_FAILS:-0}" ]]; then
      printf 'injected bootstrap failure %s\n' "${count}" >&2
      exit 1
    fi
    if [[ "${count}" -le "${TEST_BOOTSTRAP_VANISHES:-0}" ]]; then
      printf 'unloaded\n' > "${TEST_SERVICE_STATE:?}"
    else
      printf 'loaded\n' > "${TEST_SERVICE_STATE:?}"
    fi
    ;;
  print-disabled)
    case "$(< "${TEST_OVERRIDE_STATE:?}")" in
      disabled) printf '\t"dev.coinpilot.daily-scorecard" => true\n' ;;
      enabled) printf '\t"dev.coinpilot.daily-scorecard" => false\n' ;;
    esac
    ;;
esac
SH
chmod 755 \
  "${FAKE_BIN}/uname" "${FAKE_BIN}/security" \
  "${PYTHON_WRAPPER}" "${FAKE_BIN}/launchctl"

run_apply_manager() {
  env \
    HOME="${APPLY_HOME}" \
    PATH="${FAKE_BIN}:${PATH}" \
    COINPILOT_ROOT="${APPLY_HOME}/Library/Application Support/Coinpilot" \
    COINPILOT_REPORT_HOME="${APPLY_HOME}/Library/Application Support/Coinpilot/reporting" \
    COINPILOT_PYTHON="${PYTHON_WRAPPER}" \
    TEST_REAL_PYTHON="${REAL_PYTHON}" \
    TEST_LAUNCH_LOG="${LAUNCH_LOG}" \
    TEST_SERVICE_STATE="${SERVICE_STATE}" \
    TEST_OVERRIDE_STATE="${OVERRIDE_STATE}" \
    TEST_BOOTSTRAP_COUNT="${BOOTSTRAP_COUNT}" \
    TEST_DISABLE_FAIL="${TEST_DISABLE_FAIL:-0}" \
    TEST_ENABLE_FAIL="${TEST_ENABLE_FAIL:-0}" \
    TEST_BOOTOUT_FAIL="${TEST_BOOTOUT_FAIL:-0}" \
    TEST_BOOTSTRAP_FAILS="${TEST_BOOTSTRAP_FAILS:-0}" \
    TEST_BOOTSTRAP_VANISHES="${TEST_BOOTSTRAP_VANISHES:-0}" \
    TEST_REPORTER_STATUS_HEALTHY="${TEST_REPORTER_STATUS_HEALTHY:-0}" \
    "${MANAGER}" "$@"
}

run_apply_manager install --no-start --apply > "${TMP_ROOT}/apply.log"

INSTALLED_HOME="${APPLY_HOME}/Library/Application Support/Coinpilot/reporting"
INSTALLED_HELPER="${INSTALLED_HOME}/bin/coinpilot-daily-scorecard"
INSTALLED_PLIST="${APPLY_HOME}/Library/LaunchAgents/dev.coinpilot.daily-scorecard.plist"
INSTALL_MANIFEST="${INSTALLED_HOME}/state/installed-helper.json"
[[ -x "${INSTALLED_HELPER}" ]]
[[ -f "${INSTALL_MANIFEST}" ]]
[[ -f "${INSTALLED_PLIST}" ]]
python3 - "${INSTALLED_HELPER}" "${INSTALL_MANIFEST}" "${INSTALLED_PLIST}" \
  "${PYTHON_WRAPPER}" "${APPLY_HOME}" <<'PY'
import hashlib
import json
import pathlib
import plistlib
import stat
import sys

helper = pathlib.Path(sys.argv[1])
manifest = json.loads(pathlib.Path(sys.argv[2]).read_text())
plist_path = pathlib.Path(sys.argv[3])
python_executable = sys.argv[4]
home = pathlib.Path(sys.argv[5])
if stat.S_IMODE(helper.stat().st_mode) != 0o755:
    raise SystemExit("installed helper mode mismatch")
if manifest.get("schema_version") != 2:
    raise SystemExit("installed bundle manifest schema mismatch")
if manifest["helper"] != {
    "path": str(helper),
    "sha256": hashlib.sha256(helper.read_bytes()).hexdigest(),
}:
    raise SystemExit("installed helper manifest mismatch")
if manifest["launch_agent"] != {
    "label": "dev.coinpilot.daily-scorecard",
    "path": str(plist_path),
    "sha256": hashlib.sha256(plist_path.read_bytes()).hexdigest(),
}:
    raise SystemExit("installed LaunchAgent manifest mismatch")
plist = plistlib.loads(plist_path.read_bytes())
if plist["Label"] != "dev.coinpilot.daily-scorecard":
    raise SystemExit("installed plist label mismatch")
expected_root = home / "Library/Application Support/Coinpilot"
arguments = plist["ProgramArguments"]
if arguments[:2] != [python_executable, str(helper)]:
    raise SystemExit("installed plist executable binding mismatch")
if arguments[arguments.index("--root") + 1] != str(expected_root):
    raise SystemExit("installed plist root escaped HOME")
if plist["WorkingDirectory"] != str(expected_root / "reporting"):
    raise SystemExit("installed plist reporting home escaped HOME")
if any("ai-coin-trading" in str(item) for item in plist["ProgramArguments"]):
    raise SystemExit("installed plist depends on the repository checkout")
print("OK installed reporter is an immutable isolated copy")
PY

grep -Fq 'disable gui/' "${LAUNCH_LOG}"
grep -Fq '/dev.coinpilot.daily-scorecard' "${LAUNCH_LOG}"
grep -Fq 'bootout gui/' "${LAUNCH_LOG}"
if grep -Eq 'dev\.coinpilot\.((d2|c2)-(btc|eth|xrp|sol)|(btc|eth|xrp|sol))\.' \
    "${LAUNCH_LOG}"; then
  echo "error: central reporter apply targeted a trading service" >&2
  exit 1
fi
if grep -Eq 'enable|bootstrap' "${LAUNCH_LOG}"; then
  echo "error: --no-start enabled or bootstrapped the reporter" >&2
  exit 1
fi
echo "OK --no-start persistently disables only the central reporter"

printf '\n' >> "${INSTALLED_PLIST}"
enable_count_before="$(grep -c '^enable ' "${LAUNCH_LOG}" || true)"
if run_apply_manager start --apply > "${TMP_ROOT}/plist-hash-tamper.log" 2>&1; then
  echo "error: tampered LaunchAgent hash was accepted" >&2
  exit 1
fi
enable_count_after="$(grep -c '^enable ' "${LAUNCH_LOG}" || true)"
[[ "${enable_count_before}" == "${enable_count_after}" ]]
grep -Fq 'manifest mismatch' "${TMP_ROOT}/plist-hash-tamper.log"
[[ "$(< "${SERVICE_STATE}")" == "unloaded" ]]
[[ "$(< "${OVERRIDE_STATE}")" == "disabled" ]]
echo "OK LaunchAgent byte tampering fails closed before bootstrap"

run_apply_manager install --no-start --apply > "${TMP_ROOT}/reinstall.log"
python3 - "${INSTALLED_PLIST}" "${INSTALL_MANIFEST}" <<'PY'
import hashlib
import json
import pathlib
import plistlib
import sys

plist_path = pathlib.Path(sys.argv[1])
manifest_path = pathlib.Path(sys.argv[2])
plist = plistlib.loads(plist_path.read_bytes())
arguments = plist["ProgramArguments"]
arguments[arguments.index("--schedule-hour") + 1] = "1"
plist_path.write_bytes(plistlib.dumps(plist))
manifest = json.loads(manifest_path.read_text())
manifest["launch_agent"]["sha256"] = hashlib.sha256(plist_path.read_bytes()).hexdigest()
manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
PY
chmod 644 "${INSTALLED_PLIST}"
chmod 600 "${INSTALL_MANIFEST}"
if run_apply_manager start --apply > "${TMP_ROOT}/plist-semantic-tamper.log" 2>&1; then
  echo "error: policy-invalid LaunchAgent semantics were accepted" >&2
  exit 1
fi
grep -Fq 'semantics differ from policy' "${TMP_ROOT}/plist-semantic-tamper.log"
[[ "$(< "${SERVICE_STATE}")" == "unloaded" ]]
[[ "$(< "${OVERRIDE_STATE}")" == "disabled" ]]
echo "OK LaunchAgent semantic tampering fails closed even with a matching hash"

run_apply_manager install --no-start --apply > "${TMP_ROOT}/reinstall-valid.log"
printf '0\n' > "${BOOTSTRAP_COUNT}"
TEST_BOOTSTRAP_FAILS=2 run_apply_manager start --apply > "${TMP_ROOT}/bootstrap-retry.log"
[[ "$(< "${BOOTSTRAP_COUNT}")" == "3" ]]
[[ "$(< "${SERVICE_STATE}")" == "loaded" ]]
[[ "$(< "${OVERRIDE_STATE}")" == "enabled" ]]
echo "OK reporter bootstrap retries and verifies the loaded replacement"

if run_apply_manager doctor > "${TMP_ROOT}/doctor-missing-delivery.log" 2>&1; then
  echo "error: doctor accepted a missing latest due delivery" >&2
  exit 1
fi
grep -Fq 'latest due delivery receipt is missing' "${TMP_ROOT}/doctor-missing-delivery.log"

python3 - "${INSTALLED_HOME}/state" "${INSTALLED_HOME}/reports" <<'PY'
import hashlib
import json
import os
import pathlib
import sys
import time
from datetime import datetime, time as datetime_time, timedelta
from zoneinfo import ZoneInfo

state_dir = pathlib.Path(os.path.abspath(sys.argv[1]))
artifact_dir = pathlib.Path(os.path.abspath(sys.argv[2]))
now = datetime.now(ZoneInfo("Asia/Seoul"))
deadline = datetime.combine(now.date(), datetime_time(0, 10), tzinfo=now.tzinfo)
report_date = now.date() - timedelta(days=1 if now >= deadline else 2)
date_text = report_date.isoformat()
report_id = f"daily-scorecard:v1:{date_text}"
stem = f"coinpilot-daily-scorecard-v1-{date_text}"
artifact_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
report = {
    "schema_version": 1,
    "report_id": report_id,
    "report_date": date_text,
    "safety": {"verified": True},
}
json_path = artifact_dir / f"{stem}.json"
markdown_path = artifact_dir / f"{stem}.md"
checksum_path = artifact_dir / f"{stem}.sha256"
json_bytes = (json.dumps(report, sort_keys=True) + "\n").encode()
digest = hashlib.sha256(json_bytes).hexdigest()
json_path.write_bytes(json_bytes)
markdown_path.write_text("# verified test report\n")
checksum_path.write_text(f"{digest}  {json_path.name}\n", encoding="ascii")
delivery_dir = state_dir / "deliveries"
delivery_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
state_path = delivery_dir / f"{date_text}.json"
state = {
    "schema_version": 1,
    "report_id": report_id,
    "report_date": date_text,
    "status": "delivered",
    "delivered_wall_ns": time.time_ns(),
    "last_error": None,
    "artifacts": {
        "json": str(json_path),
        "markdown": str(markdown_path),
        "checksum": str(checksum_path),
        "sha256": digest,
    },
}
state_path.write_text(json.dumps(state, sort_keys=True) + "\n")
for path in (json_path, markdown_path, checksum_path, state_path):
    path.chmod(0o600)
PY

if TEST_REPORTER_STATUS_HEALTHY=0 run_apply_manager doctor \
    > "${TMP_ROOT}/doctor-backlog-unhealthy.log" 2>&1; then
  echo "error: doctor accepted an unresolved delivery backlog" >&2
  exit 1
fi
grep -Fq 'delivery backlog is not healthy' \
  "${TMP_ROOT}/doctor-backlog-unhealthy.log"
echo "OK doctor fails closed while delivery backlog health is false"

TEST_REPORTER_STATUS_HEALTHY=1 run_apply_manager doctor \
  > "${TMP_ROOT}/doctor-healthy.log"
grep -Fq 'latest due report delivered' "${TMP_ROOT}/doctor-healthy.log"
grep -Fq 'delivery backlog healthy through' "${TMP_ROOT}/doctor-healthy.log"
grep -Fq 'Doctor summary: 0 error(s)' "${TMP_ROOT}/doctor-healthy.log"
echo "OK doctor verifies the latest due delivery receipt and artifacts"

printf '0\n' > "${BOOTSTRAP_COUNT}"
TEST_BOOTSTRAP_VANISHES=1 run_apply_manager start --apply \
  > "${TMP_ROOT}/bootstrap-postcheck.log"
[[ "$(< "${BOOTSTRAP_COUNT}")" == "2" ]]
[[ "$(< "${SERVICE_STATE}")" == "loaded" ]]
echo "OK reporter retries when bootstrap does not remain loaded"

printf '0\n' > "${BOOTSTRAP_COUNT}"
if TEST_BOOTSTRAP_FAILS=99 run_apply_manager start --apply \
    > "${TMP_ROOT}/bootstrap-failure.log" 2>&1; then
  echo "error: permanent bootstrap failure was ignored" >&2
  exit 1
fi
[[ "$(< "${BOOTSTRAP_COUNT}")" == "5" ]]
[[ "$(< "${SERVICE_STATE}")" == "unloaded" ]]
[[ "$(< "${OVERRIDE_STATE}")" == "disabled" ]]
grep -Fq 'failed to start dev.coinpilot.daily-scorecard after 5 attempt(s)' \
  "${TMP_ROOT}/bootstrap-failure.log"
echo "OK permanent bootstrap failure rolls back to disabled and unloaded"

if TEST_DISABLE_FAIL=1 run_apply_manager stop --apply \
    > "${TMP_ROOT}/disable-failure.log" 2>&1; then
  echo "error: injected launchctl disable failure was ignored" >&2
  exit 1
fi
grep -Fq 'disable gui/' "${LAUNCH_LOG}"
grep -Fq 'bootout gui/' "${LAUNCH_LOG}"
grep -Fq 'injected disable failure' "${TMP_ROOT}/disable-failure.log"
echo "OK reporter stop still bootouts after persistent-disable failure"

UNINSTALL_HOME="${TMP_ROOT}/uninstall-home"
UNINSTALL_ESCAPE="${TMP_ROOT}/uninstall-escape"
mkdir -p \
  "${UNINSTALL_HOME}/Library/Application Support/Coinpilot" \
  "${UNINSTALL_HOME}/Library/LaunchAgents" \
  "${UNINSTALL_ESCAPE}/bin" \
  "${UNINSTALL_ESCAPE}/state" \
  "${UNINSTALL_ESCAPE}/launchd"
ln -s "${UNINSTALL_ESCAPE}" \
  "${UNINSTALL_HOME}/Library/Application Support/Coinpilot/reporting"
touch \
  "${UNINSTALL_ESCAPE}/bin/coinpilot-daily-scorecard" \
  "${UNINSTALL_ESCAPE}/state/installed-helper.json" \
  "${UNINSTALL_ESCAPE}/launchd/dev.coinpilot.daily-scorecard.plist" \
  "${UNINSTALL_HOME}/Library/LaunchAgents/dev.coinpilot.daily-scorecard.plist"
launch_count_before="$(wc -l < "${LAUNCH_LOG}" | tr -d ' ')"
if env \
    HOME="${UNINSTALL_HOME}" \
    PATH="${FAKE_BIN}:${PATH}" \
    COINPILOT_PYTHON="${PYTHON_WRAPPER}" \
    TEST_REAL_PYTHON="${REAL_PYTHON}" \
    TEST_LAUNCH_LOG="${LAUNCH_LOG}" \
    TEST_SERVICE_STATE="${SERVICE_STATE}" \
    TEST_OVERRIDE_STATE="${OVERRIDE_STATE}" \
    TEST_BOOTSTRAP_COUNT="${BOOTSTRAP_COUNT}" \
    "${MANAGER}" uninstall --apply \
    > "${TMP_ROOT}/uninstall-symlink.log" 2>&1; then
  echo "error: uninstall accepted a symlinked reporting boundary" >&2
  exit 1
fi
launch_count_after="$(wc -l < "${LAUNCH_LOG}" | tr -d ' ')"
[[ "${launch_count_before}" == "${launch_count_after}" ]]
grep -Fq 'symlinked managed path is not allowed' \
  "${TMP_ROOT}/uninstall-symlink.log"
[[ -f "${UNINSTALL_ESCAPE}/bin/coinpilot-daily-scorecard" ]]
[[ -f "${UNINSTALL_ESCAPE}/state/installed-helper.json" ]]
[[ -f "${UNINSTALL_HOME}/Library/LaunchAgents/dev.coinpilot.daily-scorecard.plist" ]]
echo "OK uninstall rejects path drift before launchd or filesystem mutation"

if grep -R -E 'hooks\.slack(-gov)?\.com/services/[A-Z0-9]{6,}/[A-Z0-9]{6,}/[A-Za-z0-9]{12,}' \
  "${REPO_ROOT}/ops/reporting" "${REPORTER}" "${MANAGER}" >/dev/null 2>&1; then
  echo "error: webhook-shaped value appears in reporting sources" >&2
  exit 1
fi
if grep -Eqi 'COINPILOT_ENABLE_(SHADOW|PAPER)=1' "${TEMPLATE}"; then
  echo "error: reporting plist can enable a trading service" >&2
  exit 1
fi
echo "OK daily scorecard contains no secret or trading activation"
