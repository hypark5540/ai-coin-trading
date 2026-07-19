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
if len(templates) != 6:
    raise SystemExit(f"expected 6 launchd templates, found {len(templates)}")
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

HOME="${TMP_ROOT}/home" "${REPO_ROOT}/scripts/mac-studio-backup" \
  --home "${TMP_ROOT}/app" > "${TMP_ROOT}/backup-dry-run.log"
grep -q 'dry-run' "${TMP_ROOT}/backup-dry-run.log"
HOME="${TMP_ROOT}/home" "${REPO_ROOT}/scripts/mac-studio-retention" \
  --home "${TMP_ROOT}/app" > "${TMP_ROOT}/retention-dry-run.log"
grep -q 'Dry-run only' "${TMP_ROOT}/retention-dry-run.log"
echo "OK maintenance dry-runs"

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
