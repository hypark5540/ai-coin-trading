#!/usr/bin/env bash
#
# Stable launchd entrypoint. The Python notifier reads Keychain directly;
# this wrapper never reads or exports the Slack webhook.

set -euo pipefail
IFS=$'\n\t'
umask 077

SERVICE="${1:-}"
APP_HOME="${COINPILOT_HOME:?COINPILOT_HOME is required}"
CONFIG_PATH="${COINPILOT_CONFIG:-${APP_HOME}/config/config.toml}"
RUNTIME_ENV="${COINPILOT_RUNTIME_ENV:-${APP_HOME}/config/runtime.env}"
VENV_DIR="${APP_HOME}/venv"
COINPILOT_BIN="${VENV_DIR}/bin/coinpilot"
HOURLY_NOTIFIER="${APP_HOME}/bin/coinpilot-hourly-notifier"
BOUNDED_SHADOW="${APP_HOME}/bin/coinpilot-bounded-shadow"
C2_CONFIG_GUARD="${APP_HOME}/bin/coinpilot-c2-config"
PAPER_MANIFEST_PATH="${APP_HOME}/config/paper.manifest.json"

export PYTHONUNBUFFERED=1
export COINPILOT_LIVE_TRADING=0
export COINPILOT_CONFIG="${CONFIG_PATH}"

runtime_value() {
  local key="$1"
  local fallback="$2"
  local value=""
  if [[ -f "${RUNTIME_ENV}" ]]; then
    value="$(awk -F= -v wanted="${key}" '
      $0 !~ /^[[:space:]]*#/ && $1 == wanted {
        sub(/^[^=]*=/, "")
        print
        exit
      }
    ' "${RUNTIME_ENV}")"
  fi
  if [[ -n "${value}" ]]; then printf '%s' "${value}"; else printf '%s' "${fallback}"; fi
}

[[ -x "${COINPILOT_BIN}" ]] || {
  echo "coinpilot executable is missing: ${COINPILOT_BIN}" >&2
  exit 78
}
[[ -f "${CONFIG_PATH}" ]] || {
  echo "coinpilot config is missing: ${CONFIG_PATH}" >&2
  exit 78
}

WEB_HOST="$(runtime_value COINPILOT_WEB_HOST 127.0.0.1)"
WEB_PORT="$(runtime_value COINPILOT_WEB_PORT 8765)"
SLACK_SUMMARY_SECONDS="$(runtime_value COINPILOT_SLACK_SUMMARY_SECONDS 3600)"
SLACK_SUMMARY_GRACE_SECONDS="$(runtime_value COINPILOT_SLACK_SUMMARY_GRACE_SECONDS 15)"
BOUNDED_SHADOW_ENABLED="$(runtime_value COINPILOT_BOUNDED_SHADOW 0)"
SHADOW_COOLDOWN_SECONDS="$(runtime_value COINPILOT_SHADOW_COOLDOWN_SECONDS 3600)"
SHADOW_MAX_ROUND_TRIPS="$(runtime_value COINPILOT_SHADOW_MAX_ROUND_TRIPS_PER_DAY 24)"
SHADOW_RESERVE_FULL_ORDER_LOSS="$(runtime_value COINPILOT_SHADOW_RESERVE_FULL_ORDER_LOSS 1)"
SHADOW_EXECUTION_SPREAD_RECHECK="$(runtime_value COINPILOT_SHADOW_EXECUTION_SPREAD_RECHECK 1)"
PAPER_ENABLED="$(runtime_value COINPILOT_ENABLE_PAPER 0)"
PAPER_CONFIG_PATH="${APP_HOME}/config/paper.toml"

if [[ "${WEB_HOST}" != "127.0.0.1" ]]; then
  echo "refusing non-loopback dashboard host: ${WEB_HOST}" >&2
  exit 78
fi
case "${WEB_PORT}" in
  ''|*[!0-9]*)
    echo "invalid dashboard port: ${WEB_PORT}" >&2
    exit 78
    ;;
esac

case "${SERVICE}" in
  shadow)
    export COINPILOT_MODE=shadow
    SHADOW_MODEL_VERSION="$("${VENV_DIR}/bin/python" - "${CONFIG_PATH}" <<'PY'
import pathlib
import sys
import tomllib

config = tomllib.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
print(str(config.get("shadow", {}).get("model_version", "")))
PY
)"
    case "${BOUNDED_SHADOW_ENABLED}" in
      1|true|TRUE|yes|YES|on|ON)
        [[ -x "${BOUNDED_SHADOW}" ]] || {
          echo "bounded shadow helper is missing: ${BOUNDED_SHADOW}" >&2
          exit 78
        }
        BOUNDED_SHADOW_SHA="$(/usr/bin/shasum -a 256 "${BOUNDED_SHADOW}" | awk '{print $1}')"
        SERVICE_WRAPPER_SHA="$(/usr/bin/shasum -a 256 "$0" | awk '{print $1}')"
        export COINPILOT_CODE_VERSION="bounded-shadow-v1:${SERVICE_WRAPPER_SHA}:${BOUNDED_SHADOW_SHA}:${SHADOW_COOLDOWN_SECONDS}:${SHADOW_MAX_ROUND_TRIPS}:${SHADOW_RESERVE_FULL_ORDER_LOSS}:${SHADOW_EXECUTION_SPREAD_RECHECK}"
        exec "${VENV_DIR}/bin/python" "${BOUNDED_SHADOW}" \
          --config "${CONFIG_PATH}" \
          --cooldown-seconds "${SHADOW_COOLDOWN_SECONDS}" \
          --max-round-trips-per-day "${SHADOW_MAX_ROUND_TRIPS}" \
          --reserve-full-order-loss "${SHADOW_RESERVE_FULL_ORDER_LOSS}" \
          --execution-spread-recheck "${SHADOW_EXECUTION_SPREAD_RECHECK}" \
          --audit-dir "${APP_HOME}/state"
        ;;
      0|false|FALSE|no|NO|off|OFF)
        if [[ "${SHADOW_MODEL_VERSION}" == diagnostic-bounded-* ]]; then
          echo "refusing bounded model without bounded shadow helper" >&2
          exit 78
        fi
        ;;
      *)
        echo "invalid COINPILOT_BOUNDED_SHADOW value" >&2
        exit 78
        ;;
    esac
    exec "${COINPILOT_BIN}" --config "${CONFIG_PATH}" shadow-run
    ;;
  paper)
    case "${PAPER_ENABLED}" in
      1|true|TRUE|yes|YES|on|ON) ;;
      *)
        echo "paper service is disabled by COINPILOT_ENABLE_PAPER" >&2
        exit 78
        ;;
    esac
    [[ -f "${PAPER_CONFIG_PATH}" ]] || {
      echo "paper config is missing: ${PAPER_CONFIG_PATH}" >&2
      exit 78
    }
    [[ -f "${PAPER_MANIFEST_PATH}" ]] || {
      echo "paper manifest is missing: ${PAPER_MANIFEST_PATH}" >&2
      exit 78
    }
    [[ -x "${C2_CONFIG_GUARD}" ]] || {
      echo "paper runtime guard is missing: ${C2_CONFIG_GUARD}" >&2
      exit 78
    }
    INSTALLED_PACKAGE_ROOT="$("${VENV_DIR}/bin/python" - <<'PY'
import importlib.util
import pathlib

spec = importlib.util.find_spec("coinpilot")
if spec is None or not spec.submodule_search_locations:
    raise SystemExit("installed coinpilot package is unavailable")
print(pathlib.Path(next(iter(spec.submodule_search_locations))).resolve())
PY
)"
    "${VENV_DIR}/bin/python" "${C2_CONFIG_GUARD}" verify-installed \
      --config "${PAPER_CONFIG_PATH}" \
      --manifest "${PAPER_MANIFEST_PATH}" \
      --installed-package-root "${INSTALLED_PACKAGE_ROOT}" >/dev/null
    export COINPILOT_MODE=paper
    export COINPILOT_CONFIG="${PAPER_CONFIG_PATH}"
    exec "${COINPILOT_BIN}" --config "${PAPER_CONFIG_PATH}" paper --loop
    ;;
  notifier)
    export COINPILOT_MODE=shadow
    [[ -x "${HOURLY_NOTIFIER}" ]] || {
      echo "hourly notifier is missing: ${HOURLY_NOTIFIER}" >&2
      exit 78
    }
    exec "${VENV_DIR}/bin/python" "${HOURLY_NOTIFIER}" \
      --config "${CONFIG_PATH}" \
      --summary-seconds "${SLACK_SUMMARY_SECONDS}" \
      --grace-seconds "${SLACK_SUMMARY_GRACE_SECONDS}"
    ;;
  web)
    export COINPILOT_MODE=shadow
    exec "${COINPILOT_BIN}" --config "${CONFIG_PATH}" \
      shadow-web --host "${WEB_HOST}" --port "${WEB_PORT}"
    ;;
  external-watchdog)
    export COINPILOT_MODE=shadow
    exec "${COINPILOT_BIN}" --config "${CONFIG_PATH}" shadow-watchdog --once
    ;;
  *)
    echo "usage: coinpilot-service {shadow|paper|notifier|web|external-watchdog}" >&2
    exit 64
    ;;
esac
