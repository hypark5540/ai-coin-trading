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

export PYTHONUNBUFFERED=1
export COINPILOT_LIVE_TRADING=0
export COINPILOT_MODE=shadow
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
    exec "${COINPILOT_BIN}" --config "${CONFIG_PATH}" shadow-run
    ;;
  notifier)
    exec "${COINPILOT_BIN}" --config "${CONFIG_PATH}" shadow-notify
    ;;
  web)
    exec "${COINPILOT_BIN}" --config "${CONFIG_PATH}" \
      shadow-web --host "${WEB_HOST}" --port "${WEB_PORT}"
    ;;
  external-watchdog)
    exec "${COINPILOT_BIN}" --config "${CONFIG_PATH}" shadow-watchdog --once
    ;;
  *)
    echo "usage: coinpilot-service {shadow|notifier|web|external-watchdog}" >&2
    exit 64
    ;;
esac
