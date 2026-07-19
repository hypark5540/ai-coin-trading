# Security model

CoinPilot is a research and simulated-trading project. The repository contains
no exchange order client and the `live` command refuses to trade.

## Shadow deployment boundary

- Use only Upbit's public market-data endpoints.
- Do not install Upbit API keys on the shadow host.
- Keep `shadow.mode = "observe"` until the diagnostic simulator is explicitly
  reviewed and enabled.
- Bind the status server only to `127.0.0.1`. Use an SSH tunnel or a private
  VPN ACL for remote viewing.
- Store the Slack incoming-webhook URL in macOS Keychain. Never put it in TOML,
  environment files, launchd plists, command-line arguments, or logs.
- Retention only operates on owner-controlled directories carrying the
  installer-created managed marker and fails closed outside those boundaries.
- Treat `var/`, `artifacts/`, local configuration, databases, archives, and
  backups as private runtime data. They are excluded from Git.

Every shadow artifact and alert must preserve `simulated=true`,
`own_execution=false`, and `orders_sent=0`. Any other value is a critical
invariant violation and must halt new simulated decisions.

## Reporting a vulnerability

Do not open a public issue containing credentials, webhook URLs, private
market data, or host details. Revoke an exposed credential first, then contact
the repository owner privately through GitHub.
