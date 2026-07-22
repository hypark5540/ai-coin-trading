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
- The central daily scorecard may open only the eight explicitly approved D2/C2
  SQLite paths with `mode=ro`, `query_only`, and one read transaction per
  ledger. It must verify the current installed config/code identity against the
  selected ledger/account before declaring PASS. Delivery state and report
  artifacts belong exactly in
  `~/Library/Application Support/Coinpilot/reporting`; never reuse a trading
  ledger or its notification outbox for report delivery. An incomplete source
  report may alert and retry, but must not be frozen as the final daily result.
  Older installed runtimes do not always persist explicit `simulated` and
  `own_execution` fields. Their absence is accepted only when the immutable
  installed simulation-only/public-only code and config identity, disabled live
  routing, and zero sent orders all verify; any emitted contradictory value
  fails closed.
- Retention only operates on owner-controlled directories carrying the
  installer-created managed marker and fails closed outside those boundaries.
- Treat `var/`, `artifacts/`, local configuration, databases, archives, and
  backups as private runtime data. Daily scorecard JSON/Markdown and delivery
  receipts are private runtime data too. They are excluded from Git and stored
  owner-only.

Every shadow artifact and alert must preserve `simulated=true`,
`own_execution=false`, `live_order_routing=false`, and `orders_sent=0`. Any
other value is a critical invariant violation and must halt new simulated
decisions.

Automated reporting may rank observations and propose offline research, but it
must never change an active strategy, rearm a halted account, reuse a ledger
after fingerprint drift, or promote a candidate without explicit review.

## Reporting a vulnerability

Do not open a public issue containing credentials, webhook URLs, private
market data, or host details. Revoke an exposed credential first, then contact
the repository owner privately through GitHub.
