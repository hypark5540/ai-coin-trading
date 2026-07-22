"""Self-contained localhost dashboard for the shadow trading runtime."""

from __future__ import annotations

import html
import json
import re


def dashboard_html(market: str) -> bytes:
    """Return the dependency-free, read-only operations console."""

    normalized_market = market.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]+-[A-Z0-9]+", normalized_market):
        raise ValueError("market must look like KRW-BTC")
    quote_currency, base_currency = normalized_market.split("-", 1)

    content = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="color-scheme" content="dark">
  <title>CoinPilot · @@MARKET_TEXT@@ Shadow Operations</title>
  <style>
    :root {
      --ink: #eef0ea;
      --ink-soft: #c5cbc5;
      --muted: #8e9892;
      --muted-2: #68716c;
      --void: #090c0b;
      --surface: #111513;
      --surface-raised: #151a17;
      --surface-soft: #1a201c;
      --line: rgba(222, 229, 220, .105);
      --line-strong: rgba(222, 229, 220, .18);
      --copper: #caa56a;
      --copper-soft: rgba(202, 165, 106, .12);
      --moss: #83b89a;
      --moss-soft: rgba(131, 184, 154, .13);
      --ochre: #d1a157;
      --ochre-soft: rgba(209, 161, 87, .12);
      --coral: #d97870;
      --coral-soft: rgba(217, 120, 112, .12);
      --shadow: 0 24px 70px rgba(0, 0, 0, .28);
      --radius-lg: 22px;
      --radius-md: 15px;
      --sans: -apple-system, BlinkMacSystemFont, "Segoe UI",
        "Apple SD Gothic Neo", sans-serif;
      --mono: ui-monospace, "SFMono-Regular", Menlo, Monaco, Consolas,
        monospace;
    }

    * { box-sizing: border-box; }

    html { min-width: 320px; background: var(--void); }

    body {
      min-height: 100vh;
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at 12% -10%, rgba(202, 165, 106, .09), transparent 32rem),
        radial-gradient(circle at 88% 7%, rgba(131, 184, 154, .07), transparent 28rem),
        var(--void);
      font-family: var(--sans);
      font-size: 14px;
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }

    body::before {
      position: fixed;
      inset: 0;
      z-index: -1;
      pointer-events: none;
      content: "";
      opacity: .2;
      background-image:
        linear-gradient(rgba(255, 255, 255, .018) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255, 255, 255, .018) 1px, transparent 1px);
      background-size: 48px 48px;
      mask-image: linear-gradient(to bottom, black, transparent 75%);
    }

    button, summary { font: inherit; }
    button { color: inherit; }

    button:focus-visible,
    summary:focus-visible {
      outline: 2px solid var(--copper);
      outline-offset: 3px;
    }

    .topbar {
      position: sticky;
      top: 0;
      z-index: 20;
      border-bottom: 1px solid var(--line);
      background: rgba(9, 12, 11, .94);
    }

    .topbar__inner {
      width: min(1480px, calc(100% - 48px));
      min-height: 68px;
      margin: 0 auto;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 24px;
    }

    .brand,
    .brand__copy,
    .topbar__status,
    .connection,
    .panel__heading,
    .safety__item,
    .metric__header,
    .runtime-row,
    .health-row__top,
    .chart-legend,
    .footer {
      display: flex;
      align-items: center;
    }

    .brand { gap: 12px; min-width: 0; }

    .brand__mark {
      width: 34px;
      height: 34px;
      flex: 0 0 auto;
      display: grid;
      place-items: center;
      border: 1px solid rgba(202, 165, 106, .48);
      border-radius: 10px;
      color: var(--copper);
      background: var(--copper-soft);
      font: 700 11px/1 var(--mono);
      letter-spacing: .08em;
      box-shadow: inset 0 0 0 1px rgba(0, 0, 0, .25);
    }

    .brand__copy { min-width: 0; gap: 10px; }
    .brand__name { font-weight: 650; letter-spacing: -.015em; }
    .brand__divider { width: 1px; height: 16px; background: var(--line-strong); }
    .brand__desk {
      color: var(--muted);
      font-size: 11px;
      font-weight: 650;
      letter-spacing: .13em;
      text-transform: uppercase;
    }

    .topbar__status { justify-content: flex-end; gap: 10px; }

    .connection {
      gap: 8px;
      min-height: 30px;
      padding: 0 11px;
      border: 1px solid var(--line);
      border-radius: 999px;
      color: var(--ink-soft);
      background: rgba(255, 255, 255, .025);
      font: 650 10px/1 var(--mono);
      letter-spacing: .08em;
      text-transform: uppercase;
    }

    .connection__dot,
    .health-dot {
      width: 7px;
      height: 7px;
      flex: 0 0 auto;
      border-radius: 50%;
      background: var(--muted-2);
      box-shadow: 0 0 0 4px rgba(104, 113, 108, .1);
    }

    .connection[data-state="live"] .connection__dot,
    .health-dot[data-state="ok"] {
      background: var(--moss);
      box-shadow: 0 0 0 4px var(--moss-soft);
    }

    .connection[data-state="live"] .connection__dot {
      animation: live-pulse 2.2s ease-out infinite;
    }

    .connection[data-state="syncing"] .connection__dot,
    .health-dot[data-state="warning"] {
      background: var(--ochre);
      box-shadow: 0 0 0 4px var(--ochre-soft);
    }

    .connection[data-state="stale"] .connection__dot,
    .health-dot[data-state="error"] {
      background: var(--coral);
      box-shadow: 0 0 0 4px var(--coral-soft);
    }

    .clock {
      min-width: 68px;
      color: var(--muted);
      font: 500 11px/1 var(--mono);
      text-align: right;
    }

    .icon-button {
      width: 32px;
      height: 32px;
      display: grid;
      place-items: center;
      border: 1px solid var(--line);
      border-radius: 9px;
      color: var(--muted);
      background: transparent;
      cursor: pointer;
      transition: color .18s ease, border-color .18s ease, background .18s ease;
    }

    .icon-button:hover {
      color: var(--ink);
      border-color: var(--line-strong);
      background: rgba(255, 255, 255, .035);
    }

    .icon-button[disabled] { cursor: wait; opacity: .55; }
    .icon-button svg { width: 15px; height: 15px; }
    .icon-button.is-spinning svg { animation: spin .9s linear infinite; }

    .refresh-track {
      height: 1px;
      overflow: hidden;
      background: transparent;
    }

    .refresh-track::after {
      display: block;
      width: 100%;
      height: 100%;
      content: "";
      background: linear-gradient(90deg, transparent, var(--copper), transparent);
      transform: translateX(-100%);
    }

    .refresh-track.is-active::after { animation: refresh-track 5s linear; }

    .shell {
      width: min(1480px, calc(100% - 48px));
      margin: 0 auto;
      padding: 28px 0 42px;
    }

    .eyebrow {
      margin: 0;
      color: var(--muted);
      font-size: 10px;
      font-weight: 700;
      letter-spacing: .14em;
      text-transform: uppercase;
    }

    .safety {
      min-height: 50px;
      margin-bottom: 18px;
      padding: 10px 14px;
      display: grid;
      grid-template-columns: minmax(180px, 1.25fr) repeat(3, minmax(130px, auto));
      align-items: center;
      gap: 10px;
      border: 1px solid rgba(202, 165, 106, .22);
      border-radius: var(--radius-md);
      background:
        linear-gradient(90deg, var(--copper-soft), transparent 42%),
        var(--surface);
      box-shadow: 0 12px 35px rgba(0, 0, 0, .14);
    }

    .safety__lead {
      display: flex;
      align-items: center;
      gap: 11px;
      min-width: 0;
    }

    .safety__seal {
      width: 28px;
      height: 28px;
      flex: 0 0 auto;
      display: grid;
      place-items: center;
      border-radius: 50%;
      color: var(--copper);
      background: var(--copper-soft);
    }

    .safety__seal svg { width: 14px; height: 14px; }
    .safety__title { font-weight: 650; }
    .safety__subtitle { color: var(--muted); font-size: 11px; }

    .safety__item {
      min-height: 28px;
      justify-content: center;
      gap: 7px;
      padding: 0 10px;
      border-left: 1px solid var(--line);
      color: var(--ink-soft);
      font: 600 10px/1 var(--mono);
      letter-spacing: .05em;
      text-transform: uppercase;
      white-space: nowrap;
    }

    .safety__item strong { color: var(--moss); font-size: 11px; }

    .hero-grid,
    .lower-grid {
      display: grid;
      grid-template-columns: minmax(0, 1.9fr) minmax(300px, .8fr);
      gap: 18px;
    }

    .panel {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: var(--radius-lg);
      background: linear-gradient(145deg, rgba(255, 255, 255, .018), transparent 45%), var(--surface);
      box-shadow: var(--shadow);
    }

    .hero-panel { min-height: 505px; padding: 24px 26px 20px; }
    .health-panel { min-height: 505px; padding: 22px; }

    .panel__heading {
      min-height: 26px;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 22px;
    }

    .panel__title {
      margin: 2px 0 0;
      font-size: 16px;
      font-weight: 620;
      letter-spacing: -.015em;
    }

    .market-chip,
    .mode-chip,
    .status-chip,
    .table-chip {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      font: 700 10px/1 var(--mono);
      letter-spacing: .07em;
      text-transform: uppercase;
      white-space: nowrap;
    }

    .market-chip {
      min-height: 28px;
      padding: 0 11px;
      border: 1px solid var(--line-strong);
      color: var(--ink-soft);
      background: rgba(255, 255, 255, .025);
    }

    .mode-chip {
      min-height: 26px;
      padding: 0 10px;
      color: var(--copper);
      background: var(--copper-soft);
    }

    .hero-value-row {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 24px;
      margin-bottom: 22px;
    }

    .hero-value {
      margin: 5px 0 0;
      font: 600 clamp(40px, 5vw, 66px)/.98 var(--sans);
      letter-spacing: -.055em;
      font-variant-numeric: tabular-nums;
    }

    .hero-delta {
      margin-top: 11px;
      display: flex;
      align-items: baseline;
      gap: 8px;
      color: var(--muted);
      font-size: 12px;
    }

    .hero-delta strong {
      color: var(--ink-soft);
      font: 650 12px/1 var(--mono);
    }

    .lifecycle {
      padding-bottom: 6px;
      text-align: right;
    }

    .status-chip {
      min-height: 30px;
      padding: 0 12px;
      color: var(--moss);
      background: var(--moss-soft);
    }

    .status-chip[data-state="warning"] {
      color: var(--ochre);
      background: var(--ochre-soft);
    }

    .status-chip[data-state="error"] {
      color: var(--coral);
      background: var(--coral-soft);
    }

    .lifecycle__meta {
      margin-top: 8px;
      color: var(--muted);
      font: 500 10px/1.3 var(--mono);
    }

    .chart-shell {
      position: relative;
      height: 265px;
      overflow: hidden;
      border: 1px solid var(--line);
      border-radius: 16px;
      background:
        linear-gradient(to bottom, rgba(255, 255, 255, .018), transparent),
        #0d110f;
    }

    .chart-shell::after {
      position: absolute;
      inset: 0;
      pointer-events: none;
      content: "";
      border-radius: inherit;
      box-shadow: inset 0 1px rgba(255, 255, 255, .025);
    }

    #equity-chart { width: 100%; height: 100%; display: block; }
    .chart-grid { stroke: rgba(222, 229, 220, .065); stroke-width: 1; }
    .chart-equity-fill { fill: rgba(131, 184, 154, .09); }
    .chart-equity-line {
      fill: none;
      stroke: var(--moss);
      stroke-width: 2.2;
      stroke-linecap: round;
      stroke-linejoin: round;
      vector-effect: non-scaling-stroke;
    }

    .chart-cash-line {
      fill: none;
      stroke: rgba(202, 165, 106, .78);
      stroke-width: 1.2;
      stroke-dasharray: 5 5;
      vector-effect: non-scaling-stroke;
    }

    .chart-endpoint {
      fill: var(--surface);
      stroke: var(--moss);
      stroke-width: 2;
      vector-effect: non-scaling-stroke;
    }

    .chart-label {
      fill: var(--muted-2);
      font: 9px var(--mono);
      letter-spacing: .02em;
    }

    .chart-empty {
      position: absolute;
      inset: 0;
      display: grid;
      place-items: center;
      color: var(--muted);
      font-size: 12px;
      text-align: center;
    }

    .chart-empty[hidden] { display: none; }

    .chart-meta {
      margin-top: 12px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }

    .chart-legend { gap: 16px; color: var(--muted); font-size: 11px; }
    .legend-item { display: inline-flex; align-items: center; gap: 6px; }
    .legend-line { width: 18px; height: 2px; border-radius: 999px; background: var(--moss); }
    .legend-line--cash {
      height: 1px;
      background: repeating-linear-gradient(90deg, var(--copper) 0 4px, transparent 4px 7px);
    }

    .chart-window {
      color: var(--muted-2);
      font: 500 10px/1 var(--mono);
    }

    .health-summary {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      margin-bottom: 18px;
    }

    .health-summary__item {
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 13px;
      background: rgba(255, 255, 255, .018);
    }

    .health-summary__value {
      margin-top: 7px;
      font: 650 20px/1.1 var(--mono);
      font-variant-numeric: tabular-nums;
    }

    .health-list { display: grid; gap: 9px; }

    .health-row {
      padding: 13px 14px;
      border: 1px solid var(--line);
      border-radius: 13px;
      background: rgba(255, 255, 255, .018);
    }

    .health-row__top { justify-content: space-between; gap: 10px; }
    .health-row__name-wrap { min-width: 0; display: flex; align-items: center; gap: 10px; }
    .health-row__name { overflow: hidden; font-weight: 610; text-overflow: ellipsis; white-space: nowrap; }
    .health-row__state {
      color: var(--moss);
      font: 700 10px/1 var(--mono);
      letter-spacing: .07em;
      text-transform: uppercase;
    }

    .health-row__state[data-state="warning"] { color: var(--ochre); }
    .health-row__state[data-state="error"] { color: var(--coral); }
    .health-row__detail { margin: 8px 0 0 17px; color: var(--muted); font-size: 11px; }

    .health-empty {
      padding: 24px 12px;
      color: var(--muted);
      text-align: center;
    }

    .metrics {
      margin: 18px 0;
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
    }

    .metric {
      min-width: 0;
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: var(--radius-md);
      background: var(--surface);
      box-shadow: 0 12px 30px rgba(0, 0, 0, .12);
    }

    .metric__header { justify-content: space-between; gap: 8px; }
    .metric__index { color: var(--muted-2); font: 500 9px/1 var(--mono); }

    .metric__value {
      margin-top: 13px;
      overflow: hidden;
      color: var(--ink);
      font: 600 clamp(18px, 2vw, 25px)/1.05 var(--mono);
      letter-spacing: -.035em;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }

    .metric__note {
      min-height: 17px;
      margin-top: 9px;
      overflow: hidden;
      color: var(--muted);
      font-size: 11px;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .lower-grid { grid-template-columns: minmax(0, 1.65fr) minmax(300px, .85fr); }
    .activity-panel, .runtime-panel { padding: 22px; }

    .activity-stats { display: flex; gap: 18px; color: var(--muted); font-size: 11px; }
    .activity-stats strong { color: var(--ink-soft); font: 650 11px/1 var(--mono); }

    .table-wrap {
      min-height: 235px;
      overflow-x: auto;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(0, 0, 0, .08);
    }

    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th {
      padding: 11px 13px;
      color: var(--muted-2);
      font-size: 9px;
      font-weight: 700;
      letter-spacing: .09em;
      text-align: left;
      text-transform: uppercase;
      white-space: nowrap;
    }

    td {
      padding: 13px;
      border-top: 1px solid var(--line);
      color: var(--ink-soft);
      font-family: var(--mono);
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }

    .table-chip { min-height: 22px; padding: 0 8px; }
    .table-chip[data-side="buy"] { color: var(--moss); background: var(--moss-soft); }
    .table-chip[data-side="sell"] { color: var(--coral); background: var(--coral-soft); }

    .empty-state {
      min-height: 235px;
      padding: 28px;
      display: grid;
      place-content: center;
      color: var(--muted);
      text-align: center;
    }

    .empty-state__icon {
      width: 44px;
      height: 44px;
      margin: 0 auto 13px;
      display: grid;
      place-items: center;
      border: 1px solid var(--line);
      border-radius: 14px;
      color: var(--copper);
      background: var(--copper-soft);
    }

    .empty-state__icon svg { width: 19px; height: 19px; }
    .empty-state strong { color: var(--ink-soft); font-weight: 620; }
    .empty-state p { max-width: 330px; margin: 6px auto 0; font-size: 12px; }

    .runtime-list { display: grid; }

    .runtime-row {
      min-height: 47px;
      justify-content: space-between;
      gap: 16px;
      border-bottom: 1px solid var(--line);
    }

    .runtime-row:last-child { border-bottom: 0; }
    .runtime-row__label { color: var(--muted); font-size: 11px; }
    .runtime-row__value {
      max-width: 62%;
      overflow: hidden;
      color: var(--ink-soft);
      font: 550 11px/1.25 var(--mono);
      text-align: right;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .raw-panel {
      margin-top: 18px;
      border: 1px solid var(--line);
      border-radius: var(--radius-md);
      background: rgba(17, 21, 19, .72);
    }

    .raw-panel summary {
      min-height: 48px;
      padding: 0 16px;
      color: var(--muted);
      cursor: pointer;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: .04em;
      list-style: none;
    }

    .raw-panel summary::-webkit-details-marker { display: none; }
    .raw-panel summary::before { margin-right: 9px; content: "+"; color: var(--copper); font: 600 13px var(--mono); }
    .raw-panel[open] summary::before { content: "−"; }

    .raw-panel pre {
      max-height: 360px;
      margin: 0;
      padding: 0 16px 16px;
      overflow: auto;
      color: var(--muted);
      font: 10px/1.6 var(--mono);
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }

    .footer {
      min-height: 48px;
      justify-content: space-between;
      gap: 18px;
      color: var(--muted-2);
      font-size: 10px;
    }

    .footer__safety { color: var(--moss); font: 650 10px/1 var(--mono); }

    .skeleton {
      color: transparent !important;
      border-radius: 5px;
      background: linear-gradient(100deg, rgba(255, 255, 255, .035) 25%, rgba(255, 255, 255, .07) 40%, rgba(255, 255, 255, .035) 58%);
      background-size: 220% 100%;
      animation: shimmer 1.5s ease-in-out infinite;
    }

    .tone-positive { color: var(--moss) !important; }
    .tone-negative { color: var(--coral) !important; }
    .tone-warning { color: var(--ochre) !important; }

    @keyframes live-pulse {
      0% { box-shadow: 0 0 0 0 rgba(131, 184, 154, .42); }
      65%, 100% { box-shadow: 0 0 0 7px rgba(131, 184, 154, 0); }
    }

    @keyframes spin { to { transform: rotate(360deg); } }
    @keyframes refresh-track { to { transform: translateX(100%); } }
    @keyframes shimmer { to { background-position-x: -220%; } }

    @media (max-width: 1080px) {
      .hero-grid, .lower-grid { grid-template-columns: 1fr; }
      .hero-panel, .health-panel { min-height: auto; }
      .health-summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .metrics { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .safety { grid-template-columns: 1fr repeat(3, auto); }
    }

    @media (max-width: 760px) {
      .topbar__inner, .shell { width: min(100% - 28px, 1480px); }
      .topbar__inner { min-height: 62px; }
      .brand__desk, .brand__divider, .clock { display: none; }
      .topbar__status { gap: 7px; }
      .connection { padding: 0 9px; }
      .safety { grid-template-columns: 1fr 1fr; }
      .safety__lead { grid-column: 1 / -1; padding-bottom: 7px; }
      .safety__item { border-left: 0; border-top: 1px solid var(--line); padding-top: 9px; }
      .safety__item:last-child { grid-column: 1 / -1; }
      .hero-panel, .health-panel, .activity-panel, .runtime-panel { padding: 18px; }
      .hero-value-row { align-items: flex-start; flex-direction: column; gap: 12px; }
      .lifecycle { padding: 0; text-align: left; }
      .hero-value { font-size: clamp(37px, 11vw, 52px); }
      .chart-shell { height: 235px; }
      .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .metric:last-child { grid-column: 1 / -1; }
      .chart-meta { align-items: flex-start; flex-direction: column; gap: 8px; }
      .footer { align-items: flex-start; flex-direction: column; padding: 14px 0; gap: 6px; }
    }

    @media (max-width: 520px) {
      .brand__name { font-size: 13px; }
      .connection__label { display: none; }
      .connection { min-width: 30px; padding: 0 10px; }
      .shell { padding-top: 18px; }
      .safety { grid-template-columns: 1fr; }
      .safety__lead, .safety__item:last-child { grid-column: auto; }
      .safety__item { justify-content: flex-start; }
      .metrics { grid-template-columns: 1fr; }
      .metric:last-child { grid-column: auto; }
      .health-summary { grid-template-columns: 1fr; }
      .activity-stats { display: none; }
      th:nth-child(4), td:nth-child(4),
      th:nth-child(6), td:nth-child(6) { display: none; }
    }

    @media (prefers-reduced-motion: reduce) {
      *, *::before, *::after {
        scroll-behavior: auto !important;
        animation-duration: .01ms !important;
        animation-iteration-count: 1 !important;
        transition-duration: .01ms !important;
      }
    }
  </style>
</head>
<body>
  <header class="topbar">
    <div class="topbar__inner">
      <div class="brand">
        <div class="brand__mark" aria-hidden="true">CP</div>
        <div class="brand__copy">
          <span class="brand__name">CoinPilot</span>
          <span class="brand__divider" aria-hidden="true"></span>
          <span class="brand__desk">Shadow operations</span>
        </div>
      </div>
      <div class="topbar__status">
        <div id="connection" class="connection" data-state="syncing" role="status" aria-live="polite">
          <span class="connection__dot" aria-hidden="true"></span>
          <span id="connection-label" class="connection__label">Syncing</span>
        </div>
        <time id="local-clock" class="clock">--:--:--</time>
        <button id="refresh-button" class="icon-button" type="button" aria-label="Refresh dashboard">
          <svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
            <path d="M20 11a8.1 8.1 0 0 0-15.5-2M4 4v5h5"></path>
            <path d="M4 13a8.1 8.1 0 0 0 15.5 2M20 20v-5h-5"></path>
          </svg>
        </button>
      </div>
    </div>
    <div id="refresh-track" class="refresh-track"></div>
  </header>

  <main class="shell">
    <section class="safety" aria-label="Simulation safety status">
      <div class="safety__lead">
        <div class="safety__seal" aria-hidden="true">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
            <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10Z"></path>
            <path d="m9 12 2 2 4-4"></path>
          </svg>
        </div>
        <div>
          <div class="safety__title">Simulation safety is locked</div>
          <div class="safety__subtitle">Local telemetry only · no exchange credentials</div>
        </div>
      </div>
      <div class="safety__item"><span>Orders routed</span><strong id="orders-sent">0</strong></div>
      <div class="safety__item"><span>Live routing</span><strong id="routing-state">Off</strong></div>
      <div class="safety__item"><span>Mode</span><strong id="strategy-mode">Observe</strong></div>
    </section>

    <section class="hero-grid">
      <article class="panel hero-panel" aria-labelledby="equity-title">
        <div class="panel__heading">
          <div>
            <p class="eyebrow">Simulated account</p>
            <h1 id="equity-title" class="panel__title">Equity ledger</h1>
          </div>
          <span id="market-chip" class="market-chip">@@MARKET_LABEL@@</span>
        </div>

        <div class="hero-value-row">
          <div>
            <p class="eyebrow">Current equity</p>
            <div id="hero-equity" class="hero-value skeleton">—</div>
            <div class="hero-delta">
              <span>Visible window</span>
              <strong id="window-delta">—</strong>
              <span id="window-delta-pct">—</span>
            </div>
          </div>
          <div class="lifecycle">
            <span id="lifecycle-chip" class="status-chip" data-state="warning">Loading</span>
            <div id="feed-age-label" class="lifecycle__meta">Feed age —</div>
          </div>
        </div>

        <div class="chart-shell">
          <svg id="equity-chart" viewBox="0 0 820 265" preserveAspectRatio="none" role="img" aria-labelledby="chart-title chart-description">
            <title id="chart-title">Simulated equity history</title>
            <desc id="chart-description">Loading the most recent equity samples.</desc>
          </svg>
          <div id="chart-empty" class="chart-empty">Loading equity history…</div>
        </div>
        <div class="chart-meta">
          <div class="chart-legend" aria-label="Chart legend">
            <span class="legend-item"><span class="legend-line"></span>Equity</span>
            <span class="legend-item"><span class="legend-line legend-line--cash"></span>Cash</span>
          </div>
          <div id="chart-window" class="chart-window">Awaiting samples</div>
        </div>
      </article>

      <aside class="panel health-panel" aria-labelledby="health-title">
        <div class="panel__heading">
          <div>
            <p class="eyebrow">Live diagnostics</p>
            <h2 id="health-title" class="panel__title">System health</h2>
          </div>
          <span id="readiness-chip" class="mode-chip">Checking</span>
        </div>
        <div class="health-summary">
          <div class="health-summary__item">
            <p class="eyebrow">Feed latency</p>
            <div id="health-feed-age" class="health-summary__value skeleton">0.00s</div>
          </div>
          <div class="health-summary__item">
            <p class="eyebrow">Disk free</p>
            <div id="health-disk" class="health-summary__value skeleton">00.0%</div>
          </div>
        </div>
        <div id="health-list" class="health-list">
          <div class="health-empty">Waiting for component heartbeats…</div>
        </div>
      </aside>
    </section>

    <section class="metrics" aria-label="Key metrics">
      <article class="metric">
        <div class="metric__header"><p class="eyebrow">Cash</p><span class="metric__index">01</span></div>
        <div id="cash-value" class="metric__value skeleton">—</div>
        <div id="cash-note" class="metric__note">Available quote balance</div>
      </article>
      <article class="metric">
        <div class="metric__header"><p class="eyebrow">Base exposure</p><span class="metric__index">02</span></div>
        <div id="exposure-value" class="metric__value skeleton">—</div>
        <div id="base-note" class="metric__note">0 @@BASE_TEXT@@ held</div>
      </article>
      <article class="metric">
        <div class="metric__header"><p class="eyebrow">Realized P&amp;L</p><span class="metric__index">03</span></div>
        <div id="pnl-value" class="metric__value skeleton">—</div>
        <div id="fees-note" class="metric__note">Fees —</div>
      </article>
      <article class="metric">
        <div class="metric__header"><p class="eyebrow">Max drawdown</p><span class="metric__index">04</span></div>
        <div id="drawdown-value" class="metric__value skeleton">0.00%</div>
        <div class="metric__note">Peak-to-trough, simulated</div>
      </article>
      <article class="metric">
        <div class="metric__header"><p class="eyebrow">Activity</p><span class="metric__index">05</span></div>
        <div id="activity-value" class="metric__value skeleton">0 / 0</div>
        <div class="metric__note">Decisions / fills</div>
      </article>
    </section>

    <section class="lower-grid">
      <article class="panel activity-panel" aria-labelledby="fills-title">
        <div class="panel__heading">
          <div>
            <p class="eyebrow">Execution audit</p>
            <h2 id="fills-title" class="panel__title">Recent simulated fills</h2>
          </div>
          <div class="activity-stats">
            <span>Pending <strong id="pending-count">0</strong></span>
            <span>Alerts <strong id="alert-count">0</strong></span>
          </div>
        </div>
        <div id="fills-container" class="table-wrap">
          <div class="empty-state">
            <div class="empty-state__icon" aria-hidden="true">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">
                <path d="M4 19V9M10 19V5M16 19v-7M22 19H2"></path>
              </svg>
            </div>
            <strong>Strategy is observing</strong>
            <p>No simulated fills yet. Market data is flowing while the execution gate remains closed.</p>
          </div>
        </div>
      </article>

      <aside class="panel runtime-panel" aria-labelledby="runtime-title">
        <div class="panel__heading">
          <div>
            <p class="eyebrow">Session audit</p>
            <h2 id="runtime-title" class="panel__title">Runtime ledger</h2>
          </div>
          <span class="mode-chip">Read only</span>
        </div>
        <div class="runtime-list">
          <div class="runtime-row"><span class="runtime-row__label">Run ID</span><code id="run-id" class="runtime-row__value">—</code></div>
          <div class="runtime-row"><span class="runtime-row__label">Uptime</span><span id="uptime" class="runtime-row__value">—</span></div>
          <div class="runtime-row"><span class="runtime-row__label">Warmup books</span><span id="warmup-books" class="runtime-row__value">—</span></div>
          <div class="runtime-row"><span class="runtime-row__label">Last market book</span><span id="last-book" class="runtime-row__value">—</span></div>
          <div class="runtime-row"><span class="runtime-row__label">Position quantity</span><span id="base-quantity" class="runtime-row__value">—</span></div>
          <div class="runtime-row"><span class="runtime-row__label">Halt reason</span><span id="halt-reason" class="runtime-row__value">None</span></div>
        </div>
      </aside>
    </section>

    <details class="raw-panel">
      <summary>Raw allowlisted telemetry</summary>
      <pre id="raw-status">Waiting for status payload…</pre>
    </details>

    <footer class="footer">
      <span id="updated-label">Never refreshed</span>
      <span class="footer__safety">SIMULATED · LIVE ROUTING FALSE · ORDERS SENT 0</span>
    </footer>
  </main>

  <script>
    "use strict";

    const REFRESH_MS = 5000;
    const svgNamespace = "http://www.w3.org/2000/svg";
    const configuredMarket = @@MARKET_JSON@@;
    const integer = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
    const decimal = new Intl.NumberFormat("en-US", { maximumFractionDigits: 8 });
    let quoteCurrency = @@QUOTE_JSON@@;
    let baseCurrency = @@BASE_JSON@@;
    let quoteFormatter = null;

    const byId = (id) => document.getElementById(id);
    const finite = (value) => Number.isFinite(Number(value));
    const numberOr = (value, fallback = 0) => finite(value) ? Number(value) : fallback;
    let refreshTimer = null;
    let refreshing = false;
    let lastUpdatedAt = null;
    let lastStatus = null;

    function setText(id, value) {
      const node = byId(id);
      if (node) {
        node.textContent = value;
        node.classList.remove("skeleton");
      }
    }

    function clearNode(node) {
      while (node.firstChild) node.removeChild(node.firstChild);
    }

    function makeSvg(name, attributes = {}) {
      const node = document.createElementNS(svgNamespace, name);
      Object.entries(attributes).forEach(([key, value]) => node.setAttribute(key, String(value)));
      return node;
    }

    function createQuoteFormatter(currency) {
      try {
        return new Intl.NumberFormat(currency === "KRW" ? "ko-KR" : "en-US", {
          style: "currency",
          currency,
          maximumFractionDigits: currency === "KRW" ? 0 : 8
        });
      } catch (_error) {
        return null;
      }
    }

    function configureMarket(value) {
      const market = String(value || configuredMarket).toUpperCase();
      const parts = market.split("-");
      if (parts.length === 2 && parts[0] && parts[1]) {
        quoteCurrency = parts[0];
        baseCurrency = parts[1];
      }
      quoteFormatter = createQuoteFormatter(quoteCurrency);
      setText("market-chip", market.replace("-", "—"));
      document.title = `CoinPilot · ${market} Shadow Operations`;
    }

    function formatQuote(value) {
      if (!finite(value)) return "—";
      if (quoteFormatter) return quoteFormatter.format(Number(value));
      return `${decimal.format(Number(value))} ${quoteCurrency}`;
    }

    function formatPercent(value, digits = 2) {
      if (!finite(value)) return "—";
      return `${(Number(value) * 100).toFixed(digits)}%`;
    }

    function formatQuantity(value) {
      return finite(value) ? decimal.format(Number(value)) : "—";
    }

    function formatAge(seconds) {
      if (!finite(seconds)) return "—";
      const value = Math.max(0, Number(seconds));
      if (value < 1) return `${Math.round(value * 1000)}ms`;
      if (value < 60) return `${value.toFixed(value < 10 ? 2 : 1)}s`;
      return `${Math.floor(value / 60)}m ${Math.floor(value % 60)}s`;
    }

    function formatDuration(seconds) {
      if (!finite(seconds)) return "—";
      const value = Math.max(0, Math.floor(Number(seconds)));
      const days = Math.floor(value / 86400);
      const hours = Math.floor((value % 86400) / 3600);
      const minutes = Math.floor((value % 3600) / 60);
      if (days) return `${days}d ${hours}h`;
      if (hours) return `${hours}h ${minutes}m`;
      return `${minutes}m ${value % 60}s`;
    }

    function formatTimeNs(value) {
      if (!finite(value)) return "—";
      return new Date(Number(value) / 1e6).toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
      });
    }

    function labelize(value) {
      return String(value || "unknown")
        .replace(/[_-]+/g, " ")
        .replace(/\\b\\w/g, (letter) => letter.toUpperCase());
    }

    function stateTone(value) {
      const normalized = String(value || "").toLowerCase();
      if (["ok", "ready", "running", "healthy"].includes(normalized)) return "ok";
      if (["warning", "warmup", "degraded", "recovering", "halted_recovery"].includes(normalized)) return "warning";
      return "error";
    }

    function setConnection(state, label) {
      byId("connection").dataset.state = state;
      setText("connection-label", label);
    }

    function setTone(node, value) {
      node.classList.remove("tone-positive", "tone-negative", "tone-warning");
      if (!finite(value)) return;
      if (Number(value) > 0) node.classList.add("tone-positive");
      if (Number(value) < 0) node.classList.add("tone-negative");
    }

    function renderStatus(status) {
      lastStatus = status;
      configureMarket(status.market || configuredMarket);
      const lifecycle = String(status.lifecycle_status || status.status || "unknown");
      const ready = Boolean(status.ready);
      const feedAge = numberOr(status.feed_age_seconds, NaN);
      const equity = numberOr(status.last_equity_quote);
      const cash = numberOr(status.cash_quote);
      const exposure = Math.max(0, equity - cash);
      const pnl = numberOr(status.realized_pnl_quote);
      const fees = numberOr(status.cumulative_fees_quote);
      const routing = Boolean(status.live_order_routing);

      setText("hero-equity", formatQuote(equity));
      setText("feed-age-label", `Feed age ${formatAge(feedAge)}`);
      setText("health-feed-age", formatAge(feedAge));
      setText("cash-value", formatQuote(cash));
      setText("exposure-value", formatQuote(exposure));
      setText("base-note", `${formatQuantity(status.base_quantity)} ${baseCurrency} held`);
      setText("pnl-value", formatQuote(pnl));
      setText("fees-note", `Fees ${formatQuote(fees)}`);
      setText("drawdown-value", formatPercent(status.max_drawdown));
      setText("activity-value", `${integer.format(numberOr(status.decisions))} / ${integer.format(numberOr(status.fills))}`);
      setText("pending-count", integer.format(numberOr(status.pending_orders)));
      setText("alert-count", integer.format(numberOr(status.alerts_pending)));
      setText("orders-sent", integer.format(numberOr(status.orders_sent)));
      setText("routing-state", routing ? "On" : "Off");
      setText("run-id", String(status.run_id || "—"));
      setText("warmup-books", integer.format(numberOr(status.warmup_books_seen)));
      setText("last-book", formatTimeNs(status.last_book_wall_ns));
      setText("base-quantity", `${formatQuantity(status.base_quantity)} ${baseCurrency}`);
      setText("halt-reason", status.halt_reason ? String(status.halt_reason) : "None");
      setText("raw-status", JSON.stringify(status, null, 2));

      const startedMs = finite(status.started_wall_ns) ? Number(status.started_wall_ns) / 1e6 : NaN;
      setText("uptime", finite(startedMs) ? formatDuration((Date.now() - startedMs) / 1000) : "—");

      const lifecycleChip = byId("lifecycle-chip");
      lifecycleChip.dataset.state = stateTone(lifecycle);
      lifecycleChip.textContent = labelize(lifecycle);

      const readinessChip = byId("readiness-chip");
      readinessChip.textContent = ready ? "Ready" : "Not ready";
      readinessChip.classList.toggle("tone-warning", !ready);

      const strategyHealth = Array.isArray(status.health)
        ? status.health.find((item) => item && item.component === "shadow_engine")
        : null;
      const strategyMode = strategyHealth && strategyHealth.details
        ? strategyHealth.details.strategy_mode
        : "observe";
      setText("strategy-mode", labelize(strategyMode));

      const diskFree = strategyHealth && strategyHealth.details
        ? strategyHealth.details.disk_free_pct
        : null;
      setText("health-disk", finite(diskFree) ? formatPercent(diskFree, 1) : "—");
      setText("cash-note", equity > 0 ? `${((cash / equity) * 100).toFixed(1)}% of simulated equity` : "Available quote balance");

      setTone(byId("pnl-value"), pnl);
      renderHealth(Array.isArray(status.health) ? status.health : []);

      if (ready && !routing && numberOr(status.orders_sent) === 0) {
        setConnection("live", "Live");
      } else if (!ready) {
        setConnection("stale", "Stale");
      } else {
        setConnection("syncing", "Review");
      }
    }

    function healthDetail(item) {
      const details = item && typeof item.details === "object" && item.details ? item.details : {};
      if (item.component === "market_feed") {
        const ordinal = finite(details.ordinal) ? `Book ${integer.format(Number(details.ordinal))}` : "Book stream";
        return details.continuity_reason ? `${ordinal} · ${labelize(details.continuity_reason)}` : `${ordinal} · continuity clean`;
      }
      if (item.component === "shadow_engine") {
        const mode = labelize(details.strategy_mode || "observe");
        const disk = finite(details.disk_free_pct) ? `${formatPercent(details.disk_free_pct, 1)} disk free` : "disk unknown";
        return `${mode} · ${disk}`;
      }
      return finite(item.observed_wall_ns)
        ? `Heartbeat ${formatTimeNs(item.observed_wall_ns)}`
        : "Heartbeat received";
    }

    function renderHealth(items) {
      const list = byId("health-list");
      clearNode(list);
      if (!items.length) {
        const empty = document.createElement("div");
        empty.className = "health-empty";
        empty.textContent = "No component heartbeats are available.";
        list.appendChild(empty);
        return;
      }

      items.forEach((item) => {
        const tone = stateTone(item.status);
        const row = document.createElement("article");
        row.className = "health-row";
        const top = document.createElement("div");
        top.className = "health-row__top";
        const nameWrap = document.createElement("div");
        nameWrap.className = "health-row__name-wrap";
        const dot = document.createElement("span");
        dot.className = "health-dot";
        dot.dataset.state = tone;
        dot.setAttribute("aria-hidden", "true");
        const name = document.createElement("span");
        name.className = "health-row__name";
        name.textContent = labelize(item.component);
        const state = document.createElement("span");
        state.className = "health-row__state";
        state.dataset.state = tone;
        state.textContent = labelize(item.status);
        const detail = document.createElement("p");
        detail.className = "health-row__detail";
        detail.textContent = healthDetail(item);
        nameWrap.append(dot, name);
        top.append(nameWrap, state);
        row.append(top, detail);
        list.appendChild(row);
      });
    }

    function renderEquity(payload) {
      const points = Array.isArray(payload && payload.equity)
        ? payload.equity.filter((row) => finite(row.equity_quote) && finite(row.cash_quote)).slice(-220)
        : [];
      const svg = byId("equity-chart");
      const empty = byId("chart-empty");
      clearNode(svg);

      const title = makeSvg("title", { id: "chart-title" });
      title.textContent = "Simulated equity and cash history";
      const description = makeSvg("desc", { id: "chart-description" });
      description.textContent = points.length
        ? `The most recent ${points.length} equity samples in the visible API window.`
        : "No equity samples are available.";
      svg.append(title, description);

      if (!points.length) {
        empty.hidden = false;
        empty.textContent = "No equity samples yet.";
        setText("chart-window", "No visible samples");
        return;
      }
      empty.hidden = true;

      const width = 820;
      const height = 265;
      const padX = 24;
      const padY = 22;
      const values = points.flatMap((point) => [Number(point.equity_quote), Number(point.cash_quote)]);
      let minimum = Math.min(...values);
      let maximum = Math.max(...values);
      const anchor = Math.max(Math.abs(maximum), 1);
      const minimumSpan = anchor * .0002;
      if (maximum - minimum < minimumSpan) {
        const center = (maximum + minimum) / 2;
        minimum = center - minimumSpan / 2;
        maximum = center + minimumSpan / 2;
      } else {
        const breathingRoom = (maximum - minimum) * .12;
        minimum -= breathingRoom;
        maximum += breathingRoom;
      }

      const x = (index) => padX + (index / Math.max(points.length - 1, 1)) * (width - padX * 2);
      const y = (value) => padY + ((maximum - value) / (maximum - minimum)) * (height - padY * 2);
      const pathFor = (field) => points
        .map((point, index) => `${index ? "L" : "M"}${x(index).toFixed(2)},${y(Number(point[field])).toFixed(2)}`)
        .join(" ");

      for (let index = 0; index <= 4; index += 1) {
        const gridY = padY + (index / 4) * (height - padY * 2);
        svg.appendChild(makeSvg("line", {
          x1: padX,
          y1: gridY,
          x2: width - padX,
          y2: gridY,
          class: "chart-grid"
        }));
      }

      const equityPath = pathFor("equity_quote");
      const lastX = x(points.length - 1);
      const firstX = x(0);
      const bottom = height - padY;
      const area = makeSvg("path", {
        d: `${equityPath} L${lastX.toFixed(2)},${bottom} L${firstX.toFixed(2)},${bottom} Z`,
        class: "chart-equity-fill"
      });
      const cashLine = makeSvg("path", { d: pathFor("cash_quote"), class: "chart-cash-line" });
      const equityLine = makeSvg("path", { d: equityPath, class: "chart-equity-line" });
      const lastPoint = points[points.length - 1];
      const endpoint = makeSvg("circle", {
        cx: lastX,
        cy: y(Number(lastPoint.equity_quote)),
        r: 4,
        class: "chart-endpoint"
      });
      const maxLabel = makeSvg("text", { x: padX + 2, y: padY + 11, class: "chart-label" });
      maxLabel.textContent = formatQuote(maximum);
      const minLabel = makeSvg("text", { x: padX + 2, y: height - padY - 6, class: "chart-label" });
      minLabel.textContent = formatQuote(minimum);
      svg.append(area, cashLine, equityLine, endpoint, maxLabel, minLabel);

      const first = points[0];
      const delta = Number(lastPoint.equity_quote) - Number(first.equity_quote);
      const deltaPct = Number(first.equity_quote) ? delta / Number(first.equity_quote) : 0;
      setText("window-delta", `${delta >= 0 ? "+" : "−"}${formatQuote(Math.abs(delta))}`);
      setText("window-delta-pct", `${deltaPct >= 0 ? "+" : ""}${formatPercent(deltaPct)}`);
      setTone(byId("window-delta"), delta);
      setTone(byId("window-delta-pct"), delta);

      const firstTime = formatTimeNs(first.book_wall_ns || first.created_wall_ns);
      const lastTime = formatTimeNs(lastPoint.book_wall_ns || lastPoint.created_wall_ns);
      setText("chart-window", `${points.length} samples · ${firstTime}—${lastTime}`);
    }

    function makeCell(text) {
      const cell = document.createElement("td");
      cell.textContent = text;
      return cell;
    }

    function renderFills(payload) {
      const fills = Array.isArray(payload && payload.fills) ? payload.fills.slice(0, 25) : [];
      const container = byId("fills-container");
      clearNode(container);
      if (!fills.length) {
        const empty = document.createElement("div");
        empty.className = "empty-state";
        const icon = document.createElement("div");
        icon.className = "empty-state__icon";
        icon.setAttribute("aria-hidden", "true");
        icon.textContent = "○";
        const title = document.createElement("strong");
        title.textContent = "Strategy is observing";
        const copy = document.createElement("p");
        copy.textContent = "No simulated fills yet. Market data is flowing while the execution gate remains closed.";
        empty.append(icon, title, copy);
        container.appendChild(empty);
        return;
      }

      const table = document.createElement("table");
      table.setAttribute("aria-label", "Recent simulated fills");
      const head = document.createElement("thead");
      const headerRow = document.createElement("tr");
      [
        "Time",
        "Side",
        `Base (${baseCurrency})`,
        `VWAP (${quoteCurrency})`,
        `Notional (${quoteCurrency})`,
        `Fee (${quoteCurrency})`,
        "Status"
      ].forEach((label) => {
        const cell = document.createElement("th");
        cell.scope = "col";
        cell.textContent = label;
        headerRow.appendChild(cell);
      });
      head.appendChild(headerRow);
      const body = document.createElement("tbody");
      fills.forEach((fill) => {
        const row = document.createElement("tr");
        const time = makeCell(formatTimeNs(fill.created_wall_ns || fill.book_wall_ns));
        const sideCell = document.createElement("td");
        const side = document.createElement("span");
        side.className = "table-chip";
        side.dataset.side = String(fill.side || "").toLowerCase();
        side.textContent = labelize(fill.side);
        sideCell.appendChild(side);
        row.append(
          time,
          sideCell,
          makeCell(`${formatQuantity(fill.filled_base)} ${baseCurrency}`),
          makeCell(formatQuote(fill.vwap_price)),
          makeCell(formatQuote(fill.filled_quote)),
          makeCell(formatQuote(fill.fee_quote)),
          makeCell(labelize(fill.execution_status))
        );
        body.appendChild(row);
      });
      table.append(head, body);
      container.appendChild(table);
    }

    async function fetchJson(path) {
      const response = await fetch(path, { cache: "no-store" });
      const data = await response.json();
      if (!response.ok) {
        const error = new Error(`Request failed: ${response.status}`);
        error.payload = data;
        throw error;
      }
      return data;
    }

    function restartTrack() {
      const track = byId("refresh-track");
      track.classList.remove("is-active");
      void track.offsetWidth;
      track.classList.add("is-active");
    }

    function scheduleRefresh() {
      window.clearTimeout(refreshTimer);
      refreshTimer = window.setTimeout(() => {
        if (document.visibilityState === "visible") refresh();
        else scheduleRefresh();
      }, REFRESH_MS);
      restartTrack();
    }

    async function refresh() {
      if (refreshing) return;
      refreshing = true;
      const button = byId("refresh-button");
      button.disabled = true;
      button.classList.add("is-spinning");
      setConnection("syncing", "Syncing");

      const results = await Promise.allSettled([
        fetchJson("/health/ready").catch(() => fetchJson("/api/status")),
        fetchJson("/api/equity"),
        fetchJson("/api/fills")
      ]);
      const [statusResult, equityResult, fillsResult] = results;

      if (statusResult.status === "fulfilled") {
        renderStatus(statusResult.value);
        lastUpdatedAt = Date.now();
      } else if (statusResult.reason && statusResult.reason.payload) {
        renderStatus(statusResult.reason.payload);
        setConnection("stale", "Not ready");
      } else {
        setConnection("stale", lastStatus ? "Stale" : "Offline");
      }

      if (equityResult.status === "fulfilled") renderEquity(equityResult.value);
      if (fillsResult.status === "fulfilled") renderFills(fillsResult.value);

      button.disabled = false;
      button.classList.remove("is-spinning");
      refreshing = false;
      scheduleRefresh();
      updateRelativeLabels();
    }

    function updateRelativeLabels() {
      if (lastUpdatedAt) {
        const seconds = Math.max(0, Math.floor((Date.now() - lastUpdatedAt) / 1000));
        setText("updated-label", seconds < 2 ? "Updated just now" : `Updated ${seconds}s ago`);
      }
      if (lastStatus && finite(lastStatus.started_wall_ns)) {
        setText("uptime", formatDuration((Date.now() - Number(lastStatus.started_wall_ns) / 1e6) / 1000));
      }
    }

    function updateClock() {
      byId("local-clock").textContent = new Date().toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false
      });
      updateRelativeLabels();
    }

    byId("refresh-button").addEventListener("click", refresh);
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") refresh();
    });
    window.setInterval(updateClock, 1000);
    configureMarket(configuredMarket);
    updateClock();
    refresh();
  </script>
</body>
</html>
"""
    replacements = {
        "@@MARKET_TEXT@@": html.escape(normalized_market),
        "@@MARKET_LABEL@@": html.escape(normalized_market.replace("-", "—")),
        "@@BASE_TEXT@@": html.escape(base_currency),
        "@@MARKET_JSON@@": json.dumps(normalized_market),
        "@@QUOTE_JSON@@": json.dumps(quote_currency),
        "@@BASE_JSON@@": json.dumps(base_currency),
    }
    for placeholder, value in replacements.items():
        content = content.replace(placeholder, value)
    if "@@" in content:
        raise RuntimeError("dashboard template contains an unresolved placeholder")
    return content.encode("utf-8")
