# CoinPilot operations handoff

> 마지막 갱신: 2026-07-23 03:08 KST
> 이 문서는 시점 스냅샷이다. 손익, PID, revision, readiness 같은 동적 값은
> 반드시 다시 조회하며 현재 관측 결과가 이 문서보다 우선한다.

## Git과 배포 기준

| 항목 | 스냅샷 |
| --- | --- |
| 저장소 | `hypark5540/ai-coin-trading` |
| branch | `agent/bounded-shadow-risk-controls` |
| D2 배포 코드 / handoff baseline | `ab784db022047cfaee89d5123a1e6aeac958bb2a` |
| 중앙 daily-scorecard 배포 코드 | `f5f53909f296dd6a99a1a79a46de7bbdbc5b8b24` |
| repository HEAD | 세션 시작 시 재조회. handoff 문서 commit은 D2 배포 코드보다 뒤일 수 있음 |
| PR | Draft PR #1, base `main` |
| `main` | `9033db3e93e5966a823dd44a6473a2e426e7b77c` |

`ab784db`에서 정상적인 `receive_interval` 관측 침묵이 warmup을 반복
초기화하던 문제를 수정했다. `/api/status`는 feed freshness와 lifecycle을
분리하고 dashboard는 `Live · warming up`, stale, halted, stopped를 구분한다.
`/health/ready`의 running+fresh fail-closed 계약은 바뀌지 않았다.

## 2026-07-23 통합 일일 성적표 배포

03:06 KST에 중앙 LaunchAgent `dev.coinpilot.daily-scorecard` 하나를 설치하고
명시적으로 enabled 처리했다. 이 job은 매시 `:10`에 one-shot으로 실행하되 정상
일일 deadline은 `00:10 KST`다. 승인된 D2/C2 8개 원장을 SQLite `mode=ro`와
`query_only`의 단일 read transaction으로 조회하고, 거래 instance의 package,
config, fingerprint, DB, outbox 또는 per-instance notifier는 변경하지 않는다.

배포 직전 `f5f5390`의 [PR CI](https://github.com/hypark5540/ai-coin-trading/actions/runs/29944739416)와
[Push CI](https://github.com/hypark5540/ai-coin-trading/actions/runs/29944735731)에서
Ubuntu/macOS 4개 작업이 모두 성공했다.
로컬 전체 pytest는 `352 passed`, `./scripts/mac-studio test`와
`git diff --check`도 성공했다. 첫 Ubuntu CI 실패는 GNU/BSD `stat -f` 의미 차이인
테스트 이식성 문제였고, Python `stat.S_IMODE` 검사로 교체한 뒤 4개 CI를 다시
통과했다.

RunAtLoad가 최신 완료일을 먼저 보내고 수동 one-shot 한 번으로 최초 source
일자의 backlog를 보냈다. 둘 다 Slack 성공 뒤 local receipt가 첫 시도에
`delivered`가 됐으며 `last_error=null`이다.

| KST 일자 | report ID | D2 일 회계 P&L / RT / fee | C2 일 회계 P&L / RT / fee | JSON SHA-256 |
| --- | --- | --- | --- | --- |
| 2026-07-22 | `daily-scorecard:v1:2026-07-22` | -₩476.390223 / 14 / ₩349.936773 | -₩14,583.350868 / 1 / ₩618.329990 | `858e9c959720d46d14caef93308adbb6e6ce198aa640e78fcb24979416c33a19` |
| 2026-07-21 | `daily-scorecard:v1:2026-07-21` | ₩0 / 0 / ₩0 | ₩0 / 0 / ₩625 | `362c6c1f979a23cbf0dfa279859a3aae061538dd36fec64fee7d8fbe0c4f72ac` |

7월 22일 D2 return/DD는 자정 anchor가 없는 첫 운영일이라 네 시장 모두
`partial(start +20.0h)`로 표시한다. C2 BTC 일 return은 non-flat boundary의 완료
60분봉 liquidation 추정치이며 `estimated`로 표시하고, C2 historical intraday
DD는 원장에 equity history가 없어 `N/A`다. 7월 21일에는 C2 BTC 진입 수수료
₩625와 turnover ₩1,250,000이 있었고 실현손익/완료 RT는 0이다. 당시 D2는 아직
시작 전이라 equity coverage가 `unavailable`로 표시된다.

감사 산출물과 delivery state는 모두 다음 private reporting home에 있고 파일
mode는 `0600`, 디렉터리는 `0700`이다. JSON과 `.sha256` checksum을 재검증했다.

```text
~/Library/Application Support/Coinpilot/reporting/
  bin/coinpilot-daily-scorecard
  reports/coinpilot-daily-scorecard-v1-2026-07-{21,22}.{json,md,sha256}
  state/deliveries/2026-07-{21,22}.json
  logs/daily-scorecard.{stdout,stderr}.log
```

최종 중앙 status는 `delivery_count=2`, `latest_due_delivered=true`,
`missing_backlog_days=0`, `unresolved_receipts=0`, `healthy=true`이고 Doctor는
`0 errors`다. Slack webhook은 기존 macOS Keychain item을 argv/env/file/log에
노출하지 않고 읽었다. Slack 성공 직후 local receipt 기록 전에 전원이 끊기면
원격·로컬 원자 commit이 불가능해 드문 at-least-once 중복 가능성은 남는다.

배포 전후 비교에서 8개 instance의 config/helper hash, 원장 경로, D2 run ID와
config/code fingerprint, C2 account/config fingerprint가 모두 동일했다. D2/C2
거래 PID도 유지됐고 8개 모두 flat, D2 pending 0, reconciliation PASS였다.
`simulated=true`, `own_execution=false`, `live_order_routing=false`,
`orders_sent=0`을 다시 확인했다. 이 배포 때문에 D2/C2를 install/update/restart하지
않았고 새 거래 원장도 만들지 않았다.

자가개선 gate는 현재 `COLLECTING_T0_EVIDENCE`다. C2 공통 관측 1/30일과 완료
RT 1/30건뿐이므로 손익 결론이나 활성 변경을 하지 않는다. 30일과 30 RT를 모두
충족해도 causal replay, purged walk-forward, 2배 비용 stress, 운영자 검토를 거친
새 model/account/ledger 후보만 제안하며 전략·halt·fingerprint·원장을 자동
변경하거나 rearm하지 않는다.

## 2026-07-22 재부팅 후 LaunchAgent drift

22:50 KST의 정상 shutdown/reboot 뒤, 설정상 비활성인 plist 일부가 로그인 시
`RunAtLoad`로 다시 로드됐다. 당시 `stop`과 비활성 install은 job을 bootout만 하고
launchd에 영구 `disable`하지 않았던 것이 원인이었다. 23:57 최초 재점검에서
승인되지 않은 38개 loaded job을 확인했다.

- D2: notifier 4개 running, paper 4개 EX_CONFIG 재시작 루프
- C2: shadow/web/external-watchdog 12개 running, notifier 4개 loaded/running
- legacy: shadow 4개 running, backup/retention 8개 loaded, XRP/SOL paper 2개 loaded

영향은 모두 모의거래 경계 안에 있었다. 전 원장과 API에서
`simulated=true`, `live_order_routing=false`, `orders_sent=0`이었고 실제 주문은
없었다. 다만 비활성 notifier가 D2와 C2 shadow에서 시장별 2건씩, 총 16개의
시간 요약을 Slack에 전달했다. 첫 8건은 로그인 뒤 22:54:25~54, 다음 8건은
23:00:15~17 KST였으며 00:00 이후 추가 전달은 없다.

C2의 비의도 shadow는 observe여서 결정·주문·체결이 모두 0이었다. retired 1%
diagnostic은 BTC가 기존 daily-loss halt를 그대로 보존해 결정 0, ETH/XRP/SOL은
각각 50/14/55건의 모의 체결과 -₩4,448.05/-₩1,343.82/-₩6,248.06의 이번-run
손실을 기록했다. 네 legacy 원장은 모두 flat, pending 0으로 정상 종료됐고 DB,
WAL/SHM, archive, log는 삭제하거나 초기화하지 않고 그대로 보존했다.

23:59~00:00 KST에 승인되지 않은 job을 bootout하고 launchd persistent override를
명시적으로 disabled 처리했다. 현재 승인 matrix는 D2의
shadow/web/external-watchdog/backup/retention, C2의 paper/backup/retention뿐이며
legacy 전 역할도 disabled다. 재발 방지 소스는 stop/no-start/설정상 비활성
서비스를 persistent disable한 뒤 bootout하고, start만 다시 enable하도록 한다.
Status와 Doctor는 설정과 loaded/override 상태의 drift를 명시적으로 표시한다.

이 수정은 저장소의 운영 entrypoint와 테스트만 바꾸며 설치된 D2 helper/package나
config fingerprint는 바꾸지 않는다. 따라서 현재 D2를 install/redeploy하거나 새
원장으로 전환하지 않았다. 다음 계획된 로그인/재부팅 뒤 persistent disable이
유지되는지는 다시 실측한다. 이 검증만을 위해 운영 호스트를 재부팅하지 않는다.

## 현재 운영 토폴로지

### D2 bounded diagnostic shadow

관측 시각 2026-07-23 00:03 KST에 네 인스턴스 모두 `ready`, warmup 100,
fresh feed, flat position, pending 0이었다. shadow/web/external-watchdog/backup/
retention LaunchAgent가 로드되어 있고 각 doctor는 `0 errors, 0 warnings`였다.

| instance | market | dashboard | current/lineage 체결 | current-run 실현손익 |
| --- | --- | --- | ---: | ---: |
| `d2-btc` | KRW-BTC | `http://127.0.0.1:8774` | 2 / 8 | -₩121.25 |
| `d2-eth` | KRW-ETH | `http://127.0.0.1:8775` | 2 / 8 | -₩108.85 |
| `d2-xrp` | KRW-XRP | `http://127.0.0.1:8776` | 2 / 4 | -₩80.09 |
| `d2-sol` | KRW-SOL | `http://127.0.0.1:8777` | 2 / 8 | -₩166.20 |

활성 원장은 각 instance의 다음 파일이다.

```text
~/Library/Application Support/Coinpilot/instances/<instance>/data/
  shadow-diagnostic-bounded-v1-ab784db02204.db
```

이전 `data/shadow.db`와 2026-07-22 online backup은 감사용으로 보존되어 있다.
새 원장은 각각 모의자금 ₩5,000,000으로 시작했다. 정책은 일손실 10%, peak
drawdown 10%, 진입 cooldown 1시간, 일 최대 24 round trips이다. 이 전략은
validated alpha가 아니므로 소액 손실과 수수료 발생 자체는 장애가 아니다.

재부팅 전 run은 22:50:37 KST에 모두 `graceful:sigterm`, flat, pending 0으로
종료됐다. 현재 run은 22:54:31에 같은 config/code fingerprint와 `restart_of`로
연결돼 각 원장의 run lineage가 2개다. 현재 run 중 확정된 `receive_error`
reconnect 경계가 BTC/ETH/XRP/SOL 각각 2/3/2/1회 있었지만 같은 PID와 run에서
warmup을 다시 충족했고 최신 상태는 fresh/ready다.

배포 직후 55초 연속 관찰에서 네 run ID가 유지되고 warmup은 100 아래로
떨어지지 않았다. 같은 구간에 BTC 2건, SOL 4건의 `receive_interval` marker가
실제로 있었지만 두 instance 모두 ready를 유지했다. connection은 각 1개,
monotonic regression은 0이었다.

### C2 60분봉 forward-paper

관측 시각 2026-07-23 00:08 KST에 네 paper LaunchAgent는 모두 실행 중이고
`halt_state=ACTIVE`,
`updated_at`과 revision이 증가하고 있었다.

| instance | market | 관측 revision | 관측 실현손익 |
| --- | --- | ---: | ---: |
| `c2-btc` | KRW-BTC | 5335 | -₩14,583.35 |
| `c2-eth` | KRW-ETH | 3340 | ₩0 |
| `c2-xrp` | KRW-XRP | 3344 | ₩0 |
| `c2-sol` | KRW-SOL | 3277 | ₩0 |

각 원장은 `data/coinpilot-c2.db`다. C2는 시간봉 전략이므로 PnL이 오래
고정되거나 체결이 없는 것만으로 중단이라고 판단하지 않는다.

현재 repository source는 C2 계좌 생성 당시의 frozen source와 달라
`./scripts/mac-studio doctor --instance c2-*`가
`immutable C2 manifest drift` 1건을 보고한다. 설치된 C2 package와 설치 당시
manifest의 자체 검증은 일치하고 런타임은 ACTIVE이므로 현재 계좌 손상 증거는
아니다. 이 Doctor 오류 표시를 없애기 위해 기존 C2에 `install` 또는 `update`를
실행하지 않는다. C2 코드 승격은 새 account와 새 T0로 별도 검토한다.

## 안전 상태

- D2/C2 모두 공개 시세 기반 모의거래다.
- `simulated=true`, `live_order_routing=false`, `orders_sent=0`을 확인했다.
- 모든 통합 성적표에서 `own_execution=false`도 함께 확인한다.
- Upbit private API key와 실제 주문 경로는 없다.
- D2/C2 notifier는 not-loaded이고 launchd persistent disabled이며 D2/C2
  runtime의 `COINPILOT_ENABLE_NOTIFIER=0`을 유지한다.
- 과거 1% diagnostic instance `btc/eth/xrp/sol`과 port
  `8766/8767/8772/8773`은 retired 상태이며 listener가 없다. 일부 runtime
  enable 값과 plist는 감사용으로 남아 있지만 모든 legacy launchd override는
  disabled다. 이 instance를 대상으로 `start`, `install`, `bootstrap`을
  실행하지 않는다.
- `127.0.0.1:8765`는 다른 로컬 프로젝트 소유이므로 건드리지 않는다.
- non-instance 구형 default label `dev.coinpilot.shadow/web/external-watchdog/
  notifier/backup/retention`은 matching plist와 loaded job이 없지만 일부 launchd
  override가 enabled로 남아 있다. 현재 실행 위험은 없으나 같은 plist가 다시
  생기면 활성화될 수 있으므로 별도 명시 승인 없이 정리하거나 재사용하지 않는다.

## 검증 증거

- `ab784db` 당시 로컬 Python: `306 passed`
- `ab784db` 당시 운영 shell test: `./scripts/mac-studio test` 성공
- `ab784db` 당시 `git diff --check` 성공
- commit `ab784db` GitHub Actions: Ubuntu/macOS 4개 check 모두 성공
- D2 네 doctor: 각각 `0 errors, 0 warnings`
- C2 네 doctor: 알려진 repository-source manifest drift 각 1건, 추가 오류 0,
  warning 0; installed package `verify-installed valid=true`
- 2026-07-23 현재 로컬 Python: `306 passed`
- launchd persistent disable 회귀를 포함한 `./scripts/mac-studio test` 성공
- 중앙 daily-scorecard 배포 전 로컬 Python: `352 passed`
- 중앙 daily-scorecard 운영·격리 회귀를 포함한 `./scripts/mac-studio test` 성공
- commit `f5f5390` GitHub Actions: PR/Push Ubuntu/macOS 4개 작업 모두 성공
- 중앙 reporter Doctor: `0 errors`; 7월 21/22 receipt와 artifact checksum 성공
- 중앙 배포 전후 D2/C2 managed hash와 run/account fingerprint 동일
- 배포 전 기존 D2 네 원장 SQLite integrity check와 backup checksum 성공
- 배포 후 신규 D2 네 원장 SQLite integrity check 성공, run lineage 각 1개
- 재부팅 후 D2/C2/legacy 관련 DB SQLite integrity check 성공; 현재 D2 run
  lineage 각 2개

관련 변경 이력:

- `ab784db` — false receive gap 및 readiness 표시 수정
- `2cb9ec6` — launchd restart race 수정
- `e10a9bf` — Homebrew Python 선택 수정
- `883699b` — multi-market C2/D2와 bounded 10% shadow 추가

## 다음 세션의 첫 확인

```bash
git status --short --branch
git log --oneline -6

for i in d2-btc d2-eth d2-xrp d2-sol; do
  ./scripts/mac-studio status --instance "$i"
  ./scripts/mac-studio doctor --instance "$i"
done

for i in c2-btc c2-eth c2-xrp c2-sol; do
  ./scripts/mac-studio status --instance "$i"
  ./scripts/mac-studio doctor --instance "$i"
done

./scripts/mac-studio-daily-scorecard status
./scripts/mac-studio-daily-scorecard doctor

launchctl print-disabled "gui/$(id -u)" | rg 'dev\.coinpilot'

for port in 8774 8775 8776 8777; do
  curl -fsS "http://127.0.0.1:${port}/api/status"
  printf '\n'
done
```

확인할 핵심은 D2의 `ready`, fresh feed, `halt_reason=null`, pending 주문,
10% 경계와 C2의 `ACTIVE`, revision/updated_at 증가다. 모든 API에서
`live_order_routing=false`, `orders_sent=0`을 재확인한다.

C2 doctor에서는 현재 repository source와 frozen 설치본 차이 때문에 각
instance의 알려진 `immutable C2 manifest drift` 1 error가 예상된다. 추가
error/warning은 별도 장애로 취급한다. 설치본 자체의 package/manifest 일치는
다음처럼 별도로 확인한다.

```bash
for i in c2-btc c2-eth c2-xrp c2-sol; do
  home="$HOME/Library/Application Support/Coinpilot/instances/$i"
  package_root="$("$home/venv/bin/python" -c \
    'import pathlib, coinpilot; print(pathlib.Path(next(iter(coinpilot.__path__))).resolve())')"
  "$home/venv/bin/python" "$home/bin/coinpilot-c2-config" verify-installed \
    --config "$home/config/paper.toml" \
    --manifest "$home/config/paper.manifest.json" \
    --installed-package-root "$package_root"
done
```

현재 미해결 운영 과제는 C2 doctor가 repository source drift와 설치본 runtime
건전성을 한 오류로 표시하는 점, 다음 계획된 로그인/재부팅 뒤 launchd persistent
disable을 다시 실측하는 것, matching plist가 없는 구형 default label의 enabled
override 잔재다. 중앙 reporter의 C2 historical intraday DD `N/A`와 드문 Slack
at-least-once 중복 경계도 유지된다. 향후 C2 개선 시 immutable 검증을 약화하거나
기존 C2 계좌를 덮어쓰지 말고 두 상태를 명확히 분리해 표시한다.

## 다음 세션용 복사 프롬프트

```text
현재 CoinPilot 저장소 운영을 이어서 맡아줘.

먼저 ~/.codex/AGENTS.md, 저장소의 AGENTS.md,
docs/OPERATIONS_HANDOFF.md, docs/MAC_STUDIO.md, SECURITY.md를 읽어.
handoff는 2026-07-23 시점 스냅샷이므로 그대로 믿지 말고 git 상태와 실제
LaunchAgent/API/SQLite 상태를 다시 조회해. 현재 관측 결과가 문서보다 우선이야.

반드시 지킬 것:
- git config user.*를 변경하지 말 것.
- Upbit private API key, 실제 주문 client/routing을 추가하거나 켜지 말 것.
- simulated=true, live_order_routing=false, orders_sent=0을 유지할 것.
- 기존 DB/WAL/archive/backup/log를 삭제하거나 halt/fingerprint 원장을 자동
  rearm하지 말 것.
- legacy btc/eth/xrp/sol 및 8766/8767/8772/8773을 재가동하지 말 것.
- legacy instance를 대상으로 start/install/bootstrap을 실행하지 말 것.
- 다른 프로젝트가 쓰는 8765를 건드리지 말 것.
- C2 현재 계정에 install/update를 적용하지 말 것. repo source drift와 실제
  installed runtime 건강 상태를 구분할 것.
- 중앙 `dev.coinpilot.daily-scorecard`만 승인됐다. 사용자가 별도로 명시하지 않으면
  D2/C2 per-instance Slack notifier를 켜지 말 것.
- 설정상 비활성인 LaunchAgent는 not-loaded뿐 아니라 launchctl persistent
  disabled인지도 확인할 것. loaded/override drift는 즉시 fail-closed할 것.

D2 d2-btc/d2-eth/d2-xrp/d2-sol의 status와 doctor, 8774~8777 /api/status를
확인하고, C2 c2-btc/c2-eth/c2-xrp/c2-sol의 status와 doctor,
paper revision·updated_at·ACTIVE 및 설치본 verify-installed 상태를 확인해.
현재 알려진 C2 Doctor의 repo-source manifest drift 1건과 추가 오류를 구분해.
fresh warmup은 stopped가 아니고, C2의 장시간 무체결도 시간봉 특성상 정상일 수
있어.

중앙 daily-scorecard의 status/doctor, 최신 due receipt, backlog 0, artifact
checksum도 확인해. 이 reporter의 자가개선은 분석·오프라인 후보 제안까지만이며
활성 전략·설정·halt·fingerprint·account·ledger를 자동 변경하면 안 돼.

자동 복구는 config·원장 변경이 없고 halt/fingerprint/integrity/position/pending
문제가 없음을 확인한 현재 인스턴스의 일시적 launchd 프로세스 장애에만 한정해.
D2 halt/fingerprint drift와 C2 halt/gap/장기 downtime은 자동 rearm/restart하지
말고 진단 결과를 보고해. 모든 경우 데이터 보존과 fail-closed 경계를 우선해.

코드를 바꾸면 관련 테스트, 전체 pytest, ./scripts/mac-studio test,
git diff --check를 실행하고 commit/push 뒤 Ubuntu/macOS CI 성공을 확인한 다음
배포해. D2 코드/config fingerprint가 바뀌면 기존 원장을 재사용하지 말고
online backup 후 새 versioned shadow DB로 시작해. 마지막에 실제 현재 상태,
수행한 변경, 검증 결과, 남은 위험을 한국어로 간결하게 보고해.
```
