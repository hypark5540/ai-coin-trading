# CoinPilot operations handoff

> 마지막 갱신: 2026-07-22 22:41 KST
> 이 문서는 시점 스냅샷이다. 손익, PID, revision, readiness 같은 동적 값은
> 반드시 다시 조회하며 현재 관측 결과가 이 문서보다 우선한다.

## Git과 배포 기준

| 항목 | 스냅샷 |
| --- | --- |
| 저장소 | `hypark5540/ai-coin-trading` |
| branch | `agent/bounded-shadow-risk-controls` |
| D2 배포 코드 / handoff baseline | `ab784db022047cfaee89d5123a1e6aeac958bb2a` |
| repository HEAD | 세션 시작 시 재조회. handoff 문서 commit은 D2 배포 코드보다 뒤일 수 있음 |
| PR | Draft PR #1, base `main` |
| `main` | `9033db3e93e5966a823dd44a6473a2e426e7b77c` |

`ab784db`에서 정상적인 `receive_interval` 관측 침묵이 warmup을 반복
초기화하던 문제를 수정했다. `/api/status`는 feed freshness와 lifecycle을
분리하고 dashboard는 `Live · warming up`, stale, halted, stopped를 구분한다.
`/health/ready`의 running+fresh fail-closed 계약은 바뀌지 않았다.

## 현재 운영 토폴로지

### D2 bounded diagnostic shadow

관측 시각 2026-07-22 22:41 KST에 네 인스턴스 모두 `ready`, warmup 100,
fresh feed, flat position, pending 0이었다. shadow/web/external-watchdog/backup/
retention LaunchAgent가 로드되어 있고 각 doctor는 `0 errors, 0 warnings`였다.

| instance | market | dashboard | 관측 체결 | 관측 실현손익 |
| --- | --- | --- | ---: | ---: |
| `d2-btc` | KRW-BTC | `http://127.0.0.1:8774` | 6 | -₩94.95 |
| `d2-eth` | KRW-ETH | `http://127.0.0.1:8775` | 6 | -₩83.85 |
| `d2-xrp` | KRW-XRP | `http://127.0.0.1:8776` | 2 | -₩40.06 |
| `d2-sol` | KRW-SOL | `http://127.0.0.1:8777` | 6 | -₩141.20 |

활성 원장은 각 instance의 다음 파일이다.

```text
~/Library/Application Support/Coinpilot/instances/<instance>/data/
  shadow-diagnostic-bounded-v1-ab784db02204.db
```

이전 `data/shadow.db`와 2026-07-22 online backup은 감사용으로 보존되어 있다.
새 원장은 각각 모의자금 ₩5,000,000으로 시작했다. 정책은 일손실 10%, peak
drawdown 10%, 진입 cooldown 1시간, 일 최대 24 round trips이다. 이 전략은
validated alpha가 아니므로 소액 손실과 수수료 발생 자체는 장애가 아니다.

배포 직후 55초 연속 관찰에서 네 run ID가 유지되고 warmup은 100 아래로
떨어지지 않았다. 같은 구간에 BTC 2건, SOL 4건의 `receive_interval` marker가
실제로 있었지만 두 instance 모두 ready를 유지했다. connection은 각 1개,
monotonic regression은 0이었다.

### C2 60분봉 forward-paper

관측 시각에 네 paper LaunchAgent는 모두 실행 중이고 `halt_state=ACTIVE`,
`updated_at`과 revision이 증가하고 있었다.

| instance | market | 관측 revision | 관측 실현손익 |
| --- | --- | ---: | ---: |
| `c2-btc` | KRW-BTC | 5197 | -₩14,583.35 |
| `c2-eth` | KRW-ETH | 3202 | ₩0 |
| `c2-xrp` | KRW-XRP | 3206 | ₩0 |
| `c2-sol` | KRW-SOL | 3144 | ₩0 |

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
- Upbit private API key와 실제 주문 경로는 없다.
- D2/C2 notifier는 not-loaded이며 D2 runtime의
  `COINPILOT_ENABLE_NOTIFIER=0`을 유지한다.
- 과거 1% diagnostic instance `btc/eth/xrp/sol`과 port
  `8766/8767/8772/8773`은 retired 상태이며 listener가 없다. 일부 runtime
  enable 값이나 launchd enable 상태는 남아 있을 수 있으므로 이 legacy
  instance를 대상으로 `start`, `install`, `bootstrap`을 실행하지 않는다.
- `127.0.0.1:8765`는 다른 로컬 프로젝트 소유이므로 건드리지 않는다.

## 검증 증거

- `ab784db` 당시 로컬 Python: `306 passed`
- `ab784db` 당시 운영 shell test: `./scripts/mac-studio test` 성공
- `ab784db` 당시 `git diff --check` 성공
- commit `ab784db` GitHub Actions: Ubuntu/macOS 4개 check 모두 성공
- D2 네 doctor: 각각 `0 errors, 0 warnings`
- 배포 전 기존 D2 네 원장 SQLite integrity check와 backup checksum 성공
- 배포 후 신규 D2 네 원장 SQLite integrity check 성공, run lineage 각 1개

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
건전성을 한 오류로 표시하는 점이다. 향후 개선 시 immutable 검증을 약화하거나
기존 C2 계좌를 덮어쓰지 말고, 두 상태를 명확히 분리해 표시하는 방향으로
검토한다.

## 다음 세션용 복사 프롬프트

```text
현재 CoinPilot 저장소 운영을 이어서 맡아줘.

먼저 ~/.codex/AGENTS.md, 저장소의 AGENTS.md,
docs/OPERATIONS_HANDOFF.md, docs/MAC_STUDIO.md, SECURITY.md를 읽어.
handoff는 2026-07-22 시점 스냅샷이므로 그대로 믿지 말고 git 상태와 실제
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
- 사용자가 명시하지 않으면 Slack notifier를 켜지 말 것.

D2 d2-btc/d2-eth/d2-xrp/d2-sol의 status와 doctor, 8774~8777 /api/status를
확인하고, C2 c2-btc/c2-eth/c2-xrp/c2-sol의 status와 doctor,
paper revision·updated_at·ACTIVE 및 설치본 verify-installed 상태를 확인해.
현재 알려진 C2 Doctor의 repo-source manifest drift 1건과 추가 오류를 구분해.
fresh warmup은 stopped가 아니고, C2의 장시간 무체결도 시간봉 특성상 정상일 수
있어.

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
