# CoinPilot

업비트 공개 시세로 동작하는 **AI 코인 트레이딩 연구·모의매매 시스템**입니다.
수익을 가정하지 않고, 과거 데이터 누수와 과장된 체결 가정을 줄이는 데 초점을
맞췄습니다.

기본 설정은 `KRW-BTC` 60분봉, 현물 long/cash 전용입니다. 레버리지·공매도·실제
주문은 지원하지 않습니다.

## 현재 구현 범위

- 업비트 분봉 REST API 수집과 200개 제한 역방향 pagination
- UTC 정규화, 중복·OHLCV 검증, HTTP 서버 시각 기반 미완성 봉 제외, 누락 봉 audit
- 과거 수익률·장단기 이동평균 괴리·변동성·RSI·ATR·거래량 특징
- NumPy 기반 연속수익률 ridge 회귀와 학습구간 전용 winsorization
- 시간순 보정 구간에서 기울기·절편·오차를 다시 추정하는 calibration
- 겹치는 선행수익률의 유효표본수를 보정한 불확실성 buffer
- horizon purge가 적용된 rolling walk-forward 예측
- `t` 봉 마감 신호를 `t+1` 봉 시가에 체결
- 진입 목표와 일치하는 고정 horizon 시가 청산
- 양방향 수수료와 adverse slippage
- ATR 손절 거리 기반 포지션 크기, 최대 노출, trailing stop, cooldown
- 최대 낙폭 도달 시 `HALT_PENDING → HALTED`
- 동일 회계·리스크 primitive를 쓰는 백테스트와 영속 모의매매
- SQLite 캔들 캐시, 모의 계좌 상태, revision CAS, idempotent event ledger
- 기본 비용과 2배 비용 stress backtest
- 명시적 1년 확인구간과 20% 목표 gate를 쓰는 research assessment
- 업비트 공개 WebSocket 30단계 호가·체결의 bounded JSONL 수집과 품질 profile
- 재접속·PING·UTC gzip 파티션·SHA-256 manifest를 갖춘 연속 공개피드 archive
- monotonic 수신시각 기준 인과적 L1/L5·microprice·체결흐름 피처와 미래 라벨
- 다호가 visible-depth sweep, 부분체결, 수수료·지연·gap 검증 replay
- 현재 이벤트만 사용하는 합성 HFT 신호, ask 매수/bid 매도, 지연·비용 stress
- 공개 시장 체결·모의체결·내 계좌 체결 provenance의 명시적 분리
- 동일 공개피드를 archive와 stateful shadow 원장에 fan-out하는 상시 런타임
- 소비 전 full-sync audit WAL, 지연 후 taker 부분체결, 재시작 복구, Slack outbox
- 개별 이벤트를 묶은 KST 고정 1시간 Block Kit Slack 요약과 중복 생성·backlog 폭주 방지
- 전체 설정·실제 소스 fingerprint와 커널 singleton으로 안전한 재배포
- localhost 전용 읽기 dashboard와 Mac Studio launchd 원클릭 운영 도구
- 실제 주문을 거부하는 hard live gate

## 빠른 시작

Python 3.11 이상이 필요합니다.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp config.example.toml config.toml
pytest
```

네트워크 없이 전체 파이프라인을 확인합니다.

```bash
coinpilot --config config.toml demo
```

업비트 공개 60분봉을 저장하고 백테스트합니다. API Key는 필요 없습니다.

```bash
coinpilot --config config.toml sync
coinpilot --config config.toml backtest --offline
```

`backtest`에서 `--offline`을 빼면 실행 전에 최신 공개 데이터를 동기화합니다.
결과는 기본적으로 `artifacts/`에 summary JSON, equity/trade/prediction CSV로
저장됩니다.

한 후보를 동결한 뒤 개발구간과 정확히 365일의 역사적 확인구간을 분리해
평가하려면 경계를 명시합니다.

```bash
coinpilot --config config.toml research --offline \
  --holdout-start 2025-07-19T07:00:00Z \
  --holdout-end 2026-07-19T07:00:00Z
```

이 명령은 확인구간에서 현금·무포지션 계좌로 다시 시작하며, 기본 비용과 비용
2배 실행이 같은 동결 예측을 재사용하는지 검증합니다. 통과 기준은 연환산 수익률
20% 이상, MDD 15% 이하, profit factor 1.15 이상, 비용 2배 수익률 양수입니다.
20%는 목표이자 검증 기준이지 예상 또는 보장 수익률이 아닙니다.

## HFT 연구

실제 주문 없이 업비트 공개 호가·체결만 정해진 시간 동안 기록합니다.

```bash
coinpilot --config config.toml hft-capture \
  --seconds 60 \
  --output artifacts/hft/live-events.jsonl
```

출력 JSONL은 거래소 시각과 로컬 수신 시각, 공개 체결 ID, 최우선 호가와 최대
30단계 호가를 보존합니다. 품질 sidecar는 필드 오류, 중복 체결 ID, 타임스탬프
역행, 비정상 스프레드와 이벤트율을 집계합니다. 나노초 epoch와 체결 ID는
JavaScript 안전 정수 범위를 넘을 수 있어 문자열로 직렬화합니다. raw·quality·
public-trades·run 파일은 먼저 숨은 sibling에 모두 만든 뒤 hash
`.complete.json`을 마지막에 기록합니다.

장시간 자료는 bounded capture 대신 재접속 가능한 archive 명령으로 수집합니다.
다음 예시는 실제 주문 없이 1시간 동안 공개피드만 기록합니다.

```bash
coinpilot --config config.toml hft-record \
  --seconds 3600 \
  --partition-seconds 3600 \
  --output-root artifacts/hft/archive
```

각 이벤트에는 capture/connection ID, 전역 ordinal, wall-clock과 monotonic
수신시각, 직전 이벤트 간격, gap 사유가 들어갑니다. 자료는 UTC 파티션의
`.jsonl.gz`로 쓰고, 압축 파일 바이트의 SHA-256·레코드 수·연결/gap counter를
manifest에 기록합니다. manifest를 먼저 승격하고 data 파일을 commit point로
마지막에 승격하며, reader는 hash·byte·레코드 수·ordinal 연속성을 모두
검증합니다. 요청시간·실제 monotonic 경과·counter와 생성 파일 목록은
`<capture-id>.run.json`에도 보존합니다. capture ID는 녹화 전에 원자적으로
영구 예약하므로 중단·crash 후 같은 ID로 ordinal 1부터 다시 시작할 수 없습니다.
정상 종료 시 reservation·run summary·모든 data/manifest hash를
`<capture-id>.complete.json`에 묶습니다. 재접속은 capped exponential backoff를 쓰며,
idle 연결에는 업비트가 안내한 `PING`을 보냅니다. `Ctrl-C` 시 정상인 현재
파티션을 완결하고, 쓰기 도중 끊긴 tainted 파티션은 `.partial`과 함께 제거합니다.

archive에서 미래 누수 없는 연구 dataset을 만듭니다.

```bash
coinpilot --config config.toml hft-features \
  --input artifacts/hft/archive/market=KRW-BTC/date=YYYY-MM-DD \
  --trailing-window-ms 1000 \
  --horizons-ms 100,1000,5000 \
  --max-label-overshoot-ms 250 \
  --output artifacts/hft/features-YYYY-MM-DD.jsonl.gz
```

피처는 해당 결정 호가보다 먼저 도착한 공개 체결만 사용합니다. 미래 호가는
`labels` 안에서만 첫 `decision+horizon` 이후 mid-return으로 붙습니다.
재접속, 명시적 gap, connection 변경, monotonic 역행을 가로지르는 피처·라벨은
만들지 않고, archive ordinal이 빠져도 새 segment로 분리합니다. 목표 horizon보다
기본 250ms 넘게 늦은 첫 주문장은 stale label로 무효화합니다. 각 행에는 원본
`source_ordinal`과 공개데이터 provenance가 남습니다. 기본 입력 상한은 100만
레코드, 연결 segment별 exact trade-ID 상한은 25만 개이며, 증액 전에 UTC 파티션
범위와 메모리 용량을 먼저 확인해야 합니다. 라이브러리 API
`CausalHFTFeatureBuilder`는 파일 경계에서 finalize하지 않고 이어서 feed하면
미완성 라벨 행을 streaming 처리합니다. 장시간 연결에서 25만 ID를 넘길 때는
disk-backed deduplicator가 필요합니다. dataset과 summary는 두 파일의 hash를
담은 `.complete.json`이 마지막에 생겨야 완료된 bundle입니다.

25만원 독립 taker 주문을 실제 archive의 여러 호가 단계로 replay합니다.

```bash
coinpilot --config config.toml hft-depth-replay \
  --input artifacts/hft/archive/market=KRW-BTC/date=YYYY-MM-DD \
  --quote-notional 250000 \
  --side buy \
  --latency-ms 100 \
  --output artifacts/hft/depth-replay-YYYY-MM-DD.json
```

replay는 결정 monotonic 시각에 latency를 더한 뒤 같은 연결의 첫 유효 호가만
선택합니다. ask/bid를 가격 순서로 소진하고, 표시 잔량이 모자라면 부분체결로
남깁니다. 결과에는 VWAP, 사용한 호가 단계, best/mid 대비 book-walk
slippage, 전체 주문 기준 체결률·전량체결률, decision-to-selected-book 시간,
수수료와 주문별 불변성을 확인한 2배 수수료 stress가 포함됩니다. executions와
summary 역시 `.complete.json` hash marker로 함께 커밋됩니다. 이는 독립 주문의
체결비용 연구이며 포트폴리오 손익률이나 연 20% 달성 검증은 아닙니다.

다음 명령은 실제 시장 이력이 아닌 결정론적 합성 이벤트에서 비용과 지연
메커니즘을 검증합니다.

```bash
coinpilot --config config.toml hft-simulate \
  --events 10000 \
  --seed 23 \
  --output-dir artifacts/hft/simulation
```

기본 실행은 무알파, 약한 알파, 동일 결정의 수수료 2배, 지연 5 이벤트,
손익분기 감각을 위한 강한 알파 예시를 비교합니다. 모든 모의체결은
`simulated=true`이며 단기 합성 수익률을 연환산하지 않습니다. summary·
execution·decision·equity 네 파일도 staging 후 `simulation.complete.json`으로
함께 commit하며, 기존 결과 교체에는 `--overwrite`가 필요합니다. 모든 다중 파일
교체는 completion marker별 OS 잠금 안에서 직렬화해 동시 overwrite의 혼합
bundle을 막습니다.

2026-07-19의 60초 공개 표본은 597건(호가 477, 공개 시장 체결 120)이었고,
파싱 오류·중복·역행·비정상 스프레드는 0건이었습니다. 그러나
`수신시각-거래소시각` 중앙값이 -111ms여서 시계 오프셋이 섞였고, 25만원
매수를 최우선 ask 한 단계에서 전량 소화하지 못하는 스냅샷이 35.8%였습니다.
따라서 기존 합성 replay의 L1 전량체결·무충격 가정은 실시장 적용 시
낙관적이며, 새 depth replay 결과를 함께 봐야 합니다.

| 합성 시나리오 | 이벤트 구간 수익률 | 왕복거래 | 해석 |
|---|---:|---:|---|
| 무알파·기본비용 | -1.133% | 410 | 비용 기준선 |
| 약한 알파 0.18bp/event | -1.081% | 410 | 비용 허들 미달 |
| 약한 알파·수수료 2배 | -2.106% | 410 | 동일 결정 stress |
| 약한 알파·지연 5 | -0.901% | 328 | 거래 감소; 거래당 손실은 악화 |
| 강한 알파 4bp/event 예시 | +0.032% | 410 | 임의 hurdle 예시, 수익 증거 아님 |

공개 집계 호가는 개별 주문의 queue position을 제공하지 않으므로 maker fill은
아직 시뮬레이션하지 않습니다. 다호가 sweep과 부분체결은 구현됐지만, 내 주문이
호가를 바꾸는 내생적 시장충격·숨은 유동성·잔량 재보충은 모델링하지 않습니다.
먼저 30일 이상 연속 데이터를 쌓고 NTP 오프셋을 별도 계측한 뒤 시간 분리
OOS와 shadow paper를 통과해야 합니다. 운영 절차와 승격 gate는
[`docs/HFT_RUNBOOK.md`](docs/HFT_RUNBOOK.md)에 정리했습니다. 최신 public
smoke와 feature/depth 재계산 결과는
[`docs/HFT_IMPLEMENTATION_RESULT.md`](docs/HFT_IMPLEMENTATION_RESULT.md)에
있습니다.

## HFT shadow 상시 운전

`shadow-run`은 하나의 공개 WebSocket envelope를 원본 archive와 shadow 엔진에
동시에 전달합니다. 실제 주문 클라이언트와 private Upbit API 키는 사용하지
않으며 모든 상태와 알림에 `orders_sent=0`을 유지합니다.

shadow 소비 전에 동일 envelope를 checksum audit WAL에 full-sync하므로 전원
차단으로 gzip tail이 사라져도 durable 원장 행의 공개 원본을 다음 시작에서
복구합니다. 같은 DB의 두 번째 runner는 커널 lock에서 즉시 거부됩니다.

안전한 기본값은 수집·상태 검증만 하는 `shadow.mode = "observe"`입니다.

```bash
coinpilot --config config.toml shadow-run
coinpilot --config config.toml shadow-status
coinpilot --config config.toml shadow-web
```

`diagnostic` 모드는 L5 호가 불균형과 최근 공개 체결 흐름을 이용해 전체
모의체결·원장·리스크 경로를 운동시키는 배관 검증용 휴리스틱입니다. 검증된 alpha
모델이나 수익 증거가 아닙니다. taker-only 현물 long/cash, 한 포지션·한 pending,
receive-monotonic 지연, visible-depth 부분체결만 지원합니다. gap이나 재접속을
가로질러 fill을 만들지 않고, 재시작 중 열린 모의 포지션은 첫 fresh book에서
보수적으로 청산합니다.

Mac Studio에서는 먼저 설치 preview를 확인한 뒤 적용합니다.

```bash
git clone git@github.com:hypark5540/ai-coin-trading.git
cd ai-coin-trading
./scripts/mac-studio bootstrap
./scripts/mac-studio slack set --apply
./scripts/mac-studio bootstrap --apply
./scripts/mac-studio doctor
```

설치기는 owner-only 디렉터리, 잠금 의존성, venv, secret 없는 설정, 사용자
LaunchAgent 7개(기본 비활성 C2 paper 포함), Keychain Slack, SQLite online
backup과 90일 retention을
멱등하게 구성합니다. Dashboard는 `http://127.0.0.1:8765`에만 열립니다.
전체 설치·업데이트·로그·복구 절차와 Terraform 대신 이 방식을 사용한 이유는
[`docs/MAC_STUDIO.md`](docs/MAC_STUDIO.md)에 있습니다.

BTC와 ETH 공개피드를 분리된 5백만원 observe 계좌로 동시에 운영할 때는 named
instance를 사용합니다. 각 인스턴스는 DB, archive/audit lock, backup, logs,
복사 설치된 코드, LaunchAgent와 dashboard port를 완전히 분리합니다.

```bash
./scripts/mac-studio bootstrap --instance btc --market KRW-BTC \
  --initial-cash 5000000 --order-quote 125000 \
  --shadow-mode observe --web-port 8766 --no-start --apply
./scripts/mac-studio bootstrap --instance eth --market KRW-ETH \
  --initial-cash 5000000 --order-quote 125000 \
  --shadow-mode observe --web-port 8767 --no-start --apply
./scripts/mac-studio start all --instance btc --apply
./scripts/mac-studio start all --instance eth --apply
```

두 계좌는 총자산을 공유하는 portfolio가 아니라 위험한도가 독립된 두 shadow
계좌입니다. Dashboard는 BTC `127.0.0.1:8766`, ETH `127.0.0.1:8767`이며 실제
주문 경로는 두 인스턴스 모두 비활성입니다. Slack은 체결 한 건마다 보내지 않고
직전 완료 KST 1시간의 손익·수수료·체결·승패·잔고·운영 이벤트를 시장별 한 건으로
요약합니다. 거래가 없는 시간에도 짧은 상태 heartbeat를 보내며, 즉시 이벤트
알림은 비활성입니다. `diagnostic` 모드는 배관 점검용 체결 시뮬레이션일 뿐
검증된 alpha가 아니며, Strategy Research V2 이후 운영 기본값으로 사용하지
않습니다.

손실 원인 재현이 필요한 경우에는 기존 고빈도 원장을 재개하지 않고
`d2-btc`, `d2-eth`, `d2-xrp`, `d2-sol`의 별도
`diagnostic-bounded-v1` 원장을 사용합니다. 각 계좌는 모의자금 5백만원,
주문금액 2만5천원이며 일손실과 peak drawdown의 하드 경계는 모두 10%입니다.
10% 이상이면 신규 진입을 차단하고 열린 모의 포지션을 정리한 뒤 halt를
원장에 영구 기록합니다. 누락된 owner-only 진단 JSON은 재시작 시 원장에서
복구합니다. 1시간 재진입 제한, KST 일 24회 진입/왕복 cap, 전체 주문손실
reserve, 체결시점 spread·visible-depth 재검사는 끌 수 없으며 잘못된 runtime
값은 일반 diagnostic으로 우회하지 않고 시작 실패합니다. 이는 손실 통제와
진단 개선이지 수익성 증명이 아니며, 자동 코드변경·자동 전략 승격은 하지
않습니다.

현재 D2 dashboard는 BTC `127.0.0.1:8774`, ETH `:8775`, XRP `:8776`,
SOL `:8777`입니다. Slack notifier는 명시적으로 비활성화되어 있고 모든 경로는
public-feed simulated, `orders_sent=0`입니다.

## Strategy Research V2 결과

기존 72시간 예상수익 모델과 초단기 diagnostic shadow의 손실을 본 뒤,
`KRW-BTC`, `KRW-ETH`, `KRW-XRP`, `KRW-SOL`을 각각 25% sleeve로 고정해
느린 예상수익(C1), 336/168시간 돌파 추세(C2), 두 전략의 50/50 혼합(C3)을
검사했습니다. 아래 수익률은 포트폴리오 전체의 `기본 비용 / 비용 2배` 결과입니다.

| 후보 | D1 2024-07~2025-01 | D2 2025-01~2025-07 | D3 2025-07~2026-07 | 개발 판정 |
|---|---:|---:|---:|---|
| C0 기존 ER72, 비교 전용 | -1.879% / -2.154% | +0.517% / +0.202% | +0.370% / -0.262% | 평가 제외 |
| C1 느린 ER168 | +0.488% / +0.095% | -1.528% / -1.934% | +0.444% / -0.048% | FAIL |
| C2 추세 돌파 336/168 | +2.869% / +2.192% | +3.594% / +3.084% | -3.914% / -4.308% | PASS |
| C3 C1/C2 50/50 | +1.679% / +1.144% | +1.033% / +0.575% | -1.735% / -2.178% | PASS |

D1·D2만 사용하는 동결 규칙에서는 C2가 다음 development challenger로
선정됐습니다. 그러나 D3는 설계 전에 이미 확인한 오염된 진단 구간이고, C2와
C3가 이 최신 구간에서 모두 손실이므로 승격 근거가 아닙니다. 활성 champion은
계속 `cash/observe-only`이며, 초단기 diagnostic 정책도 alpha로 재가동하지
않습니다. C2의 다음 증거는 변경 없는 새 forward-paper 원장에서 쌓아야 합니다.

동일한 고정 suite는 다음처럼 다시 실행합니다. 계산 코드·입력 캔들·후보 설정과
각 산출물의 SHA-256이 함께 기록되며, 거래소 주문 경로는 없습니다.

```bash
./scripts/run-strategy-research-v2 \
  --output-dir artifacts/strategy-v2/review-$(date +%F)
```

세부 gate와 오염 방지 규칙은
[`docs/STRATEGY_RESEARCH_V2.md`](docs/STRATEGY_RESEARCH_V2.md)에 있습니다.
20%는 장기 확인 목표이지 보장 수익률이 아닙니다.

## 모의매매

한 번 실행하면 최신 **마감 봉**을 기록하고 계좌를 준비만 합니다. 이후 새 마감
봉이 발견되면 그 시점에 다시 학습한 신호를 **호출 시점의 업비트 공개 현재가**로
모의 체결합니다. 이미 지나간 봉의 시가로 체결을 소급하지 않습니다.

```bash
coinpilot --config config.toml paper
coinpilot --config config.toml status
```

계속 관찰하려면 다음처럼 실행할 수 있습니다.

```bash
coinpilot --config config.toml paper --loop
```

모의 계좌와 event ledger는 `var/coinpilot.db`에 원자적으로 저장됩니다. 같은 봉은
재시작 후에도 두 번 처리하지 않습니다. 동시 runner는 revision compare-and-swap
외에 계좌별 프로세스 잠금으로 차단합니다. 한 봉보다 긴 downtime이나 데이터
간격이 발견되면 과거 체결을 replay하지 않고 계좌를 안전 중단합니다.
현재가 조회가 신호 마감 시각보다 기본 120초 이상 늦어도 stale signal로
중단합니다. ticker의 거래소 체결 시각이 봉 마감보다 이르거나, 조회 시각보다
기본 60초 이상 오래된 가격도 거부합니다. 각 gap의 feature warm-up 손실까지
포함한 유효 학습 이력이 부족하거나 모델이 준비되지 않으면 신규 진입 없이
중단하며, 기존 포지션이 있으면 관측한 ticker에서 먼저 청산합니다. 따라서
시간당 한 번 수동 실행하기보다 `paper --loop`를 사용해야 합니다.
가격 신선도에는 ticker 갱신 시각이 아니라 실제 마지막 체결의
`trade_timestamp`를 사용하며, 로컬 관측 시각과 거래소 체결 시각이 모두 이전
paper state보다 역행하지 않아야 합니다.

기존 포지션의 stop 검사는 캔들 동기화와 모델 계산보다 먼저 실행됩니다. 따라서
캔들 API나 모델이 실패해도 ticker API가 정상인 동안 현재 관측가의 stop을
원장에 반영하고, 모델 예외는 `model_not_ready`로 안전 중단합니다.

`paper`와 `status`는 반드시 `--config`를 요구합니다. DB와 artifact 상대 경로는
process CWD가 아니라 해당 config 파일의 디렉터리를 기준으로 절대 경로화됩니다.
설정 fingerprint가 달라지면 기존 paper 계좌를 이어 쓰지 않으므로, 전략이나
리스크 설정을 바꿀 때는 `paper.account_name`도 새로 지정합니다.
공개 데이터 API 주소와 명시적인 paper 전략 버전도 fingerprint에 포함됩니다.
loop 중 일시적인 API·SQLite 오류는 제한된 exponential backoff로 재시도하며,
복구 뒤 봉을 놓쳤다면 gap 규칙이 계좌를 중단합니다.

## 신호와 누수 방지

각 `t` 봉의 특징에는 그 봉 마감까지의 데이터만 들어갑니다.

```text
feature time = close[t]
entry time   = open[t+1]
label end    = open[t+horizon+1]
```

예측 시점 `i`의 학습 데이터는 label 종료 시각이 이미 관측된 행만 사용합니다.
누락 봉을 forward-fill하지 않으며, gap을 가로지르는 rolling feature와 label은
무효화합니다. scaler와 모델은 매 walk-forward 재학습 시 과거 행에만 fit됩니다.

기본 모델은 72시간 뒤의 gross 수익률을 직접 예측합니다. 최근 보정구간에서
예측값의 기울기와 절편을 다시 맞추고, 표본이 겹치는 정도만큼 유효표본수를
줄여 uncertainty buffer를 계산합니다.

```text
net edge = calibrated gross forecast
           - 왕복 수수료
           - 왕복 slippage
           - calibration uncertainty buffer
```

net edge가 `minimum_edge_pct` 이상이고 장기 추세 gate도 통과할 때만 다음 봉
시가에 진입합니다. 손절·risk halt가 먼저 발생하지 않으면 정확히 설정한 horizon
뒤 시가에 청산합니다. 기존 확률 로지스틱 모드는 호환용으로 남아 있지만 예제
설정의 기본은 기대수익 모드입니다. LLM은 주문 판단 경로에 들어가지 않습니다.

## 리스크 규칙

진입 quote 금액은 다음 세 상한의 최솟값입니다.

```text
equity × risk_per_trade / (ATR stop distance + 왕복 비용 buffer)
equity × max_position_fraction
available cash / (1 + fee)
```

ATR이 없거나 비정상적으로 크면 진입하지 않습니다. 손절 gap은 stop 가격으로
좋게 체결됐다고 가정하지 않고, 불리한 다음 시가에 slippage를 더해 처리합니다.
전략 equity가 peak 대비 설정된 최대 낙폭에 도달하면 신규 진입을 막고, 보유
포지션은 다음 체결 가능 시점에 한 번만 청산한 뒤 수동 검토 전까지 중단합니다.

## 명령

| 명령 | 동작 |
|---|---|
| `coinpilot demo` | 결정론적 합성 데이터 smoke test |
| `coinpilot sync` | 업비트 공개 캔들 수집 |
| `coinpilot backtest` | 기본/2배 비용 walk-forward backtest |
| `coinpilot research` | 개발/365일 확인구간과 20% 목표 gate 평가 |
| `coinpilot paper` | 새 마감 봉을 모의 계좌에 한 번 처리 |
| `coinpilot paper --loop` | 설정한 주기로 반복 |
| `coinpilot status` | 모의 계좌와 최근 event 조회 |
| `coinpilot hft-capture` | bounded 공개 호가·체결 JSONL 수집 |
| `coinpilot hft-record` | 재접속 가능한 UTC gzip 공개피드 archive |
| `coinpilot hft-features` | 인과적 피처와 시간 horizon 라벨 생성 |
| `coinpilot hft-depth-replay` | 다호가 독립 taker 체결·부분체결 replay |
| `coinpilot hft-simulate` | 결정론적 합성 HFT 비용·지연 연구 |
| `coinpilot live` | 실제 주문이 잠겨 있음을 확인 |

## 실거래 잠금

`coinpilot live`는 항상 종료 코드 3으로 거부됩니다. 이 저장소에는 private API
인증이나 실제 주문 전송 코드가 없습니다. 실거래 어댑터는 최소한 다음 검증 뒤
별도 리뷰·승인 과제로 구현해야 합니다.

- 여러 OOS 구간에서 비용 차감 후 일관된 성과
- 2배 비용에서도 치명적 붕괴가 없음
- 최소 30일 연속 paper 운용
- 중복 fill, 음수 잔고, 원장 불일치, risk 위반 0건
- API Key는 조회·주문 권한만 사용하고 출금 권한 제외
- 주문 테스트, 고유 identifier, timeout reconciliation
- 운용 자본 상한과 수동 arm 절차

## 알려진 한계

- 단일 종목·단일 포지션만 지원합니다.
- OHLCV backtest는 bar 내부 가격 순서, 부분 체결, spread, 시장 충격을 재현하지
  못합니다. full fill + 고정 adverse slippage 가정입니다.
- 새 visible-depth replay는 최대 30단계를 걷고 부분체결을 남기지만, 주문끼리
  잔량을 공유하지 않는 독립 replay이며 숨은 유동성·잔량 재보충·내생적
  시장충격·API 주문 왕복시간은 재현하지 않습니다.
- 공개 호가는 가격대별 집계 잔량이며 개별 주문 ID·queue 순위를 제공하지
  않으므로 maker 체결확률을 신뢰성 있게 계산할 수 없습니다.
- 짧은 공개 WebSocket 표본은 수집기 검증용이며 장기 시장 국면이나 수익성을
  대표하지 않습니다.
- paper stop은 실제 주문이 아니라 `paper --loop`가 관측한 ticker에서만 평가됩니다.
  polling 사이의 intrabar stop touch는 재현하지 않습니다.
- 거래가 드문 종목은 업비트가 무거래 분봉을 반환하지 않을 수 있습니다. 이
  시스템은 빈 봉을 임의 생성하지 않으므로 gap-adjusted 유효 이력 검사에서
  중단될 수 있습니다.
- 학습 AUC가 높아도 미래 수익이나 실제 체결 성과를 보장하지 않습니다.
- 현재 전략이 buy-and-hold를 이길 거라고 가정하지 않습니다. 비용 차감 후 edge가
  없으면 현금 유지 또는 `HALTED`가 정상 결과입니다.
- 세금·신고·거래소 약관 준수는 사용자가 확인해야 합니다.

## Upbit 사양

구현은 2026-07-19에 확인한 한국 공식 개발자센터 v1.6.3을 기준으로 합니다.

- [분 캔들 조회](https://docs.upbit.com/kr/reference/list-candles-minutes):
  `GET https://api.upbit.com/v1/candles/minutes/{unit}`, 요청당 최대 200개,
  `to`는 exclusive
- [요청 수 제한](https://docs.upbit.com/kr/reference/rate-limits):
  candle 그룹 초당 최대 10회/IP, `Remaining-Req` 확인
- [REST API 가이드](https://docs.upbit.com/kr/reference/rest-api-guide)
- [현재가 조회](https://docs.upbit.com/kr/reference/list-tickers-by-pairs)
- [WebSocket 호가](https://docs.upbit.com/kr/reference/websocket-orderbook)
- [WebSocket 체결](https://docs.upbit.com/kr/reference/websocket-trade)
- [WebSocket 연결·PING/PONG](https://docs.upbit.com/kr/reference/websocket-guide)
- [주문 생성 테스트](https://docs.upbit.com/kr/reference/order-test) — 현재
  코드에서는 호출하지 않음

가상자산은 변동성이 매우 크며 원금 전부를 잃을 수 있습니다. 이 프로젝트는
연구용 소프트웨어이며 투자 조언이나 수익 보장이 아닙니다.
