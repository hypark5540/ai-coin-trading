# Architecture

```text
Upbit public REST
        │
        ▼
validation ──► SQLite candle cache
        │
        ▼
gap-aware features + next-open labels
        │
        ▼
purged rolling walk-forward logistic model
        │
        ▼
probability ──► deterministic RiskManager
                         │
                         ▼
        deterministic accounting + RiskManager
                    │             │
                    ▼             ▼
             bar backtest   ticker-price paper
```

신호 모델은 주문이나 잔고를 직접 수정할 수 없습니다. 모델은 확률만 만들고,
`RiskManager`가 ATR 유효성, 손실 예산, 최대 allocation, 가용 현금, 최소 주문
금액을 검사합니다. 승인된 intent만 `SimulatedBroker`가 체결하고, 모든 fill은
수수료와 adverse slippage를 포함합니다.

## Timing contract

Upbit minute candle timestamp는 봉 시작 시각입니다. 백테스트에서는 봉 `t`가
완전히 마감된 뒤 생성한 확률이 정확히 다음 연속 봉 `t+1`의 open에서만 실행
대상이 됩니다.

Paper에서는 이미 마감된 `t+1`의 과거 open을 체결가로 쓰지 않습니다. 새 마감
봉을 발견한 호출에서 그 신호를 계산하고, 모델 계산 뒤 새로 조회한 공개 ticker의
가격·timestamp로만 체결합니다. 첫 실행은 체결 없이 prime하며, 한 봉보다 긴
downtime은 과거 fill replay 대신 `HALTED`를 만듭니다.

캔들 마감 판정은 첫 API 응답의 HTTP `Date`와 로컬 요청 시각 중 더 이른 값을
사용합니다. ticker는 로컬 관측 시각뿐 아니라 거래소의 마지막 체결 시각도 봉
마감 이후여야 합니다. 이 두 시각을 분리해 로컬 시계가 빠른 경우 아직 열린 봉의
신호를 실행하는 것을 막습니다. 거래소 시각은 일반 갱신 `timestamp`가 아니라
실제 마지막 체결의 `trade_timestamp`에서 읽고, 마지막으로 처리한 거래소 체결
시각도 state에 저장해 역행 응답을 거부합니다.

학습 행 `j`의 label은 `open[j+1]`부터 `open[j+horizon+1]`까지의 수익입니다.
prediction 행 `i`에서 사용하는 학습 행은 다음을 만족합니다.

```text
j <= i - horizon - 1
label_end_time[j] <= timestamp[i]
```

따라서 현재 예측에 아직 완성되지 않은 미래 label이 들어가지 않습니다.

## Gap behavior

- feature/label: gap마다 독립 segment로 계산하며 gap을 가로지르지 않습니다.
  각 추가 segment의 72봉 feature warm-up과 label purge 손실만큼 최소 이력
  요구량을 늘립니다.
- backtest: stale signal을 삭제하고 열린 포지션은 gap 다음 open에서 보수적으로
  청산하지만, feature warm-up 뒤 연구를 계속합니다.
- paper: 연속된 새 봉 하나만 처리합니다. 그 이상을 놓치면 데이터 연속성과
  체결 가능 시각을 신뢰할 수 없으므로 `HALT_PENDING` 또는 `HALTED`로 전환하고
  자동 재개하지 않습니다. gap-adjusted 유효 이력이나 최신 segment warm-up이
  부족하거나 모델이 준비되지 않아도 신규 진입을 막되, 기존 포지션의 ticker
  기반 청산 경로는 계속 실행합니다. 보유 중에는 이 risk-only ticker 검사를
  캔들 수집과 모델 계산보다 먼저 원자 저장합니다.

## Persistent paper account

SQLite의 `paper_state`와 `paper_events`를 한 transaction에서 기록합니다.
event ID는 account key로 scope되고, paper state의 revision을 compare-and-swap해
동시 runner의 stale write를 거부합니다. CLI는 계좌별 OS file lock도 잡아 서로
다른 ticker snapshot의 commit 순서가 뒤집히지 않게 합니다. transaction 전
crash는 state/event가 함께 rollback되고, commit 뒤 재시작은 `last_bar_time`과
revision을 확인하므로 중복 fill이나 상태 rewind를 만들지 않습니다.

Paper state에는 model/risk 설정 fingerprint가 저장됩니다. fingerprint가 달라진
설정으로 같은 계좌를 재개하면 실패하며, 설정 변경은 새 account name 또는
명시적 migration이 필요합니다. DB와 artifact 경로는 config 파일 위치를 기준으로
해석됩니다.

## Live boundary

private key, JWT, `/v1/orders`, `/v1/orders/test` 호출 코드는 존재하지 않습니다.
CLI의 `live` subcommand는 설명과 함께 실패합니다. live broker는 paper 검증과
사용자 승인 후 별도 변경으로만 추가합니다.

## HFT research boundary

HFT 연구 경로는 시간봉 엔진과 실제 주문 경로에서 분리합니다.

```text
Upbit public WebSocket
   ├─ orderbook (aggregated L2, up to 30 levels)
   └─ trade (public market executions)
                │
                ▼
lossless ID/time normalization
                │
                ├─► bounded atomic JSONL + quality profile
                │
                └─► reconnect/PING recorder
                        │
                        ▼
              UTC gzip partitions + SHA-256 manifests
                        │
             ┌──────────┴──────────┐
             ▼                     ▼
 causal L1/L5/flow features   monotonic-latency replay
 + explicit future labels     + multi-level taker sweep
                              + partial visible fills
             │                     │
             └──── hash-verified `.complete.json` bundles
                │
                ▼
quality profile: errors / duplicates / regressions / spread / clock

deterministic synthetic events
                │
                ▼
current-event signal ──► event-step latency ──► ask buy / bid sell
                                                  │
                                                  ▼
                          simulated execution/equity/decision artifacts
```

공개 체결은 시장 전체 관측이며 CoinPilot 주문 체결이 아닙니다. 공개 주문장은
가격대별 집계 잔량만 제공하므로 개별 주문의 queue position을 추론하지 않습니다.
기존 합성 replay는 가격 면에서는 불리한 taker 체결을 사용하지만, 최우선
호가에서 전량 체결되고 시장충격이 없다는 유동성 가정이 남아 있습니다. 별도
`hft_depth` 경로는 archive의 표시 잔량을 가격 순서로 걷고 부족분을 부분체결로
남깁니다. 다만 각 주문은 독립적이며 이후 snapshot 잔량을 감소시키지 않고,
숨은 유동성·재보충·내생적 시장충격도 추정하지 않습니다.

나노초 epoch와 공개 체결 ID는 IEEE-754 안전 정수 범위를 넘으므로 JSONL에서는
문자열로 보존하고, 품질 계산 때만 Python 정수로 변환합니다. 거래소 이벤트
시각과 로컬 wall-clock 수신 시각의 차이는 시계 오프셋을 포함하므로 순수
네트워크 지연으로 부르지 않습니다. 재생 지연 보정 전에는 monotonic inter-arrival,
NTP offset, 처리 지연을 따로 측정해야 합니다.

피처는 archive arrival order에서 이미 받은 체결만 사용하고, 미래 mid는
명시적인 label에만 들어갑니다. capture/connection 변경, gap, monotonic 역행은
segment 경계이며 이를 가로지르는 label이나 latency fill은 거부합니다. 전역
archive ordinal의 누락·중복·역행도 경계 또는 입력 오류이고, label 목표 이후
허용 overshoot를 넘긴 주문장은 stale label로 무효화합니다. archive 파일 회전
자체는 같은 connection이고 ordinal이 연속이면 시장 경계로 취급하지 않습니다.

gzip archive는 data와 finalized manifest가 모두 있어야 연구 입력으로 인정합니다.
recorder 실행시간과 counter는 별도 run summary에도 남깁니다. capture ID는
녹화 전에 durable reservation으로 단 한 번만 할당하고, 정상 종료 시
reservation·run summary·모든 data/manifest를 full-run completion marker로
묶습니다. 따라서 Ctrl-C나 hard crash 뒤 같은 ID로 ordinal을 다시 1부터
쓸 수 없습니다. bounded capture,
feature, depth replay, synthetic study의 여러 출력 파일은 먼저 고유한 숨은
sibling에 모두 쓰고, 최종 경로로 승격한 뒤 모든 저장 byte hash를 담은
`.complete.json`을 마지막에 생성합니다. `overwrite=False` 승격은 hard link의
원자적 no-clobber를 사용합니다. marker 무효화부터 모든 파일 승격과 새 marker
hash 생성까지 completion marker별 OS file lock 안에서 실행하므로 동시
`--overwrite` 두 프로세스의 member가 섞이지 않습니다. 기존 bundle을 덮어쓸
때도 새 파일 staging이 전부 성공한 뒤에만 이전 marker를 무효화하므로
입력·계산 실패가 기존 완성본을 decommit하지 않습니다. 승격 중 실패하면
marker가 없으므로 혼합 산출물을 완료 실행으로 오인하지 않습니다.

이 분기는 공개 데이터 수집과 연구용 독립 체결 replay만 지원합니다. private
`myOrder`, 주문 생성, 취소/재주문, maker fill, 실제 계좌 reconciliation은
구현하지 않으며 기존 `live` hard lock을 우회하지 않습니다.

## HFT shadow boundary

상시 shadow는 연구 replay와 별도 상태형 계좌지만 동일한 공개 archive envelope를
입력으로 사용합니다.

```text
Upbit public WebSocket
        │
        ▼
normalize + connection/gap metadata
        ├──────────────► UTC gzip archive + manifest
        ├──────────────► full-sync checksum audit WAL
        │
        ▼
immediate L5/flow snapshot (future label 없음)
        │
        ▼
observe 또는 diagnostic intent
        │
        ▼
risk gate ──► receive-monotonic latency queue
        │                    │
        │                    ▼
        │        first fresh same-connection book
        │                    │
        │                    ▼
        │          visible-depth taker sweep
        │                    │
        └──────────────► SQLite WAL transaction
                         ├─ state/decision/order/fill/equity
                         ├─ health heartbeat
                         └─ notification outbox
                                      │
                         ┌────────────┴────────────┐
                         ▼                         ▼
                  Slack notifier           localhost dashboard
```

의사결정 주문은 다음 호가가 latency deadline에 도달한 뒤에만 체결됩니다.
중간 trade envelope에 gap marker가 있어도 그 경계를 다음 orderbook으로
전파하므로 pending 주문이 gap을 건너 체결되지 않습니다. 공개 trade는
`own fill`이 아니며 flow feature로만 사용합니다.

DB는 한 시장 event의 결정, 주문, 체결, 계좌, outbox를 같은 transaction에
저장합니다. ID는 run/capture/connection/ordinal/config에서 결정론적으로 만들고,
재시작 시 pending 주문을 만료해 중복 fill을 방지합니다. 열린 모의 포지션이
남은 재시작은 첫 fresh book에서 `system-recovery-v1` taker 청산을 수행한 뒤
warm-up을 다시 시작합니다. 설정 또는 code fingerprint가 달라지거나 외부
risk HALT가 발생하면 자동 신규 진입을 허용하지 않습니다.

gzip은 처리량을 위해 파티션 종료까지 buffering할 수 있지만, callback 전에
canonical envelope와 checksum을 별도 WAL에 `F_FULLFSYNC`(macOS) 또는 `fsync`
합니다. hard crash 뒤에는 checksum-valid prefix로 gzip+manifest를 복구하고,
손상된 complete frame은 격리한 뒤 fail-closed합니다. 정상 archive commit
뒤에만 WAL을 제거합니다. 따라서 durable decision/fill은 복구 가능한
`capture_id/connection_id/ordinal` 공개 원본을 가집니다.

shadow DB별 커널 `flock`은 서비스 전체 수명 동안 유지되어 launchd와 수동
runner의 중복 실행을 DB mutation 전에 거부합니다. 재시작 gate는 전체 shadow
설정·모델·리스크·운영 guardrail의 canonical hash와 Git commit·실제 Python
source content hash를 결합합니다. 불일치 HALT는 단순 재시작으로 해제되지
않으며 새 shadow DB를 명시적으로 선택해야 합니다.

Equity와 feed health는 각각 기본 5초와 10초 간격으로 샘플링하되 결정·fill·gap은
강제 기록해 장기 DB 증가를 제한합니다. notifier는 outbox lease와 retry를
사용하므로 Slack 장애가 feed writer transaction을 막지 않습니다. dashboard는
loopback bind와 GET endpoint만 지원합니다.

`shadow.mode = "observe"`는 수집과 상태 검증만 수행합니다. `diagnostic`은
파이프라인을 운동시키는 명시적 휴리스틱일 뿐 동결 OOS alpha가 아니며, 두
모드 모두 exchange credential, 주문 endpoint, live broker를 갖지 않습니다.
