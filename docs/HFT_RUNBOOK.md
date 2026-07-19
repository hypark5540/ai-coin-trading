# HFT Public-Data Runbook

이 절차는 공개 업비트 호가·시장 체결을 수집하고 연구용으로 재생합니다.
API Key, 계좌 조회, 실제 주문은 사용하지 않습니다.

## 1. 연속 수집

먼저 짧은 smoke capture로 디스크·네트워크·manifest를 확인합니다.

```bash
coinpilot --config config.toml hft-record \
  --seconds 60 \
  --partition-seconds 3600 \
  --capture-id krw-btc-smoke \
  --output-root artifacts/hft/archive
```

정상 확인 항목:

- `written_events > 0`, `rejected_messages = 0`
- 결과의 모든 `.jsonl.gz`마다 같은 stem의 `.manifest.json` 존재
- `<capture-id>.reservation.json`으로 ID 단일 사용 보장
- `<capture-id>.run.json`에 요청시간·실제 monotonic 경과·counter 보존
- `<capture-id>.complete.json`으로 reservation/run/data/manifest hash 검증
- manifest의 `sha256_scope = compressed_file_bytes`
- `.partial` 파일 0개
- `connection_count`, `reconnect_count`, `gap_count`, error counter 기록
- 이벤트의 `received_wall_ns`, `received_monotonic_ns`가 문자열
- 공개 trade를 내 계좌 체결로 분류하지 않음

장기 실행은 명시적으로 유한한 `--seconds`를 지정합니다. 터미널 종료보다
프로세스 supervisor의 정상 종료 신호와 충분한 종료 유예시간을 권장합니다.
`Ctrl-C`가 들어오면 건강한 현재 파티션을 먼저 완결합니다. 완결이 실패하면
부분 파일을 승격하지 않고 제거합니다. JSONL 쓰기 도중 인터럽트된 파티션은
tainted로 간주해 폐기합니다. manifest를 먼저 승격한 뒤 data 파일을 마지막
commit point로 승격하므로 최종 gzip만 있고 검증자가 없는 상태도 만들지 않습니다.
capture ID reservation은 중단 뒤에도 남습니다. 파티션이 전혀 없음을 확인하지
않고 reservation을 지우거나 같은 ID를 재사용하면 안 됩니다.

60초 실측 표본의 압축 전 속도를 단순 연장하면 30일에 약 2,583만 이벤트,
71.7GB입니다. 실제 압축률·시장 활동은 달라지므로 최소 여유공간과 보존 정책을
먼저 정하고 디스크 사용량을 별도 감시해야 합니다.

## 2. 무결성·연속성

연구 입력 reader는 압축 archive에 finalized manifest를 필수로 요구하고,
분석 전에 압축 파일 SHA-256, 저장 byte 수, 레코드 수, capture ID,
partition 안팎의 전역 ordinal 연속성을 검증합니다. manifest 없는 직접 입력은
기존 bounded `.jsonl`에만 허용됩니다. 신뢰된 legacy gzip은 라이브러리 API에서
`allow_unmanifested_gzip=True`를 명시한 경우에만 읽을 수 있고 recorder
디렉터리에는 사용하면 안 됩니다. 다음은 데이터 경계입니다.

- `capture_id` 변경
- `connection_id` 변경
- `gap_before = true`
- archive ordinal 누락·중복·역행
- receive monotonic timestamp 역행
- 허용한 최대 orderbook 간격 초과

UTC 파일 회전만 일어났고 capture/connection이 같다면 경계가 아닙니다.
wall-clock과 거래소 timestamp 차이는 호스트 시계 오프셋을 포함하므로
네트워크 latency로 사용하지 않습니다.

## 3. 인과적 dataset

```bash
coinpilot --config config.toml hft-features \
  --input artifacts/hft/archive/market=KRW-BTC/date=YYYY-MM-DD \
  --trailing-window-ms 1000 \
  --horizons-ms 100,1000,5000 \
  --max-label-overshoot-ms 250 \
  --output artifacts/hft/features-YYYY-MM-DD.jsonl.gz
```

dataset 점검:

- decision row는 orderbook arrival에만 생성
- L1/L5 imbalance, microprice, spread, trailing signed trade flow 존재
- feature는 해당 arrival 이전 공개 체결만 반영
- 각 label에 `valid`, label end timestamp, invalid reason 존재
- gap·재접속을 가로지르는 label 0개
- 목표 horizon보다 250ms 넘게 늦은 첫 호가는 stale label로 무효화
- 각 행의 `source_ordinal`로 원본 archive와 감사 조인 가능
- 입력 combined SHA-256과 파일별 manifest 검증 여부가 summary에 기록
- dataset·summary hash를 담은 `.complete.json`이 있을 때만 bundle 완료로 취급

자동 소비자는 `.complete.json`의 존재만 보지 말고 라이브러리
`verify_artifact_completion()`으로 모든 member의 저장 byte 수와 SHA-256까지
검증해야 합니다.

dataset·summary는 최종 경로를 건드리기 전에 숨은 sibling에 모두 staging합니다.
`--overwrite`에서도 staging이 실패하면 기존 완료 marker가 유지됩니다. 승격이
시작된 뒤 실패하면 marker가 없으므로 해당 bundle을 사용하면 안 됩니다.
marker 무효화·모든 member 승격·새 hash marker 생성은 같은 OS file lock 안에서
직렬화되므로 같은 출력에 동시 `--overwrite`를 실행해도 서로 섞이지 않습니다.

기본 명령은 실수로 한 달 전체를 메모리에 올리지 않도록 100만 입력 레코드에서
중단하고, 한 연결 segment의 exact public-trade ID는 기본 25만 개에서
fail-closed합니다. 더 큰 작업은 날짜/hour 범위를 나누거나 disk-backed
trade-ID deduplicator를 제공한 streaming
`CausalHFTFeatureBuilder`를 유지한 채 파티션을 순서대로 feed합니다. 일반 파일
회전에서 builder를 finalize하면 경계 label을 불필요하게 잃으므로, 논리적
archive 끝에서만 finalize합니다.

## 4. visible-depth 비용 replay

```bash
coinpilot --config config.toml hft-depth-replay \
  --input artifacts/hft/archive/market=KRW-BTC/date=YYYY-MM-DD \
  --quote-notional 250000 \
  --side buy \
  --latency-ms 100 \
  --max-book-gap-ms 1000 \
  --output artifacts/hft/depth-replay-YYYY-MM-DD.json
```

summary에서 확인할 값:

- full/partial/invalid/unavailable 수
- L1 전량체결 수와 여러 단계 소진 수
- levels consumed p50/p95/max
- best/mid 대비 slippage p50/p95
- 전체 주문 기준 `execution_rate`, `full_fill_rate_all_orders`
- 실행된 주문만의 `conditional_visible_depth_full_fill_rate`
- decision-to-selected-book monotonic 시간 p50/p95
- 기본/2배 수수료 합계와 동일 체결 선택 여부
- 첫·마지막 수신 사이 실제 표본 span, gap reason, regression, reconnect 구간
- executions·summary hash를 담은 `.complete.json`

이 replay는 같은 주문을 실제로 전송하지 않습니다. 독립 주문들이 서로 잔량을
소진하지 않으므로 전략을 고빈도로 실행했을 때의 내생적 impact보다 낙관적일 수
있습니다. best 대비 book-walk 비용을 “실제 시장충격”이라고 부르면 안 됩니다.
`receive_interval` gap은 임계값보다 긴 관측 침묵이지 그 자체로 패킷 유실이
확정됐다는 뜻이 아닙니다.

bounded `hft-capture`의 raw·quality·public-trades·run 파일과
`hft-simulate`의 summary·execution·decision·equity 파일도 같은 staging/hash
completion 절차를 사용합니다. 합성 결과를 같은 디렉터리에 교체하려면
`hft-simulate --overwrite`를 명시합니다.

## 5. 상시 shadow

Mac Studio 설치 전 로컬에서 bounded observe smoke를 먼저 실행합니다.

```bash
coinpilot --config config.toml shadow-run \
  --seconds 60 --max-events 1000 --capture-id shadow-smoke
coinpilot --config config.toml shadow-status
```

확인 항목:

- `written_events > 0`, `consumer_errors = 0`
- `orders_sent = 0`, `live_order_routing = false`
- shadow run/reservation/completion marker 존재
- callback 전에 checksum audit WAL이 durable하고 정상 archive commit 뒤 제거됨
- kill -9 뒤 WAL의 exact `(capture, connection, ordinal)` 원본이 복구됨
- 같은 shadow DB의 두 번째 runner가 DB mutation 전에 즉시 거부됨
- pending 주문이 gap·connection 변경·재시작을 가로질러 fill되지 않음
- DB state, 결정, 주문, fill, equity, outbox가 idempotent
- observe에서는 결정·fill이 0
- notifier 장애 중에도 archive와 account transaction은 계속됨
- `/health/live`는 응답하고 stopped/stale run의 `/health/ready`는 503

초기 설치 후 48~72시간은 `shadow.mode = "observe"`를 유지합니다. diagnostic
모드는 설치·체결 배관을 검증할 때만 새 run으로 시작하며, 성과를 alpha 증거로
사용하지 않습니다. 설정 변경은 실행 중 hot reload하지 말고 서비스를 멈춘 뒤
검토합니다. 전체 설정·모델·리스크 또는 실제 소스 fingerprint가 바뀌면 기존
원장의 HALT는 재시작으로 풀리지 않습니다. 검토 후 새
`shadow.database_path`를 지정해 새 shadow 계좌로 시작합니다.

장애주입에는 SIGTERM, kill -9, 네트워크 단절, connection gap, DB lock, 디스크
warning/halt, Slack 429를 포함합니다. 재시작 중 pending은 만료되고 열린 모의
포지션은 과거 호가 소급 없이 첫 fresh book에서 청산되어야 합니다. Mac 전원
상실은 같은 호스트 watchdog가 Slack으로 알릴 수 없으므로 별도 off-device
dead-man이 필요합니다.

## 6. 승격 gate

실제 주문 코드를 추가하기 전 최소 gate:

1. 30일 이상 연속 공개피드, feed 가동률 99.9% 이상
2. 모든 gap·재접속·중복·거부 레코드의 원인 설명 가능
3. 고정 입력 hash에서 feature/replay 결과 재현
4. purged walk-forward와 별도 미접촉 30일 검증
5. 최소 1,000회 독립 왕복 의사결정, 기본비용과 2배비용 보고
6. p95 지연·2배 수수료에서도 순비용 후 edge 양수
7. 4주 이상 주문 없는 shadow 운용, 중복 결정·stale feed 위반 0
8. 실제 주문·private fill 수집은 별도 코드 리뷰와 사용자 승인

연 20%는 목표 gate이지 예상 수익률이 아닙니다. 약 5bp/일 복리 목표를 짧은
event-window 수익률로 연환산하지 말고, 수개월 일별 순자산과 실제 비용으로
평가해야 합니다.
