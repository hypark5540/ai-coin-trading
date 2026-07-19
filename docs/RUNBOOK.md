# Paper Runbook

## Start

```bash
cp config.example.toml config.toml
python3 -m coinpilot --config config.toml paper
python3 -m coinpilot --config config.toml paper --loop
```

첫 호출은 계좌를 prime하고 주문을 만들지 않습니다. loop가 새 마감 봉을 발견하면
모델 계산 뒤 조회한 공개 ticker 가격으로만 모의 체결합니다.
신호 마감 시각에서 기본 120초 이상 늦은 ticker는 체결하지 않고 `HALTED`로
전환합니다. 거래소의 마지막 체결 시각이 봉 마감보다 이르거나 ticker가 기본
60초 이상 오래된 경우에도 상태를 진행시키지 않습니다.

동일 account를 두 프로세스에서 실행하지 마세요. 실수로 실행하면 계좌별 file
lock이 두 번째 runner를 거부하고, revision CAS가 추가 방어선으로 남습니다.
rate limit과 운영 로그는 서로 다른 account 프로세스 간에는 공유되지 않습니다.

loop는 일시적인 공개 API·SQLite 오류를 최대 300초 간격으로 재시도합니다.
오류는 stderr의 `paper_loop_error` JSON으로 남습니다. 복구 시 한 봉보다 많이
놓쳤다면 정상 거래를 소급하지 않고 gap 규칙으로 계좌를 중단합니다.
봉 마감 이후 체결이 아직 없거나 ticker가 일시적으로 오래된 경우도 같은
backoff로 기다리며, 계좌 상태나 설정 불일치 오류는 재시도하지 않습니다.
보유 포지션의 ticker stop 검사는 캔들 동기화보다 먼저 수행되므로 candles
endpoint 장애 중에도 ticker endpoint가 정상이라면 관측 stop을 저장합니다.

## Health check

```bash
python3 -m coinpilot --config config.toml status --events 20
```

확인할 값:

- `halt_state`가 `ACTIVE`
- `last_bar_time`이 최근 마감 60분봉
- `updated_at`과 최근 ticker 시간이 정상적으로 증가
- `ticker_exchange_time`이 최신 봉 마감 이후인지
- `market_data_gap`, `model_not_ready`, `drawdown_halt`, `entry_rejected` event 유무
- cash, quantity, realized PnL이 예상 범위

## Halt

한 봉보다 긴 downtime, 데이터 gap, 최대 drawdown은 자동 재개하지 않습니다.
현재 MVP에는 위험한 자동 rearm 명령이 없습니다.

1. 프로세스를 중단합니다.
2. event와 state를 백업하고 원인을 확인합니다.
3. 설정 또는 전략을 바꾼다면 새 `paper.account_name`으로 새 검증 구간을
   시작합니다.
4. 기존 계좌 migration/rearm은 별도 코드 리뷰 과제로 처리합니다.

## Backup

프로세스를 잠시 중단한 뒤 SQLite online backup을 사용합니다.

```bash
sqlite3 var/coinpilot.db ".backup 'coinpilot-backup.db'"
```

단순 파일 복사 시 WAL 파일과 시점이 어긋날 수 있으므로 `.backup`을 권장합니다.
DB와 WAL/SHM sidecar는 생성 시 소유자만 읽고 쓸 수 있도록 `0600`으로
설정됩니다.

## Recovery

- crash 전 transaction: state와 event가 함께 rollback됩니다.
- commit 뒤 crash: revision과 `last_bar_time` 때문에 같은 결정을 다시 fill하지
  않습니다.
- concurrent revision 오류: 다른 runner가 먼저 commit한 것이므로 현재 process를
  종료하고 `status`로 최신 state를 확인합니다.
- downtime으로 여러 봉 누락: historical price replay 없이 `HALTED`가 정상입니다.
- gap-adjusted 유효 학습 이력 부족: 신규 계좌는 시작하지 않으며, 기존 계좌는
  신규 진입을 막고 보유 포지션을 정리합니다. 공개 데이터를 다시 동기화한 뒤
  원인을 검토하고 새 account로 재검증합니다.

## Promotion

Paper는 실제 주문이 아닙니다. 최소 30일 동안 중복 fill, revision 충돌, 원장
불일치, risk 위반이 0건이어야 live adapter 검토 후보가 됩니다. Live 구현은 이
runbook 범위 밖이며 현재 CLI에서 하드 차단됩니다.
