# Mac Studio Shadow 운영

이 문서는 전용 Mac Studio에서 Coinpilot을 실제 주문 없이 상시 실행하는 절차다.
설치 도구는 사용자 범위의 `launchd` LaunchAgent를 사용하고, 변경 명령은 기본
dry-run이며 `--apply`가 있을 때만 반영한다. `sudo`, 실제 주문 API, private
Upbit 키는 사용하지 않는다.

여기서 상시는 **로그인·잠금 해제된 전용 운영 사용자 세션 안에서의 상시 운전**을
뜻한다. 정전 후 로그인 전부터 자동 실행되는 시스템 서비스는 이 패키지가
제공하거나 보장하지 않는다.

## 구성

기본 경로는 사용자와 저장소 위치를 하드코딩하지 않는다.

| 항목 | 기본 위치 |
| --- | --- |
| 저장소 | 사용자가 clone한 임의의 경로 |
| 설정·데이터·venv | `~/Library/Application Support/Coinpilot` |
| 로그 | `~/Library/Logs/Coinpilot` |
| LaunchAgent | `~/Library/LaunchAgents/dev.coinpilot.*.plist` |
| Slack webhook | macOS Keychain의 `coinpilot-slack-webhook` |
| Dashboard | `http://127.0.0.1:8765` |

프로세스는 다음과 같이 분리된다.

- `shadow`: 공개 WebSocket, 실시간 feature, 모의 주문·체결, shadow 원장
- `paper`: 기본 비활성인 60분봉 C2 forward-paper 계좌
- `notifier`: 직전 완료 KST 1시간을 집계해 Slack 요약 전송 및 재시도
- `web`: localhost 전용 읽기 API와 dashboard
- `external-watchdog`: 60초마다 실행되는 별도 watchdog 프로세스
- `backup`: 매일 03:15 SQLite online backup과 integrity check
- `retention`: 매일 04:15 원본·백업·회전 로그의 제한적 보존 처리

`paper`는 기존 `shadow`와 설정 및 SQLite 원장을 공유하지 않는다. 설치는 되지만
`COINPILOT_ENABLE_PAPER=0`이 기본값이므로 설정을 생성하고 검토하기 전에는
LaunchAgent가 로드되지 않는다.

`external-watchdog`는 주 엔진과 분리되어 프로세스 장애를 알릴 수 있지만 같은
Mac에서 실행된다. Mac의 전원이나 인터넷이 완전히 끊기면 Slack을 보낼 수
없다. 진정한 외부 dead-man 감시는 별도 서버나 SaaS가 이 Mac의 heartbeat
부재를 확인하도록 추가해야 한다. 공급자를 정하기 전까지 이 부분은 의도적으로
placeholder다.

## 최초 설치

Mac Studio의 GitHub SSH 공개키를 `hypark5540` 계정에 등록한 뒤 clone한다.

```bash
git clone git@github.com:hypark5540/ai-coin-trading.git
cd ai-coin-trading
```

설치 도구는 이미 구성된 Git identity와 SSH 설정을 읽거나 변경하지 않는다.

먼저 변경 내용을 확인한다.

```bash
./scripts/mac-studio bootstrap
```

Slack Incoming Webhook을 Keychain에 넣는다. URL은 파일, shell history,
environment, process argv에 들어가지 않으며 입력은 macOS Keychain의 보안
prompt가 직접 받는다.

```bash
./scripts/mac-studio slack set --apply
```

설치를 한 번에 적용하고 확인한다.

```bash
./scripts/mac-studio bootstrap --apply
./scripts/mac-studio doctor
./scripts/mac-studio status
```

`bootstrap`은 다음 작업을 idempotent하게 수행한다.

1. macOS와 Python 3.11 이상을 확인한다.
2. 기존 Homebrew가 있고 Python이 없을 때만 `python@3.12` 설치를 수행한다.
   Homebrew 자체는 자동 설치하지 않는다.
3. owner-only 디렉터리와 격리된 venv를 만든다.
4. 저장소 공통 `requirements-build.lock`과 `requirements.lock`의 정확한
   의존성을 순서대로 설치한다.
5. secret 없는 `config.toml`과 `runtime.env`를 최초 한 번 생성한다.
6. retention 대상 디렉터리에 owner-only managed marker를 만든다.
7. 현재 사용자 경로로 7개 plist를 렌더링하고 활성화된 작업만
   `launchctl bootstrap`한다.

기존 설정, DB, 원본 데이터 및 백업은 다시 실행해도 덮어쓰거나 지우지 않는다.
Slack secret이 없으면 notifier만 설치하고 시작하지 않는다. watchdog는 secret을
읽지 않고 장애 알림을 DB outbox에 넣으므로 계속 실행된다.

설정 파일과 plist까지 검토한 뒤 시작하고 싶다면 `bootstrap --apply --no-start`를
사용하고, 검토 후 `start --apply`를 실행한다.

## BTC + ETH 격리 인스턴스

한 shadow 엔진은 의도적으로 한 시장만 처리한다. 여러 시장을 한 프로세스에
섞는 대신 named instance가 단일시장 스택 전체를 복제한다. 다음 구성은 총
모의자금 경계를 BTC와 ETH에 각각 5백만원씩 분리하고, 수익 전략을 켜지 않은
`observe` 모드로 공개피드와 운영 상태만 수집한다. 주문금액 12만5천원은 이후
명시적인 diagnostic 배관 검사를 할 때의 상한이며 observe에서는 사용되지 않는다.

먼저 두 인스턴스를 서비스 시작 없이 생성한다.

```bash
./scripts/mac-studio bootstrap \
  --instance btc \
  --market KRW-BTC \
  --initial-cash 5000000 \
  --order-quote 125000 \
  --shadow-mode observe \
  --web-port 8766 \
  --no-start --apply

./scripts/mac-studio bootstrap \
  --instance eth \
  --market KRW-ETH \
  --initial-cash 5000000 \
  --order-quote 125000 \
  --shadow-mode observe \
  --web-port 8767 \
  --no-start --apply
```

설정과 plist를 검토한 뒤 각각 시작하고 확인한다.

```bash
./scripts/mac-studio start all --instance btc --apply
./scripts/mac-studio doctor --instance btc

./scripts/mac-studio start all --instance eth --apply
./scripts/mac-studio doctor --instance eth
```

Dashboard는 BTC `http://127.0.0.1:8766`, ETH
`http://127.0.0.1:8767`이다. 각 인스턴스는 다음 항목을 공유하지 않는다.

- config, shadow DB와 DB writer lock
- raw archive와 full-sync audit WAL/recorder lock
- backup 디렉터리와 backup lock
- 로그와 retention 경계
- venv의 non-editable 애플리케이션 복사본
- LaunchAgent label과 plist
- dashboard port

Slack webhook 목적지만 기존 Keychain 항목을 함께 사용한다. notifier는 매시
BTC와 ETH를 각각 한 개의 `KRW-BTC`/`KRW-ETH` Block Kit 요약으로 보내므로
출처가 구분된다. 두 계좌의 cash, drawdown, daily-loss는 독립적으로 계산되며
공유 현금 포트폴리오가 아니다.

Strategy Research V2 검수 뒤의 운영 champion은 `cash/observe-only`다.
`diagnostic`은 알파 전략으로 재가동하지 않으며, 과거 diagnostic 원장을 새
observe 원장으로 전환할 때는 먼저 SQLite online backup과 integrity check를
수행하고 기존 DB를 timestamped legacy 디렉터리에 보존한다. fingerprint가 다른
DB를 같은 경로에서 그대로 이어 쓰면 의도적으로 recovery HALT가 걸린다.

named instance의 일상 명령에는 항상 같은 `--instance`를 붙인다.

```bash
./scripts/mac-studio status --instance btc
./scripts/mac-studio logs shadow --instance eth
./scripts/mac-studio backup --instance btc --apply
./scripts/mac-studio restart shadow --instance eth --apply
```

새 named `diagnostic` 인스턴스는 실수로 즉시 시작되지 않도록 최초 설치에
`--no-start`가 필수다. 기존 config를 다시 설치할 때 전달한 시장·자금·주문금액·
mode·port가 다르면 설치기는 보존된 설정을 덮어쓰지 않고 실패한다. named
인스턴스의 DB, archive, backup, log 경로가 자기 app home을 벗어나거나 다른
instance와 dashboard port가 겹쳐도 시작을 거부한다.

### D2 10% bounded 진단

기존 `diagnostic-imbalance-flow-v0`의 1% 손실 원장은 재개하지 않는다. 손실
구조를 추가 관찰할 때만 새 원장인 D2를 사용하며, 이 정책은 여전히 alpha가
아니다.

| 인스턴스 | 시장 | Dashboard | 모의자금 | 주문금액 |
| --- | --- | --- | ---: | ---: |
| `d2-btc` | KRW-BTC | `127.0.0.1:8774` | ₩5,000,000 | ₩25,000 |
| `d2-eth` | KRW-ETH | `127.0.0.1:8775` | ₩5,000,000 | ₩25,000 |
| `d2-xrp` | KRW-XRP | `127.0.0.1:8776` | ₩5,000,000 | ₩25,000 |
| `d2-sol` | KRW-SOL | `127.0.0.1:8777` | ₩5,000,000 | ₩25,000 |

D2 운영 profile은 다음 값을 fail-closed로 강제한다.

```text
shadow.model_version=diagnostic-bounded-v1
shadow.max_daily_loss_pct=0.10
shadow.max_drawdown_pct=0.10
COINPILOT_BOUNDED_SHADOW=1
COINPILOT_SHADOW_COOLDOWN_SECONDS=3600
COINPILOT_SHADOW_MAX_ROUND_TRIPS_PER_DAY=24
COINPILOT_SHADOW_RESERVE_FULL_ORDER_LOSS=1
COINPILOT_SHADOW_EXECUTION_SPREAD_RECHECK=1
COINPILOT_ENABLE_NOTIFIER=0
```

일손실 또는 peak drawdown이 `>= 10%`가 되면 열린 포지션의 risk exit를 먼저
시도하고 flat이 된 뒤 `external_halt:*`를 영구 기록한다. 신규 진입의 spread
검사는 risk exit를 막지 않는다. 시장 gap에서는 청산 체결가 때문에 최종 손실이
10%를 넘을 수 있으므로 10%를 보장된 stop 체결가로 해석하면 안 된다. 신규 진입
전에는 주문원금과 진입 수수료 전체를 잔여 손실예산으로 예약해, 알고 있는
위험만으로 경계를 넘는 주문을 거부한다.

halt 시 `state/<run-id>.halt-diagnostic.json`에 초기자금 손익, KST 당일 손실,
peak drawdown, restart lineage 누적 수수료·회전율과 개선 gate를 mode `0600`으로
기록한다. 전원 차단이 halt commit과 파일 기록 사이에 발생해도 다음 시작에서
원장을 읽어 누락 파일을 재생성한다. 자동으로 코드를 바꾸거나 같은 원장을
재개하지는 않는다. 동결 원장 검산, 모든 비용을 포함한 causal replay, 시간분할과
2배 비용 stress를 통과한 새 모델 버전만 새 원장에서 시작할 수 있다.

점검 명령은 다음과 같다.

```bash
for instance in d2-btc d2-eth d2-xrp d2-sol; do
  ./scripts/mac-studio doctor --instance "$instance"
  ./scripts/mac-studio status --instance "$instance"
done
```

`doctor`는 helper 존재, 10% 설정과 bounded profile까지 검사한다. bounded flag가
없거나 오타이거나, 필수 reserve/recheck가 꺼졌거나, cooldown/cap이 완화되면
일반 shadow로 내려가지 않고 시작을 거부한다. helper와 service wrapper 및 네
bounded runtime 값은 run deployment fingerprint에 포함된다.

## 필수 macOS 설정

이 설치 도구는 보안상 전원 설정을 root 권한으로 바꾸지 않는다. System Settings의
Energy에서 다음을 직접 확인한다.

- 디스플레이가 꺼져도 자동 sleep하지 않음
- 정전 후 자동 재시작
- 가능하면 UPS 연결
- 유선 Ethernet 사용
- 자동 OS 업데이트로 무인 재부팅되지 않도록 유지보수 시간 지정

LaunchAgent와 로그인 Keychain을 쓰므로 전용 운영 사용자가 로그인된 상태여야
한다. FileVault가 켜진 Mac은 정전 후 부팅 과정에서 사용자 잠금 해제가 필요할
수 있다. 이 제한을 피하려고 자동 로그인을 켜는 것은 권장하지 않는다. 무인
재부팅이 필수라면 공개피드 recorder를 별도 최소권한 LaunchDaemon으로 두고,
사용자 Keychain을 쓰는 notifier·dashboard와 IPC로 분리하는 추가 보안 설계를
먼저 리뷰해야 한다. 그 설계와 root 설치는 현재 원클릭 범위에 포함되지 않는다.

## 설정

전략 설정은 아래 파일에 있고 mode `0600`으로 생성된다.

```text
~/Library/Application Support/Coinpilot/config/config.toml
```

운영 설정은 같은 디렉터리의 `runtime.env`다. 허용되는 주요 값은 다음과 같다.

```text
COINPILOT_WEB_HOST=127.0.0.1
COINPILOT_WEB_PORT=8765
COINPILOT_ENABLE_SHADOW=1
COINPILOT_ENABLE_PAPER=0
COINPILOT_ENABLE_NOTIFIER=1
COINPILOT_ENABLE_WEB=1
COINPILOT_ENABLE_WATCHDOG=1
COINPILOT_SLACK_SUMMARY_SECONDS=3600
COINPILOT_SLACK_SUMMARY_GRACE_SECONDS=15
COINPILOT_RETENTION_DAYS=90
COINPILOT_BACKUP_RETENTION_DAYS=35
COINPILOT_LOG_RETENTION_DAYS=30
COINPILOT_LOG_MAX_MB=100
```

실제 shadow DB·archive·backup 경로는 최초 생성 시 `config.toml`의
`[shadow].database_path`, `[shadow].archive_root`,
`[operations].backup_dir`에 절대 경로로 기록된다. 활성 C2 paper 원장은 별도
`config/paper.toml`과 `data/coinpilot-c2.db`만 사용한다. backup helper는 shadow와
활성 paper DB를 서로 다른 파일명으로 online backup하며 retention은 같은 검증된
backup 및 `paper.stdout.log`/`paper.stderr.log` 경계를 처리한다.

Slack URL은 어떤 설정 파일에도 넣지 않는다. notifier가 `config.toml`에 있는
Keychain service/account 식별자로 Keychain을 직접 조회한다. service wrapper나
launchd 환경변수로 webhook을 전달하지 않는다. dashboard host를
`127.0.0.1` 이외로 변경하면 service wrapper와 애플리케이션이 시작을 거부해야
한다. 원격 확인은 공개 포트를 여는 대신 SSH tunnel 또는 Tailscale Serve를
사용한다.

### C2 forward-paper 서비스

C2는 `KRW-BTC`, `KRW-ETH`, `KRW-XRP`, `KRW-SOL`을 독립된 250만원 sleeve로
운영하는 60분봉 336/168 돌파 전략이다. 각 인스턴스는 다음 고정 경계를 쓴다.

```text
config/paper.toml
data/coinpilot-c2.db
COINPILOT_ENABLE_PAPER=1
```

설치기는 C2의 전체 모델·위험·paper 실행 정책, 동일 instance 시장, 공개 Upbit API,
고정 DB 경계와 `paper.manifest.json`의 설정·설치 package SHA-256을 시작 전에
검증한다. 같은 검증은 LaunchAgent의 최초 실행, KeepAlive 재시작, 로그인 후 재실행
때마다 repo 없이 설치된 package 바이트를 대상으로 반복된다. account 또는 DB 경로,
설정 바이트나 설치된 소스가 manifest와 다르면 wrapper가 엔진 실행 전에 실패한다.
활성 paper 계좌의 install/update도 기존 manifest와 소스가 동일하지 않으면
거부하므로 변경된 코드는 새 계좌와 새 T0가 필요하다.

상태 점검은 launchd job 등록만 보지 않고 실제 `running` state와 살아 있는 PID를
확인한 뒤 원장을 읽기 전용으로 열어 schema v8, config fingerprint, ACTIVE 상태,
revision과 최근 갱신시각을 확인한다. 어느 검증이든 실패하면 ready로 표시하지
않는다. wrapper는 항상
`COINPILOT_LIVE_TRADING=0`과 `COINPILOT_MODE=paper`를 강제하며 private API 키나
실주문 adapter를 사용하지 않는다.

사용자가 요청한 즉시 activation에서는 각 새 원장의 첫 성공 초기화 시각을 T0로
기록한다. 미리 prime한 DB를 중지했다가 공식 원장으로 재사용하면 안 된다. 네 시장의
최초 2,501개 캔들 동기화는 Upbit 호출 집중을 피하도록 인스턴스를 순차 시작한다.

기존 `notifier`와 `web`은 shadow DB만 읽는다. 따라서 C2 체결은 현재 Slack 시간별
shadow 요약과 dashboard에 섞이지 않으며 다음 명령으로 확인한다.

```bash
./scripts/mac-studio status --instance c2-btc
./scripts/mac-studio doctor --instance c2-btc
./scripts/mac-studio logs paper --instance c2-btc
```

paper 전용 named instance에서는 `COINPILOT_ENABLE_SHADOW`,
`COINPILOT_ENABLE_NOTIFIER`, `COINPILOT_ENABLE_WEB`,
`COINPILOT_ENABLE_WATCHDOG`를 모두 `0`으로 두고 paper·backup·retention만 실행할
수 있다. 이때 Doctor는 비활성 Slack과 dashboard를 장애로 보고하지 않는다.

### Slack 알림 정책

운영 LaunchAgent는 개별 체결·시작·종료·재시작·continuity 이벤트를 Slack으로
즉시 보내지 않는다. 원본 fill/run/health와 outbox 행은 SQLite에 남기고, notifier가
KST 기준 직전 완료 고정 구간 `[HH:00, 다음 HH:00)`을 하나의 요약으로 압축한다.
요약에는 다음 항목이 들어간다.

- 구간 평가자산 변동과 수익률, 마감 평가자산·포지션
- 완료 거래 손익, 승/패/보합, 평균 보유시간
- 매수·매도 체결 수, 거래대금, 수수료
- runtime error, 재시작, halt, continuity, warning/critical 횟수
- `simulated=true`, `live_order_routing=false`, `orders_sent=0`

거래가 없는 시간도 상태 heartbeat 한 건을 보낸다. 시장·시간 경계의
deterministic key로 정상 재시작과 동시 실행 때 같은 요약이 중복 생성되는 것을
막는다. 단, Slack webhook POST 성공 직후 `delivered` 기록 전에 프로세스가
종료되는 드문 경우에는 같은 구간이 한 번 더 전송될 수 있는 at-least-once
경계가 있다. 오랜 중단 뒤에는 가장 최근 완료 구간만 만들며 과거 시간별
메시지를 몰아서 보내지 않는다. Slack 실패는 lease와 지수 backoff로 재시도한다.

이 정책은 즉시 critical Slack도 시간 요약으로 지연한다. 실제 주문 경로가 없는
shadow 전용 운용을 전제로 한 저소음 설정이다. 외부 dead-man 감시가 필요하면
별도 서버/SaaS에서 구성해야 한다.

설정을 변경했다면 실행 중 프로세스에 임의 반영하지 말고 다음 순서를 사용한다.

```bash
./scripts/mac-studio stop --apply
# 설정 검토 및 수정
./scripts/mac-studio start --apply
./scripts/mac-studio doctor
```

전략, 모델, 위험 설정을 변경하면 기존 shadow 결과와 같은 검증 구간으로
이어붙이지 말고 새 run/account로 시작한다. 구체적으로 서비스를 멈춘 뒤
`shadow.database_path`를 이전 파일과 다른 새 경로로 바꾼다. 기존 DB는
감사·비교용으로 보존한다.

## 일상 운영

```bash
./scripts/mac-studio status
./scripts/mac-studio doctor
./scripts/mac-studio logs shadow
./scripts/mac-studio logs paper
./scripts/mac-studio logs notifier --follow
./scripts/mac-studio restart shadow --apply
```

수동 백업과 retention은 먼저 dry-run 결과를 보고 적용할 수 있다.

```bash
./scripts/mac-studio backup
./scripts/mac-studio backup --apply
./scripts/mac-studio retention
./scripts/mac-studio retention --apply
```

백업은 SQLite 파일을 단순 복사하지 않는다. Python의 SQLite online backup을
사용한 뒤 `PRAGMA integrity_check`를 통과한 shadow 및 활성 paper 파일만 서로
다른 이름으로 원자적으로 확정하고 SHA-256 파일을 함께 만든다.

retention은 지정한 전용 디렉터리 안의 일반 파일만 대상으로 한다. 기본값은
raw 90일, 검증된 backup 35일, 회전 log 30일이다. 활성 로그가 기본 100MB를
넘으면 사본을 만든 뒤 원본을 비우고 사본을 gzip한다. 수동 실행은 기본
dry-run이고, 예약된 LaunchAgent만 명시적인 `--apply`로 실행한다.
각 대상은 canonical 경로가 Coinpilot 관리 경계 안에 있고 설치기가 만든
owner-only marker가 있어야 한다. 경로 오설정, symlink, marker 부재 시에는
삭제를 한 건도 하지 않고 실패한다.
미복구 WAL·recorder lock·격리 증거가 있는 `.audit-journal`과
`.audit-quarantine`은 자동 retention에서 항상 제외하며 수동 감사 후에만
다룬다.

## 안전한 업데이트와 제거

업데이트는 dirty working tree를 거부하고 fast-forward만 허용한다.

```bash
./scripts/mac-studio update
./scripts/mac-studio update --apply
./scripts/mac-studio doctor
```

코드 또는 lock dependency가 달라진 업데이트는 기존 원장의 fingerprint와
의도적으로 불일치한다. 이때 shadow는 자동으로 신규 모의결정을 재개하지 않고
`restart_fingerprint_changed` HALT를 유지한다. 로그와 변경 내용을 검토한 뒤
위 설정 절차로 새 `shadow.database_path`를 선택해야 한다.

제거도 먼저 preview할 수 있다.

```bash
./scripts/mac-studio uninstall
./scripts/mac-studio uninstall --apply
```

`uninstall`은 알려진 LaunchAgent와 설치된 helper만 제거한다. 아래 항목은
항상 보존한다.

- 설정과 Keychain item
- SQLite DB와 raw 데이터
- 백업
- 로그
- venv
- Git 저장소

데이터 삭제 명령은 의도적으로 제공하지 않는다.

## 장애 확인

가장 먼저 아래를 실행한다.

```bash
./scripts/mac-studio doctor
./scripts/mac-studio status
./scripts/mac-studio logs shadow
./scripts/mac-studio logs external-watchdog
```

일반적인 원인은 다음과 같다.

- `notifier not-loaded`: Slack Keychain item을 만든 뒤 `restart --apply`
- dashboard unavailable: web stderr와 `127.0.0.1:8765` 점유 확인
- launch domain unavailable: 운영 사용자로 GUI 로그인 후 다시 실행
- shadow 반복 재시작: stdout/stderr에서 config/model fingerprint 및 DB 오류 확인
- paper unavailable: paper LaunchAgent, C2 fingerprint, ACTIVE 상태와 최근 갱신 확인
- 정전 후 미시작: FileVault unlock과 사용자 로그인 상태 확인

재시작 중 놓친 시장 데이터를 과거 호가로 소급 체결하면 안 된다. gap이 발생한
run은 HALT 상태와 사유를 보존하고 검토 후 새 run으로 시작한다.

## 왜 Terraform이 아닌가

Terraform은 클라우드 API처럼 선언적 원격 자원을 관리하는 데 적합하다. 한 대의
Mac에서 사용자 Keychain prompt, venv, owner-only 파일, 로그인 launchd domain과
프로세스 재시작을 `local-exec`로 감싸면 다음 문제가 생긴다.

- 실제 로컬 상태를 Terraform provider가 이해하지 못한다.
- secret이 state나 plan에 들어갈 위험이 있다.
- 실패한 local command의 재실행과 rollback 의미가 불명확하다.
- 사용자·로그인·FileVault 상태 때문에 다른 Mac에서 같은 plan이 같게 동작하지
  않는다.

따라서 현재는 검토 가능한 plist template과 idempotent shell entrypoint가 더
작고 안전하다. Mac을 여러 대 운영하거나 OS package와 사용자 정책까지 중앙
관리해야 할 때는 이 경계를 유지한 채 Ansible role이나 MDM profile을 추가하는
것이 적절하다. Terraform은 이후 외부 heartbeat, DNS, cloud backup bucket 같은
실제 원격 인프라에만 사용하는 편이 좋다.

## 정적 검증

```bash
./scripts/mac-studio test
```

검증 항목은 shell syntax, 렌더링된 plist 파싱, bootstrap/backup/retention의
비변경 dry-run, tracked 파일 내 Slack webhook 형태 값 부재, 암묵적 `sudo`
호출 부재다.
