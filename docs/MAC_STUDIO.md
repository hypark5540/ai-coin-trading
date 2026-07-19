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
- `notifier`: DB outbox에서 Slack 전송 및 재시도
- `web`: localhost 전용 읽기 API와 dashboard
- `external-watchdog`: 60초마다 실행되는 별도 watchdog 프로세스
- `backup`: 매일 03:15 SQLite online backup과 integrity check
- `retention`: 매일 04:15 원본·백업·회전 로그의 제한적 보존 처리

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
git config user.name hypark5540
```

commit 작성까지 할 Mac이라면 GitHub 계정에 등록된 이메일도 이 저장소의
`user.email`로 설정한다. 이메일은 저장소가 추측해 넣지 않는다.

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
7. 현재 사용자 경로로 plist를 렌더링하고 `launchctl bootstrap`한다.

기존 설정, DB, 원본 데이터 및 백업은 다시 실행해도 덮어쓰거나 지우지 않는다.
Slack secret이 없으면 notifier만 설치하고 시작하지 않는다. watchdog는 secret을
읽지 않고 장애 알림을 DB outbox에 넣으므로 계속 실행된다.

설정 파일과 plist까지 검토한 뒤 시작하고 싶다면 `bootstrap --apply --no-start`를
사용하고, 검토 후 `start --apply`를 실행한다.

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
COINPILOT_ENABLE_NOTIFIER=1
COINPILOT_ENABLE_WEB=1
COINPILOT_ENABLE_WATCHDOG=1
COINPILOT_RETENTION_DAYS=90
COINPILOT_BACKUP_RETENTION_DAYS=35
COINPILOT_LOG_RETENTION_DAYS=30
COINPILOT_LOG_MAX_MB=100
```

실제 shadow DB·archive·backup 경로는 최초 생성 시 `config.toml`의
`[shadow].database_path`, `[shadow].archive_root`,
`[operations].backup_dir`에 절대 경로로 기록된다. 백업·retention helper도
이 설정을 직접 읽으므로 paper DB와 혼동하거나 경로를 이중 관리하지 않는다.

Slack URL은 두 파일 어디에도 넣지 않는다. notifier가 `config.toml`에 있는
Keychain service/account 식별자로 Keychain을 직접 조회한다. service wrapper나
launchd 환경변수로 webhook을 전달하지 않는다. dashboard host를
`127.0.0.1` 이외로 변경하면 service wrapper와 애플리케이션이 시작을 거부해야
한다. 원격 확인은 공개 포트를 여는 대신 SSH tunnel 또는 Tailscale Serve를
사용한다.

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
사용한 뒤 `PRAGMA integrity_check`를 통과한 파일만 원자적으로 확정하고 SHA-256
파일을 함께 만든다.

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
