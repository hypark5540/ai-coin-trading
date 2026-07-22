# CoinPilot agent instructions

이 파일은 저장소 전체에 적용된다. 운영 작업을 시작할 때
`docs/OPERATIONS_HANDOFF.md`를 읽되, 그 문서는 시점 스냅샷이므로 반드시 현재
프로세스·원장·API 상태를 다시 조회한다. 실시간 확인 결과가 문서보다 우선한다.

## Git과 작업 경계

- 이 Mac의 Git identity와 기본 GitHub SSH 키는 이미 올바르다.
  `git config user.*`를 설정하거나 변경하지 않는다.
- 별도 `includeIf`, SSH alias 또는 개인용 remote를 만들지 않는다. `gh` CLI가
  있다고 가정하지 않는다.
- 사용자의 기존 변경과 운영 데이터를 보존한다. DB, WAL/SHM, archive, backup,
  log를 삭제·초기화·덮어쓰지 않는다.
- PR 병합, 실제 거래 활성화처럼 현재 요청보다 권한이 커지는 작업은 추론해서
  실행하지 않는다.

## 절대 안전 조건

- 이 프로젝트는 공개 시세 기반 연구와 모의거래 전용이다. Upbit private API
  key, 실제 주문 client, 실제 주문 routing을 추가하거나 활성화하지 않는다.
- 모든 운영 확인에서 `simulated=true`, `live_order_routing=false`,
  `orders_sent=0`이어야 한다. 하나라도 다르면 신규 모의결정을 중단하고 원인을
  조사한다.
- D2는 수익성이 검증된 alpha가 아니라 체결·위험·회계 배관을 검증하는
  diagnostic 전략이다. 일손실 또는 peak drawdown 10% 경계는 fail-closed로
  유지한다. 경계를 넘긴 원장은 자동 rearm하지 않는다.
- 코드·설정 fingerprint가 달라졌거나 halt된 원장은 그대로 재사용하지 않는다.
  기존 원장과 검증 백업을 보존하고, 검토된 새 versioned
  `shadow.database_path`로 시작한다.
- C2 halt, 데이터 gap, 장기 downtime은 자동 복구하지 않는다. 전략·코드 변경은
  기존 계좌를 덮어쓰지 말고 새 account와 새 T0 검증 구간을 사용한다.

## 활성 토폴로지

- D2 bounded diagnostic shadow: `d2-btc`, `d2-eth`, `d2-xrp`, `d2-sol`.
  Dashboard port는 각각 `8774`, `8775`, `8776`, `8777`이다.
- C2 60분봉 forward-paper: `c2-btc`, `c2-eth`, `c2-xrp`, `c2-sol`.
  C2 dashboard와 shadow 서비스는 비활성이며 paper 서비스만 운용한다.
- 통합 일일 성적표는 중앙 LaunchAgent `dev.coinpilot.daily-scorecard` 하나만
  사용한다. 이 job은 승인된 D2/C2 8개 원장을 read-only로 조회하고 별도
  `~/Library/Application Support/Coinpilot/reporting` 아래에만 전송 상태와
  감사 산출물을 쓴다. 불완전 source 결과는 최종 성적표로 확정하지 않고 재시도하며,
  여러 날 중단 뒤에도 latest-first로 하루씩 backlog를 복구한다.
- 과거 `btc`, `eth`, `xrp`, `sol` 인스턴스와 port
  `8766`, `8767`, `8772`, `8773`은 폐기된 1% diagnostic 화면이다. 재가동하지
  않는다.
- 이 Mac Studio의 마지막 관측에서 port `8765`는 다른 로컬 프로젝트가
  사용했다. 어떤 점유 port도 현재 listener 소유권을 먼저 검증하지 않고
  프로세스를 중단하거나 설정을 변경하지 않는다.
- D2/C2 per-instance hourly Slack notifier는 계속 비활성이다. 통합 일일 성적표
  승인은 이 notifier들을 다시 켤 권한이 아니며, 중앙 daily-scorecard 외의
  notifier는 사용자의 별도 명시적 요청 없이 켜지 않는다.
- `runtime.env`의 비활성 값만으로 서비스가 멈췄다고 판단하지 않는다. 로그인·
  재부팅 뒤에는 해당 LaunchAgent가 실제 `not-loaded`이고 launchd override도
  `disabled`인지 확인한다. 설정상 비활성인 job이 loaded/running이면 안전
  드리프트로 취급해 원장과 로그를 보존한 채 중단하고 원인을 조사한다.

## 상태 해석

- `receive_interval`은 event-driven 공개피드의 관측 침묵 표시이지 단독으로
  packet loss나 연결 단절을 증명하지 않는다. 이 사유만 warmup을 초기화하지
  않는다.
- reconnect/error, capture 또는 connection 변경, ordinal 불연속,
  monotonic regression, normalization 오류는 계속 확정된 continuity 경계로
  처리한다.
- D2의 fresh `warmup`은 살아 있는 상태다. `stopped`, `halted_recovery`,
  `feed_stale`과 구분한다. `/health/ready`는 running+fresh일 때만 200이어야 한다.
- C2는 60분봉 전략이므로 체결과 손익이 오랫동안 변하지 않아도 정상일 수 있다.
  LaunchAgent PID, `revision`, `updated_at`, `halt_state`를 함께 확인한다.
- 일일 성적표의 자가개선 항목은 관측·분석·오프라인 후보 제안까지만 허용한다.
  활성 전략·설정·halt·fingerprint·원장을 자동 변경하거나 rearm하지 않는다.

## 운영과 검증

- 변경 전에 `status`, `doctor`, 로그, 원장 포지션과 pending 주문을 확인한다.
  손익, PID, run ID, readiness는 문서의 과거 값을 그대로 인용하지 않는다.
- 변경 명령은 기본 dry-run을 우선하고, 적용 시에만 `--apply`를 사용한다.
  `scripts/mac-studio` 문법은 command가 먼저다. 예:
  `./scripts/mac-studio status --instance d2-btc`.
- 운영 코드 변경은 관련 테스트, 전체 `pytest`,
  `./scripts/mac-studio test`, `git diff --check`를 통과시킨다. 사용자가
  commit/push를 요청한 경우 push 뒤 Ubuntu와 macOS GitHub Actions를 확인한
  다음 배포한다.
- 중앙 reporter만 변경·배포하는 경우 D2/C2 instance에 `install` 또는 `update`를
  실행하지 않는다. `./scripts/mac-studio-daily-scorecard`만 사용하며 기존
  runtime, 설치 package, fingerprint, 원장과 outbox가 바뀌지 않았는지 확인한다.
- D2 shadow 동작·source·config·fingerprint에 영향을 주는 변경은 먼저 현재
  position이 flat이고 pending 주문이 0인지 확인한다. non-flat이면 원장을
  고아로 만들거나 강제 청산하지 말고 중단·보고한다. flat일 때만 online backup,
  clean stop, 새 versioned ledger, `install --no-start --apply`, 순차 start,
  doctor, readiness 연속 관찰 순서를 지킨다. 문서만 바뀐 경우 새 원장을 만들지
  않는다.

## 기준 문서

- Mac Studio 설치·운영: `docs/MAC_STUDIO.md`
- 현재 시점 인수인계: `docs/OPERATIONS_HANDOFF.md`
- paper halt·복구: `docs/RUNBOOK.md`
- HFT shadow 경계: `docs/HFT_RUNBOOK.md`
- 보안 불변조건: `SECURITY.md`
