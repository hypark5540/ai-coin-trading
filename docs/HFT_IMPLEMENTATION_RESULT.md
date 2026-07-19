# HFT Foundation Implementation Result

기준 시각은 2026-07-19 KST입니다. 이 문서는 공개 업비트 데이터만 사용한
HFT 연구 기반의 구현·검증 결과입니다. API key, 계좌 데이터, private WebSocket,
실제 주문·취소는 사용하지 않았고 모든 replay의 `orders_sent`는 0입니다.
아래 원본 capture와 생성 산출물은 로컬 `artifacts/` 아래에 보존하며,
용량·재배포·운영 데이터 혼입을 막기 위해 Git에는 포함하지 않습니다.

## 결론

연속 공개피드 수집, 인과적 microstructure dataset, 다호가 taker 비용 replay까지
필수 기반은 구현됐습니다. 다만 10초 smoke는 구현 검증일 뿐 전략 수익성이나
연 20% 가능성을 검증하지 않습니다. 다음 단계는 최소 30일 연속 자료와
시간분리 OOS·shadow 운용입니다.

## 구현 범위

| 계층 | 구현 |
|---|---|
| 수집 | public `orderbook`/`trade`, reconnect, text `PING`, idle 재연결 |
| 저장 | UTC gzip partition, capture/connection ID, 전역 ordinal, wall/monotonic ns |
| 무결성 | SHA-256 manifest, manifest-first/data-last commit, partial·tainted 폐기 |
| 실행 ID | 녹화 전 durable reservation, run/data/manifest full-run completion |
| 입력 | hash/byte/record/capture/ordinal 검증, uncommitted gzip fail-closed |
| 피처 | L1/L5 imbalance, microprice, spread, trailing signed public-trade flow |
| 라벨 | receive-time causal horizon, gap/connection/ordinal 경계, 250ms overshoot 제한 |
| 메모리 | 100만 input record 기본 상한, segment당 exact trade-ID 25만 상한 |
| 비용 replay | visible multi-level sweep, partial fill, VWAP/slippage/fee, monotonic latency |
| provenance | public observation과 counterfactual execution을 행 단위로 명시 |
| 산출물 | 모든 다중 파일 bundle을 staging 후 hash `.complete.json`으로 commit |
| 안전선 | private/auth/order 코드 없음, `live` 명령 hard-disabled |

전체 자동 테스트 236개를 통과했습니다.

## 최신 공개피드 smoke

원본은 로컬 `artifacts/hft/foundation-smoke-v3`에 있습니다.

| 항목 | 결과 |
|---|---:|
| 요청 실행시간 | 10.000초 |
| recorder 경과 | 10.049초 |
| 첫 이벤트~마지막 이벤트 span | 9.367초 |
| 전체 record | 65 |
| orderbook / public trade | 63 / 2 |
| ordinal | 1~65 연속 |
| 연결 / 재연결 | 1 / 0 |
| recorder gap / reject / protocol error | 0 / 0 / 0 |
| 압축 byte | 7,849 |
| data SHA-256 | `be0b9b5c304561b9d73accbcd5879c2d0a921e39bf4cf2b3e4cc213a67069ced` |
| combined input SHA-256 | `34c54943b3bdd3e09b40b87754406dffa02e3ce922dac0a40c6e7f5ad5ccf717` |

manifest 검증은 통과했고 `.partial`은 0개입니다. recorder gap threshold는
2초지만 replay의 보수적 orderbook gap 한도는 1초이므로, 아래 100ms replay는
한 구간을 별도로 invalid 처리했습니다. 요청시간·recorder 경과와 카운터는 로컬
`artifacts/hft/foundation-smoke-v3/foundation-v3-20260719.run.json`에
보존했습니다.
공개 orderbook 63건에는 연속 sequence ID가 없어 ordinal 연속성은 로컬
기록 연속성만 증명하며 거래소 패킷의 완전성을 증명하지는 않습니다.

최신 capture-ID reservation 경로는 별도 2초 실행 13건으로 확인했습니다. 로컬
`artifacts/hft/reservation-smoke-v1/reservation-smoke-20260719.complete.json`은
reservation·run summary·data·manifest 네 member의 저장 byte hash를 모두
검증합니다.

## 인과적 피처·라벨

완료 marker는 로컬
`artifacts/hft/foundation-smoke-v3-features.complete.json`입니다.

| horizon | valid | invalid: overshoot | invalid: tail | 전체 |
|---|---:|---:|---:|---:|
| 100ms | 58 | 4 | 1 | 63 |
| 1,000ms | 44 | 14 | 5 | 63 |
| 5,000ms | 21 | 13 | 29 | 63 |

첫 eligible orderbook이 목표 horizon보다 250ms 넘게 늦으면
`label_overshoot_exceeded`로 무효화했습니다. 따라서 장시간 뒤의 호가를 짧은
horizon 정답처럼 붙이지 않습니다. 각 feature 행은 원본 `source_ordinal`과
`source_kind=captured_public_market_data`를 보존합니다.

이 smoke에서는 best ask 1개, best bid 3개, mid 3개가 관측됐습니다. 유효 라벨
중 100ms 4건, 1초 14건, 5초 13건의 return이 0이 아니었지만, 가격 변화 폭은
약 ±1.47bp 이내이고 표본은 10초뿐입니다. 피처 계산·인과성 검증용일 뿐
예측 신호나 알파를 검증한 결과가 아닙니다.

## 25만원 독립 taker replay

| 항목 | 0ms 결정 snapshot | 100ms latency |
|---|---:|---:|
| 의사결정 | 63 | 63 |
| 실행 선택 | 63 (100.00%) | 61 (96.83%) |
| 전체 주문 기준 전량체결 | 63 (100.00%) | 61 (96.83%) |
| invalid / unavailable | 0 / 0 | 1 / 1 |
| L1 전량 / 다호가 | 63 / 0 | 61 / 0 |
| decision→selected book p50 | 0.000ms | 188.433ms |
| decision→selected book p95 | 0.000ms | 301.990ms |
| 기본 수수료 합계 | 7,875원 | 7,625원 |
| 2배 수수료 합계 | 15,750원 | 15,250원 |

0ms 결과: 로컬 `artifacts/hft/foundation-smoke-v3-depth-0ms.json`

100ms 결과: 로컬 `artifacts/hft/foundation-smoke-v3-depth-100ms.json`

2배 수수료 replay는 각 주문의 선택 book ordinal, 체결 수량, VWAP, 상태와 사유가
기본 replay와 동일한지 주문별로 검사했습니다. 바뀐 값은 fee와 fee를 반영한
cash flow뿐입니다.

모든 execution 행에는 `simulated=true`, `counterfactual=true`,
`own_execution=false`, `source_kind=public_orderbook_counterfactual`,
`orders_sent=0`이 들어갑니다.

## 해석 제한

- 공개 주문장은 가격대별 집계 표시 잔량이며 개별 queue position이 없습니다.
- 각 주문은 서로 독립이므로 앞 주문이 뒤 snapshot의 잔량을 감소시키지 않습니다.
- 숨은 유동성, 재보충, 내 주문의 시장충격과 실제 네트워크·거래소 주문 지연은
  모델링하지 않았습니다.
- `decision_to_selected_book`은 설정 latency 이후 처음 관측한 공개 book까지의
  시간이며 실제 계좌 fill latency가 아닙니다.
- 10초 동안 모든 25만원 매수가 L1에서 소화된 것은 그 짧은 표본의 상태입니다.
  이 표본의 최소 L1 ask 표시금액도 약 2,589.7만원으로 주문보다 컸습니다.
  다른 60초 legacy 표본에서는 35.8%가 여러 ask 단계를 필요로 했으므로
  일반화하면 안 됩니다.
- public trade 두 건은 시장 전체 체결 관측이며 CoinPilot 체결이 아닙니다.
- 짧은 event-window 결과를 연환산하거나 연 20% 수익 근거로 사용할 수 없습니다.

## 합성 비용·지연 hurdle

10,000개 결정론적 합성 이벤트 결과와 네 파일의 hash completion marker는
로컬 `artifacts/hft/foundation-simulation-v3`에 있습니다.

| 합성 시나리오 | 이벤트 구간 수익률 | 왕복거래 | 해석 |
|---|---:|---:|---|
| 무알파·기본비용 | -1.133% | 410 | 비용 기준선 |
| 약한 알파 0.18bp/event | -1.081% | 410 | 비용 허들 미달 |
| 약한 알파·수수료 2배 | -2.106% | 410 | 동일 결정 stress |
| 약한 알파·지연 5 | -0.901% | 328 | 거래 감소로 총손실 축소 |
| 강한 알파 4bp/event 예시 | +0.032% | 410 | 임의 hurdle 예시 |

약한 합성 엣지는 비용을 이기지 못했고 수수료 2배에서 손실이 거의 두 배가
됐습니다. 강한 4bp/event 사례만 가까스로 양수지만, 이는 데이터에서 추정한
알파가 아니라 손익분기 감각을 위한 입력값입니다. 모든 fill은
`l1_full_fill_no_impact` 합성 체결이므로 실시장 수익 근거가 아닙니다.

별도 3초 bounded 캡처도 raw·quality·public-trades·run 네 파일을 staging한 뒤
로컬 `artifacts/hft/bounded-capture-v3/events.complete.json`을
마지막에 생성했고, 10건(호가 8·공개체결 2)의 모든 member hash가 검증됐습니다.

## 다음 필수 순서

1. 30일 이상 public archive와 디스크·프로세스 가동률 수집
2. NTP offset과 처리 지연을 별도 계측하고 모든 gap 원인 분류
3. 날짜 단위 train/validation/test 분리와 purged walk-forward
4. 비용·latency·표시잔량 stress 후에도 양수인 후보만 한 번 동결
5. 미접촉 forward 기간과 최소 1,000회 독립 왕복 의사결정 평가
6. 4주 이상 주문 없는 shadow 운용
7. 그 뒤에도 통과할 때만 private fill/reconciliation과 주문 adapter를 별도 리뷰

운영 명령과 승격 기준은
[`HFT_RUNBOOK.md`](HFT_RUNBOOK.md)에 있습니다.
