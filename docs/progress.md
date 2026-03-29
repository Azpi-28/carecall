# 구현 현황 정리

## 프로젝트 개요
독거노인 AI 안부 전화 시스템 — 매일 자동으로 전화를 걸고, 음성을 분석해 위험도를 판단하며, 담당 복지사에게 알림을 보내는 서비스

---

## 백엔드 (AWS Lambda — 코드 구현 완료, 실제 연결 미완)

| 파일 | 역할 |
|---|---|
| `schedulerLambda/index.js` | EventBridge 트리거 → DynamoDB에서 대상자 조회 → Connect로 자동 발신 |
| `riskJudgeLambda/index.js` | 통화 종료 후 Comprehend(감정분석) → Bedrock(위험도 판단) → SNS 알림 → DynamoDB 저장 |
| `dashboardLambda/index.js` | API Gateway 연결 — `/recipients`, `/calls/today`, `/calls/at-risk`, `/calls/history/:id` |
| `errors/AppError.js` | 커스텀 에러 클래스 5종 (NotFound, Validation, ExternalService, Database, App) |

## 테스트 (Jest, 27개 전체 통과)

| 파일 | 테스트 수 |
|---|---|
| `AppError.test.js` | 6개 — 에러 클래스 statusCode 및 기본 메시지 |
| `schedulerLambda.test.js` | 5개 — 발신 성공/실패/환경변수 누락 등 |
| `riskJudgeLambda.test.js` | 7개 — 정상/주의/위험 분기, 파라미터 검증 등 |
| `dashboardLambda.test.js` | 9개 — 엔드포인트 3개, CORS, 에러 처리 등 |

---

## 프론트엔드 (React + Vite — 목데이터로 로컬 동작 중)

| 파일 | 역할 |
|---|---|
| `App.jsx` | 탭 기반 대시보드 루트 |
| `mockData.js` | 목 데이터 (대상자 6명, 통화기록 12건) |
| `RecipientList.jsx` | 대상자 목록 테이블 (이름/나이/주소/담당복지사/위험도) |
| `RiskStatusPanel.jsx` | 오늘 통화 현황 카드 + 주의·위험·미응답 목록 |
| `CallTimeline.jsx` | 통화 이력 타임라인 모달 (통화시간/통화시간(분)/응답여부) |

---

## 인프라

| 항목 | 상태 |
|---|---|
| GitHub (`potato-pancake-png/carecall`) | 완료 |
| Vercel 배포 | 진행 중 (Root Directory 설정 후 Redeploy) |
| AWS 연결 | 미완 — Lambda 배포, DynamoDB, Connect, Bedrock, SNS 설정 필요 |

---

## 다음 단계 (AWS 실제 연결 시)

1. DynamoDB 테이블 2개 생성 (WelfareRecipients, WelfareCallRecords)
2. Lambda 3개 배포 + IAM 권한 설정
3. Amazon Connect 인스턴스 + 전화번호 구매
4. API Gateway 연결
5. Vercel 환경변수에 `VITE_API_BASE_URL` 추가 → 목데이터 → 실제 API 전환
