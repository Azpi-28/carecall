# S3 + CloudFront 배포 가이드

## 전체 흐름

```
git push (main)
    ↓
GitHub Actions 트리거
    ↓
npm run build → frontend/dist/ 생성
    ↓
AWS S3 버킷에 dist/ 업로드
    ↓
CloudFront 캐시 무효화
    ↓
https://도메인 으로 서비스
```

---

## Step 1. S3 버킷 생성

- 버킷 이름: `carecall-frontend` (전 세계 고유해야 함)
- 리전: `ap-northeast-2` (서울)
- **퍼블릭 액세스 차단: 활성화 유지** (CloudFront를 통해서만 접근)
- 정적 웹사이트 호스팅: 비활성화 (CloudFront Origin으로만 사용)

---

## Step 2. CloudFront 배포 생성

- Origin domain: 위에서 만든 S3 버킷 선택
- Origin access: **Origin Access Control(OAC)** 생성 후 연결
  - S3 버킷 정책에 CloudFront만 읽기 허용하는 정책 자동 생성됨
- Default root object: `index.html`
- **Custom error response 추가** (SPA 라우팅 필수)
  - HTTP 403 → `/index.html` → 200 반환
  - HTTP 404 → `/index.html` → 200 반환
- Price class: `Use only North America and Europe` 또는 `All` (비용 차이)
- HTTPS: 기본 제공 (CloudFront 도메인 `xxxx.cloudfront.net`)

---

## Step 3. IAM 사용자 생성 (GitHub Actions용)

- IAM → 사용자 생성 → 프로그래밍 방식 액세스
- 권한 정책 연결:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::carecall-frontend",
        "arn:aws:s3:::carecall-frontend/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "cloudfront:CreateInvalidation",
      "Resource": "arn:aws:cloudfront::*:distribution/*"
    }
  ]
}
```

- **Access Key ID** 와 **Secret Access Key** 발급 후 저장

---

## Step 4. GitHub Secrets 등록

GitHub 레포 → Settings → Secrets and variables → Actions:

| Secret 이름 | 값 |
|---|---|
| `AWS_ACCESS_KEY_ID` | IAM 액세스 키 |
| `AWS_SECRET_ACCESS_KEY` | IAM 시크릿 키 |
| `AWS_REGION` | `ap-northeast-2` |
| `S3_BUCKET` | `carecall-frontend` |
| `CLOUDFRONT_DISTRIBUTION_ID` | CloudFront 배포 ID |
| `VITE_API_BASE_URL` | API Gateway URL (추후 추가) |

---

## Step 5. GitHub Actions 워크플로우 추가

파일 경로: `.github/workflows/deploy-frontend.yml`

```yaml
name: Deploy Frontend

on:
  push:
    branches:
      - main
    paths:
      - 'frontend/**'

jobs:
  deploy:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'
          cache-dependency-path: frontend/package-lock.json

      - name: Install dependencies
        run: npm ci
        working-directory: frontend

      - name: Build
        run: npm run build
        working-directory: frontend
        env:
          VITE_API_BASE_URL: ${{ secrets.VITE_API_BASE_URL }}

      - name: Upload to S3
        run: |
          aws s3 sync frontend/dist/ s3://${{ secrets.S3_BUCKET }} --delete
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          AWS_DEFAULT_REGION: ${{ secrets.AWS_REGION }}

      - name: Invalidate CloudFront cache
        run: |
          aws cloudfront create-invalidation \
            --distribution-id ${{ secrets.CLOUDFRONT_DISTRIBUTION_ID }} \
            --paths "/*"
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          AWS_DEFAULT_REGION: ${{ secrets.AWS_REGION }}
```

---

## Step 6. 커스텀 도메인 연결 (선택)

도메인이 있다면:
1. ACM(AWS Certificate Manager) → 인증서 발급 (리전: **반드시 us-east-1**)
2. CloudFront → Alternate domain name(CNAME)에 도메인 입력
3. Route 53 또는 도메인 DNS에 CNAME 레코드 추가
   - `www.도메인` → CloudFront 도메인(`xxxx.cloudfront.net`)

---

## 배포 후 확인 순서

1. `git push origin main` (frontend 파일 수정 후)
2. GitHub Actions 탭에서 워크플로우 실행 확인
3. `https://xxxx.cloudfront.net` 접속 확인
4. API Gateway 연결 후 → GitHub Secrets의 `VITE_API_BASE_URL` 업데이트 → 재배포

---

## 비용 추정 (월)

| 서비스 | 예상 비용 |
|---|---|
| S3 저장 (< 1GB) | ~$0.02 |
| CloudFront (10GB 트래픽) | ~$0.85 |
| **합계** | **< $1** |
