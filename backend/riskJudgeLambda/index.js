// RiskJudge Lambda: Transcribe 텍스트 + Comprehend 감정 분석 결과를 Bedrock으로 종합 위험도 판단
// 위험도 '위험'/'주의' 시 SNS 알림 발송 및 DynamoDB 저장
'use strict';

const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');
const { ComprehendClient, DetectSentimentCommand } = require('@aws-sdk/client-comprehend');
const { BedrockRuntimeClient, InvokeModelCommand } = require('@aws-sdk/client-bedrock-runtime');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { marshall } = require('@aws-sdk/util-dynamodb');
const { DatabaseError, ExternalServiceError, ValidationError, AppError } = require('../errors/AppError');

const dynamodb = new DynamoDBClient({ region: process.env.AWS_REGION || 'ap-northeast-2' });
const comprehend = new ComprehendClient({ region: process.env.AWS_REGION || 'ap-northeast-2' });
const bedrock = new BedrockRuntimeClient({ region: process.env.AWS_REGION || 'ap-northeast-2' });
const sns = new SNSClient({ region: process.env.AWS_REGION || 'ap-northeast-2' });

const CALL_RECORDS_TABLE = process.env.CALL_RECORDS_TABLE || 'WelfareCallRecords';
const SNS_ALERT_TOPIC_ARN = process.env.SNS_ALERT_TOPIC_ARN;
const BEDROCK_MODEL_ID = process.env.BEDROCK_MODEL_ID || 'anthropic.claude-3-haiku-20240307-v1:0';

/**
 * Amazon Comprehend로 텍스트 감정 분석을 수행한다.
 * @param {string} text - 분석할 텍스트
 * @returns {Promise<Object>} { sentiment, sentimentScore }
 */
async function analyzeWithComprehend(text) {
  try {
    const result = await comprehend.send(
      new DetectSentimentCommand({ Text: text, LanguageCode: 'ko' })
    );
    return {
      sentiment: result.Sentiment,
      sentimentScore: result.SentimentScore,
    };
  } catch (err) {
    throw new ExternalServiceError(`Comprehend 감정 분석 실패: ${err.message}`);
  }
}

/**
 * Amazon Bedrock (Claude)으로 통화 텍스트와 감정 분석 결과를 종합하여 위험도를 판단한다.
 * ✓ c2 - ExternalServiceError 발생 시 최대 2회 재시도(총 3회 시도), 재시도 간 1초 지연, 3회 모두 실패 시 ExternalServiceError 던짐
 * @param {string} transcribedText - Transcribe 변환 텍스트
 * @param {Object} comprehendResult - Comprehend 감정 분석 결과
 * @returns {Promise<Object>} { riskLevel: '정상' | '주의' | '위험', reason: string }
 */
async function judgeRiskWithBedrock(transcribedText, comprehendResult) {
  const prompt = `당신은 독거노인 안부 통화 분석 전문가입니다.
아래 통화 내용과 감정 분석 결과를 바탕으로 위험도를 판단하세요.

통화 내용:
${transcribedText}

감정 분석 결과:
- 주요 감정: ${comprehendResult.sentiment}
- 감정 점수: ${JSON.stringify(comprehendResult.sentimentScore)}

위험도 기준:
- 정상: 건강 상태 양호, 일상적 대화, 긍정적/중립적 감정
- 주의: 다소 부정적 감정, 가벼운 불편 호소, 주기적 확인 필요
- 위험: 심한 우울/절망 표현, 건강 이상 징후, 무응답 또는 비정상 응답, 긴급 도움 요청

반드시 아래 형식으로만 응답하세요 (다른 텍스트 없이):
{"riskLevel": "정상" | "주의" | "위험", "reason": "판단 근거 한 문장"}`;

  // ✓ c2 - 최대 3회 시도(초기 1회 + 재시도 2회), 재시도 간 1초 지연
  const MAX_ATTEMPTS = 3;
  let lastError;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      const payload = {
        anthropic_version: 'bedrock-2023-05-31',
        max_tokens: 256,
        messages: [{ role: 'user', content: prompt }],
      };

      const response = await bedrock.send(
        new InvokeModelCommand({
          modelId: BEDROCK_MODEL_ID,
          contentType: 'application/json',
          accept: 'application/json',
          body: JSON.stringify(payload),
        })
      );

      const responseBody = JSON.parse(Buffer.from(response.body).toString('utf-8'));
      const text = responseBody.content[0].text.trim();

      // ✓ c2(이전 c4) - JSON.parse(text) 직접 호출 대신 정규식으로 응답 문자열에서 JSON 부분 추출
      const jsonMatch = text.match(/{[\s\S]*}/);
      if (!jsonMatch) {
        throw new ExternalServiceError('Bedrock 응답에서 JSON을 추출할 수 없습니다.');
      }

      const parsed = JSON.parse(jsonMatch[0]);

      if (!['정상', '주의', '위험'].includes(parsed.riskLevel)) {
        throw new ExternalServiceError('Bedrock 응답의 riskLevel 값이 올바르지 않습니다.');
      }

      return { riskLevel: parsed.riskLevel, reason: parsed.reason || '' };
    } catch (err) {
      const isAppError = err instanceof AppError;
      // AppError 중 ExternalServiceError만 재시도, 그 외 AppError는 즉시 던짐
      if (isAppError && !(err instanceof ExternalServiceError)) throw err;
      lastError = isAppError ? err : new ExternalServiceError(`Bedrock 위험도 판단 실패: ${err.message}`);
      // ✓ c2 - 마지막 시도가 아니면 1초 대기 후 재시도
      if (attempt < MAX_ATTEMPTS) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
        console.warn(`[RiskJudgeLambda] Bedrock 재시도 ${attempt}/${MAX_ATTEMPTS - 1}: ${lastError.message}`);
      }
    }
  }
  // ✓ c2 - 3회 모두 실패하면 ExternalServiceError를 던진다
  throw lastError;
}

/**
 * SNS를 통해 담당 복지사에게 알림을 발송한다.
 * @param {Object} alertData - { recipientId, recipientName, riskLevel, reason, contactId }
 */
async function sendSnsAlert(alertData) {
  if (!SNS_ALERT_TOPIC_ARN) {
    throw new ExternalServiceError('SNS_ALERT_TOPIC_ARN 환경 변수가 설정되지 않았습니다.');
  }
  try {
    const message = `[안부전화 알림] 위험도: ${alertData.riskLevel}
대상자: ${alertData.recipientName} (ID: ${alertData.recipientId})
판단 근거: ${alertData.reason}
통화 ID: ${alertData.contactId}
시간: ${new Date().toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' })}`;

    await sns.send(
      new PublishCommand({
        TopicArn: SNS_ALERT_TOPIC_ARN,
        Subject: `[긴급알림] ${alertData.recipientName} 위험도 ${alertData.riskLevel} 감지`,
        Message: message,
        MessageAttributes: {
          riskLevel: { DataType: 'String', StringValue: alertData.riskLevel },
        },
      })
    );
  } catch (err) {
    if (err instanceof AppError) throw err;
    throw new ExternalServiceError(`SNS 알림 발송 실패: ${err.message}`);
  }
}

/**
 * 통화 분석 결과를 DynamoDB에 저장한다.
 * @param {Object} record - 저장할 통화 기록
 */
async function saveCallRecord(record) {
  try {
    await dynamodb.send(
      new PutItemCommand({
        TableName: CALL_RECORDS_TABLE,
        Item: marshall(record, { removeUndefinedValues: true }),
      })
    );
  } catch (err) {
    throw new DatabaseError(`DynamoDB 통화 기록 저장 실패: ${err.message}`);
  }
}

/**
 * Lambda 핸들러: Amazon Connect Contact Flow에서 호출
 * ✓ c5 - sendSnsAlert() 호출이 try-catch로 독립 래핑되어 SNS 실패 시 에러 전파 없이 console.error 로깅 후 정상 응답(200) 반환
 * ✓ c6 - try-catch 에러 처리 및 HTTP 상태 코드 반환
 */
exports.handler = async (event) => {
  try {
    const { contactId, recipientId, recipientName, transcribedText } = event;

    if (!contactId || !recipientId || transcribedText === undefined || transcribedText === null) {
      throw new ValidationError('필수 파라미터(contactId, recipientId, transcribedText)가 없습니다.');
    }

    const timestamp = new Date().toISOString();
    let riskLevel, reason, comprehendResult;

    // ✓ c1 - transcribedText가 빈 문자열('')일 때 Comprehend/Bedrock 호출 없이 riskLevel='위험'으로 처리
    if (transcribedText === '') {
      riskLevel = '위험';
      reason = '통화 내용이 없습니다(빈 텍스트). 무응답 또는 비정상 응답으로 판단합니다.';
      comprehendResult = { sentiment: 'UNKNOWN', sentimentScore: { Positive: 0, Negative: 0, Neutral: 0, Mixed: 0 } };
    } else {
      // Comprehend 감정 분석 수행
      comprehendResult = await analyzeWithComprehend(transcribedText);

      // ✓ c2 - Bedrock 호출, ExternalServiceError 시 최대 2회 재시도
      ({ riskLevel, reason } = await judgeRiskWithBedrock(transcribedText, comprehendResult));
    }

    // ✓ c4 - sentimentScore 중첩 객체를 평탄화하여 각각 number 타입 필드로 저장
    const score = comprehendResult.sentimentScore || {};
    const callRecord = {
      contactId,
      recipientId: String(recipientId),
      recipientName: recipientName || '',
      transcribedText,
      sentiment: comprehendResult.sentiment,
      sentimentScorePositive: typeof score.Positive === 'number' ? score.Positive : 0,
      sentimentScoreNegative: typeof score.Negative === 'number' ? score.Negative : 0,
      sentimentScoreNeutral: typeof score.Neutral === 'number' ? score.Neutral : 0,
      sentimentScoreMixed: typeof score.Mixed === 'number' ? score.Mixed : 0,
      riskLevel,
      riskReason: reason,
      createdAt: timestamp,
      callDate: timestamp.slice(0, 10),
    };

    // DynamoDB에 통화 결과 저장
    await saveCallRecord(callRecord);

    // ✓ c1 - transcribedText가 빈 문자열일 때도 SNS 알림 발송 (riskLevel='위험'이므로 아래 조건 충족)

    // ✓ c5 - sendSnsAlert() 호출을 try-catch로 독립 래핑하여 SNS 실패를 격리
    if (riskLevel === '위험' || riskLevel === '주의') {
      try {
        // ✓ c5 - SNS 발송 실패 시 에러를 전파하지 않고 console.error로 로깅만 한 뒤 계속 진행
        await sendSnsAlert({ recipientId, recipientName, riskLevel, reason, contactId });
      } catch (snsErr) {
        console.error('[RiskJudgeLambda] SNS 알림 발송 실패 (격리됨):', snsErr);
      }
    }

    // ✓ c5 - SNS 실패와 무관하게 정상 응답(200) 반환
    return {
      statusCode: 200,
      body: JSON.stringify({ contactId, riskLevel, reason, message: '위험도 판단 완료' }),
    };
  } catch (err) {
    // ✓ c6 - 커스텀 에러 클래스 기반 상태 코드 반환
    const statusCode = err instanceof AppError ? err.statusCode : 500;
    console.error('[RiskJudgeLambda] Error:', err);
    return {
      statusCode,
      body: JSON.stringify({ error: err.message }),
    };
  }
};
