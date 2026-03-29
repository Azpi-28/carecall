'use strict';

jest.mock('@aws-sdk/client-dynamodb');
jest.mock('@aws-sdk/client-comprehend');
jest.mock('@aws-sdk/client-bedrock-runtime');
jest.mock('@aws-sdk/client-sns');
jest.mock('@aws-sdk/util-dynamodb');

const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { ComprehendClient } = require('@aws-sdk/client-comprehend');
const { BedrockRuntimeClient } = require('@aws-sdk/client-bedrock-runtime');
const { SNSClient } = require('@aws-sdk/client-sns');
const { marshall } = require('@aws-sdk/util-dynamodb');

const mockDynamoSend = jest.fn();
const mockComprehendSend = jest.fn();
const mockBedrockSend = jest.fn();
const mockSnsSend = jest.fn();

DynamoDBClient.mockImplementation(() => ({ send: mockDynamoSend }));
ComprehendClient.mockImplementation(() => ({ send: mockComprehendSend }));
BedrockRuntimeClient.mockImplementation(() => ({ send: mockBedrockSend }));
SNSClient.mockImplementation(() => ({ send: mockSnsSend }));
marshall.mockImplementation((obj) => obj);

process.env.SNS_ALERT_TOPIC_ARN = 'arn:aws:sns:ap-northeast-2:123456789:WelfareAlert';

const { handler } = require('../riskJudgeLambda/index');

// Bedrock 응답 헬퍼
function makeBedrockResponse(riskLevel, reason = '테스트 근거') {
  const text = JSON.stringify({ riskLevel, reason });
  const responseBody = { content: [{ text }] };
  const body = Buffer.from(JSON.stringify(responseBody));
  return { body };
}

// Comprehend 응답 헬퍼
const comprehendOk = {
  Sentiment: 'NEGATIVE',
  SentimentScore: { Positive: 0.05, Negative: 0.8, Neutral: 0.1, Mixed: 0.05 },
};

describe('riskJudgeLambda', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const baseEvent = {
    contactId: 'c-001',
    recipientId: 'r-001',
    recipientName: '홍길동',
    transcribedText: '요즘 너무 힘들고 아무것도 하기 싫어요.',
  };

  test('정상 케이스: 위험도 정상 → SNS 미발송, DynamoDB 저장', async () => {
    mockComprehendSend.mockResolvedValueOnce(comprehendOk);
    mockBedrockSend.mockResolvedValueOnce(makeBedrockResponse('정상'));
    mockDynamoSend.mockResolvedValueOnce({});

    const result = await handler({ ...baseEvent, transcribedText: '오늘 날씨 좋네요. 잘 지내고 있어요.' });
    const body = JSON.parse(result.body);

    expect(result.statusCode).toBe(200);
    expect(body.riskLevel).toBe('정상');
    expect(mockSnsSend).not.toHaveBeenCalled();
    expect(mockDynamoSend).toHaveBeenCalledTimes(1);
  });

  test('위험 케이스: 위험도 위험 → SNS 발송 + DynamoDB 저장', async () => {
    mockComprehendSend.mockResolvedValueOnce(comprehendOk);
    mockBedrockSend.mockResolvedValueOnce(makeBedrockResponse('위험', '심한 우울 표현 감지'));
    mockDynamoSend.mockResolvedValueOnce({});
    mockSnsSend.mockResolvedValueOnce({});

    const result = await handler(baseEvent);
    const body = JSON.parse(result.body);

    expect(result.statusCode).toBe(200);
    expect(body.riskLevel).toBe('위험');
    expect(mockSnsSend).toHaveBeenCalledTimes(1);
    expect(mockDynamoSend).toHaveBeenCalledTimes(1);
  });

  test('주의 케이스: 위험도 주의 → SNS 발송 + DynamoDB 저장', async () => {
    mockComprehendSend.mockResolvedValueOnce(comprehendOk);
    mockBedrockSend.mockResolvedValueOnce(makeBedrockResponse('주의', '가벼운 불편 호소'));
    mockDynamoSend.mockResolvedValueOnce({});
    mockSnsSend.mockResolvedValueOnce({});

    const result = await handler(baseEvent);
    const body = JSON.parse(result.body);

    expect(result.statusCode).toBe(200);
    expect(body.riskLevel).toBe('주의');
    expect(mockSnsSend).toHaveBeenCalledTimes(1);
  });

  test('필수 파라미터 누락 시 400 에러 반환', async () => {
    const result = await handler({ contactId: 'c-001', recipientId: 'r-001' }); // transcribedText 없음
    expect(result.statusCode).toBe(400);
    expect(JSON.parse(result.body).error).toMatch('필수 파라미터');
  });

  test('Comprehend 실패 시 502 에러 반환', async () => {
    mockComprehendSend.mockRejectedValueOnce(new Error('Comprehend 연결 오류'));

    const result = await handler(baseEvent);
    expect(result.statusCode).toBe(502);
  });

  test('Bedrock이 잘못된 riskLevel 반환 시 502 에러 반환', async () => {
    mockComprehendSend.mockResolvedValueOnce(comprehendOk);
    mockBedrockSend.mockResolvedValueOnce(makeBedrockResponse('알수없음')); // 유효하지 않은 값

    const result = await handler(baseEvent);
    expect(result.statusCode).toBe(502);
  });

  test('DynamoDB 저장 실패 시 503 에러 반환', async () => {
    mockComprehendSend.mockResolvedValueOnce(comprehendOk);
    mockBedrockSend.mockResolvedValueOnce(makeBedrockResponse('정상'));
    mockDynamoSend.mockRejectedValueOnce(new Error('DynamoDB 저장 실패'));

    const result = await handler(baseEvent);
    // ✓ c5 - DatabaseError catch 시 HTTP 503 반환 검증
    expect(result.statusCode).toBe(503);
  });

  // ✓ c1 - transcribedText가 빈 문자열('')일 때: riskLevel='위험', Comprehend/Bedrock 미호출, SNS 1회 발송
  test('transcribedText가 빈 문자열일 때 위험으로 판단하고 Comprehend/Bedrock 미호출, SNS 1회 발송', async () => {
    mockDynamoSend.mockResolvedValueOnce({});
    mockSnsSend.mockResolvedValueOnce({});

    const result = await handler({ ...baseEvent, transcribedText: '' });
    const body = JSON.parse(result.body);

    // ✓ c1 - riskLevel이 '위험'이어야 함
    expect(result.statusCode).toBe(200);
    expect(body.riskLevel).toBe('위험');
    // ✓ c1 - Comprehend와 Bedrock은 호출되지 않아야 함
    expect(mockComprehendSend).not.toHaveBeenCalled();
    expect(mockBedrockSend).not.toHaveBeenCalled();
    // ✓ c1 - SNS는 정확히 1회 발송되어야 함
    expect(mockSnsSend).toHaveBeenCalledTimes(1);
    // ✓ c1 - DynamoDB 저장은 정상 완료되어야 함
    expect(mockDynamoSend).toHaveBeenCalledTimes(1);
  });

  // ✓ c1 - Bedrock이 첫 2회 실패 후 3회째 성공: statusCode 200, 올바른 riskLevel 반환
  test('Bedrock이 첫 2회 실패 후 3회째 성공 시 statusCode 200과 올바른 riskLevel 반환', async () => {
    jest.useFakeTimers();
    mockComprehendSend.mockResolvedValueOnce(comprehendOk);
    // 첫 번째, 두 번째 Bedrock 호출 실패
    mockBedrockSend
      .mockRejectedValueOnce(new Error('Bedrock 일시적 오류'))
      .mockRejectedValueOnce(new Error('Bedrock 일시적 오류'))
      // 세 번째 호출 성공
      .mockResolvedValueOnce(makeBedrockResponse('주의', '재시도 후 성공'));
    mockDynamoSend.mockResolvedValueOnce({});
    mockSnsSend.mockResolvedValueOnce({});

    // ✓ c1 - 재시도 지연(1초)을 가짜 타이머로 처리하여 테스트 속도 확보
    const handlerPromise = handler(baseEvent);
    // 재시도 타이머 2회 진행
    await jest.runAllTimersAsync();
    const result = await handlerPromise;
    const body = JSON.parse(result.body);

    jest.useRealTimers();

    // ✓ c1 - 3회째 성공 시 statusCode 200과 올바른 riskLevel 반환
    expect(result.statusCode).toBe(200);
    expect(body.riskLevel).toBe('주의');
    // Bedrock은 총 3회 호출되어야 함
    expect(mockBedrockSend).toHaveBeenCalledTimes(3);
  });

  // ✓ c1 - SNS 발송 실패 시에도 statusCode 200이 반환되고 DynamoDB 저장은 정상 완료됨
  test('SNS 발송 실패 시에도 statusCode 200이 반환되고 DynamoDB 저장은 정상 완료됨', async () => {
    mockComprehendSend.mockResolvedValueOnce(comprehendOk);
    mockBedrockSend.mockResolvedValueOnce(makeBedrockResponse('위험', '위험 감지'));
    mockDynamoSend.mockResolvedValueOnce({});
    // SNS 발송 실패
    mockSnsSend.mockRejectedValueOnce(new Error('SNS 연결 오류'));

    const result = await handler(baseEvent);
    const body = JSON.parse(result.body);

    // ✓ c1 - SNS 실패와 무관하게 statusCode 200 반환
    expect(result.statusCode).toBe(200);
    expect(body.riskLevel).toBe('위험');
    // ✓ c1 - DynamoDB 저장은 정상 완료
    expect(mockDynamoSend).toHaveBeenCalledTimes(1);
    // SNS는 호출 시도했으나 실패
    expect(mockSnsSend).toHaveBeenCalledTimes(1);
  });
});
