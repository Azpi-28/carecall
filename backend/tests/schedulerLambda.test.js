'use strict';

// AWS SDK 모킹
jest.mock('@aws-sdk/client-dynamodb');
jest.mock('@aws-sdk/client-connect');
jest.mock('@aws-sdk/util-dynamodb');

const { DynamoDBClient, ScanCommand } = require('@aws-sdk/client-dynamodb');
const { ConnectClient, StartOutboundVoiceContactCommand } = require('@aws-sdk/client-connect');
const { unmarshall } = require('@aws-sdk/util-dynamodb');

// 모킹 설정 - send 메서드를 jest.fn()으로
const mockDynamoSend = jest.fn();
const mockConnectSend = jest.fn();
DynamoDBClient.mockImplementation(() => ({ send: mockDynamoSend }));
ConnectClient.mockImplementation(() => ({ send: mockConnectSend }));

// unmarshall은 실제 동작처럼 그대로 반환
unmarshall.mockImplementation((item) => {
  const result = {};
  for (const [k, v] of Object.entries(item)) {
    result[k] = Object.values(v)[0];
  }
  return result;
});

// 환경 변수 설정 (모듈 로드 전)
process.env.CONNECT_INSTANCE_ID = 'test-instance-id';
process.env.CONNECT_CONTACT_FLOW_ID = 'test-flow-id';
process.env.CONNECT_SOURCE_PHONE = '+821012345678';

const { handler } = require('../schedulerLambda/index');

describe('schedulerLambda', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('대상자가 없으면 count:0 응답을 반환한다', async () => {
    mockDynamoSend.mockResolvedValueOnce({ Items: [] });

    const result = await handler({});
    const body = JSON.parse(result.body);

    expect(result.statusCode).toBe(200);
    expect(body.count).toBe(0);
    expect(body.message).toMatch('오늘 발신 대상자가 없습니다');
  });

  test('대상자가 있으면 Connect에 발신 요청을 하고 결과를 반환한다', async () => {
    mockDynamoSend.mockResolvedValueOnce({
      Items: [
        { recipientId: { S: 'r1' }, phoneNumber: { S: '+82101111111' }, name: { S: '홍길동' } },
        { recipientId: { S: 'r2' }, phoneNumber: { S: '+82102222222' }, name: { S: '김영희' } },
      ],
    });
    mockConnectSend
      .mockResolvedValueOnce({ ContactId: 'contact-001' })
      .mockResolvedValueOnce({ ContactId: 'contact-002' });

    const result = await handler({});
    const body = JSON.parse(result.body);

    expect(result.statusCode).toBe(200);
    expect(body.dialed).toBe(2);
    expect(body.failed).toBe(0);
    expect(mockConnectSend).toHaveBeenCalledTimes(2);
  });

  test('Connect 발신 실패 시 errors 배열에 기록하고 계속 진행한다', async () => {
    mockDynamoSend.mockResolvedValueOnce({
      Items: [
        { recipientId: { S: 'r1' }, phoneNumber: { S: '+82101111111' }, name: { S: '홍길동' } },
        { recipientId: { S: 'r2' }, phoneNumber: { S: '+82102222222' }, name: { S: '김영희' } },
      ],
    });
    mockConnectSend
      .mockResolvedValueOnce({ ContactId: 'contact-001' })
      .mockRejectedValueOnce(new Error('Connect API 오류'));

    const result = await handler({});
    const body = JSON.parse(result.body);

    expect(result.statusCode).toBe(200);
    expect(body.dialed).toBe(1);
    expect(body.failed).toBe(1);
  });

  test('DynamoDB 조회 실패 시 503 에러를 반환한다', async () => {
    mockDynamoSend.mockRejectedValueOnce(new Error('DynamoDB 연결 실패'));

    const result = await handler({});

    // ✓ c5 - DatabaseError catch 시 HTTP 503 반환 검증
    expect(result.statusCode).toBe(503);
    expect(JSON.parse(result.body).error).toMatch('DynamoDB');
  });

  test('Connect 환경 변수 누락 시 502 에러를 반환한다', async () => {
    const originalInstanceId = process.env.CONNECT_INSTANCE_ID;
    delete process.env.CONNECT_INSTANCE_ID;

    mockDynamoSend.mockResolvedValueOnce({
      Items: [
        { recipientId: { S: 'r1' }, phoneNumber: { S: '+82101111111' }, name: { S: '홍길동' } },
      ],
    });

    const result = await handler({});
    const body = JSON.parse(result.body);

    // ✓ c3 - 환경변수 누락 시 handler 최상단에서 ExternalServiceError(502)로 즉시 반환
    expect(result.statusCode).toBe(502);

    process.env.CONNECT_INSTANCE_ID = originalInstanceId;
  });

  // ✓ c2 - Connect 발신 성공 후 saveDialResult DynamoDB 저장 실패 시: 전체 statusCode 200, dialed 카운트 증가, 처리 중단 없음
  test('Connect 발신 성공 후 saveDialResult DynamoDB 저장 실패 시 statusCode 200이고 dialed 카운트가 증가한다', async () => {
    // DynamoDB QueryCommand(fetchTodayRecipients)는 성공, PutItemCommand(saveDialResult)는 실패
    mockDynamoSend
      .mockResolvedValueOnce({
        Items: [
          { recipientId: { S: 'r1' }, phoneNumber: { S: '+82101111111' }, name: { S: '홍길동' } },
          { recipientId: { S: 'r2' }, phoneNumber: { S: '+82102222222' }, name: { S: '김영희' } },
        ],
      })
      // saveDialResult - r1 저장 실패
      .mockRejectedValueOnce(new Error('DynamoDB PutItem 실패'))
      // saveDialResult - r2 저장 실패
      .mockRejectedValueOnce(new Error('DynamoDB PutItem 실패'));

    // Connect 발신은 두 건 모두 성공
    mockConnectSend
      .mockResolvedValueOnce({ ContactId: 'contact-001' })
      .mockResolvedValueOnce({ ContactId: 'contact-002' });

    const result = await handler({});
    const body = JSON.parse(result.body);

    // ✓ c2 - 전체 statusCode는 200이어야 함
    expect(result.statusCode).toBe(200);
    // ✓ c2 - Connect 발신은 성공했으므로 dialed 카운트 2 증가
    expect(body.dialed).toBe(2);
    // ✓ c2 - Connect 발신 실패는 0
    expect(body.failed).toBe(0);
    // ✓ c2 - 처리가 중단되지 않고 Connect는 2회 호출됨
    expect(mockConnectSend).toHaveBeenCalledTimes(2);
  });
});
