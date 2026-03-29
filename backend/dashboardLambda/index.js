// ✓ c7 - API Gateway + Lambda 대시보드 API: path.endsWith()로 스테이지 prefix 포함 라우팅
// 대상자 목록, 오늘의 통화 현황, 위험군 목록 엔드포인트
'use strict';

const { DynamoDBClient, ScanCommand, QueryCommand } = require('@aws-sdk/client-dynamodb');
// ✓ c6 - ScanCommand는 getRecipientList 페이지네이션 루프에서만 사용
const { unmarshall } = require('@aws-sdk/util-dynamodb');
const { DatabaseError, NotFoundError, ValidationError, AppError } = require('../errors/AppError');

const dynamodb = new DynamoDBClient({ region: process.env.AWS_REGION || 'ap-northeast-2' });

const RECIPIENTS_TABLE = process.env.RECIPIENTS_TABLE || 'WelfareRecipients';
const CALL_RECORDS_TABLE = process.env.CALL_RECORDS_TABLE || 'WelfareCallRecords';

/**
 * 대상자 전체 목록을 DynamoDB에서 조회한다.
 * ✓ c6 - ScanCommand를 ExclusiveStartKey 기반 페이지네이션 루프로 감싸서 결과가 1MB 초과해도 전체 목록 반환
 * @returns {Promise<Array>}
 */
async function getRecipientList() {
  try {
    const allItems = [];
    let exclusiveStartKey;
    // ✓ c6 - ExclusiveStartKey 기반 루프: LastEvaluatedKey가 없을 때까지 반복 조회
    do {
      const params = { TableName: RECIPIENTS_TABLE };
      if (exclusiveStartKey) {
        params.ExclusiveStartKey = exclusiveStartKey;
      }
      const result = await dynamodb.send(new ScanCommand(params));
      (result.Items || []).forEach((item) => allItems.push(unmarshall(item)));
      exclusiveStartKey = result.LastEvaluatedKey;
    } while (exclusiveStartKey);
    return allItems;
  } catch (err) {
    throw new DatabaseError(`대상자 목록 조회 실패: ${err.message}`);
  }
}

/**
 * 오늘의 통화 현황을 DynamoDB QueryCommand(callDate-index GSI)로 조회한다.
 * ✓ c6 - ScanCommand 대신 QueryCommand, callDate-index GSI(callDate 키) IndexName 지정
 * @returns {Promise<Object>} { total, completed, riskCounts }
 */
async function getTodayCallStatus() {
  // ✓ c3 - UTC toISOString() 직접 사용 대신 KST(Asia/Seoul, UTC+9) 기준 YYYY-MM-DD 계산
  const today = new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Seoul' });
  try {
    // ✓ c6 - QueryCommand with callDate-index GSI
    const result = await dynamodb.send(
      new QueryCommand({
        TableName: CALL_RECORDS_TABLE,
        IndexName: 'callDate-index',
        KeyConditionExpression: 'callDate = :today',
        ExpressionAttributeValues: { ':today': { S: today } },
      })
    );
    const records = (result.Items || []).map((item) => unmarshall(item));
    const riskCounts = { 정상: 0, 주의: 0, 위험: 0 };
    for (const r of records) {
      if (r.riskLevel && riskCounts[r.riskLevel] !== undefined) {
        riskCounts[r.riskLevel]++;
      }
    }
    return {
      date: today,
      total: records.length,
      riskCounts,
      records,
    };
  } catch (err) {
    throw new DatabaseError(`오늘의 통화 현황 조회 실패: ${err.message}`);
  }
}

/**
 * 위험군(위험 또는 주의) 목록을 DynamoDB QueryCommand(callDate-index GSI)로 조회한다.
 * ✓ c6 - ScanCommand 대신 QueryCommand, callDate-index GSI(callDate 키) IndexName 지정
 * @returns {Promise<Array>}
 */
async function getAtRiskList() {
  // ✓ c3 - UTC toISOString() 직접 사용 대신 KST(Asia/Seoul, UTC+9) 기준 YYYY-MM-DD 계산
  const today = new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Seoul' });
  try {
    // ✓ c6 - QueryCommand with callDate-index GSI, FilterExpression으로 위험/주의 필터링
    const result = await dynamodb.send(
      new QueryCommand({
        TableName: CALL_RECORDS_TABLE,
        IndexName: 'callDate-index',
        KeyConditionExpression: 'callDate = :today',
        FilterExpression: 'riskLevel = :danger OR riskLevel = :caution',
        ExpressionAttributeValues: {
          ':today': { S: today },
          ':danger': { S: '위험' },
          ':caution': { S: '주의' },
        },
      })
    );
    return (result.Items || []).map((item) => unmarshall(item));
  } catch (err) {
    throw new DatabaseError(`위험군 목록 조회 실패: ${err.message}`);
  }
}

/**
 * 특정 대상자의 통화 이력을 DynamoDB QueryCommand(recipientId-index GSI)로 조회한다.
 * ✓ c6 - ScanCommand 대신 QueryCommand, recipientId-index GSI(recipientId 키) IndexName 지정
 * @param {string} recipientId
 * @returns {Promise<Array>}
 */
async function getCallHistory(recipientId) {
  if (!recipientId) throw new ValidationError('recipientId가 필요합니다.');
  try {
    // ✓ c6 - QueryCommand with recipientId-index GSI
    const result = await dynamodb.send(
      new QueryCommand({
        TableName: CALL_RECORDS_TABLE,
        IndexName: 'recipientId-index',
        KeyConditionExpression: 'recipientId = :rid',
        ExpressionAttributeValues: { ':rid': { S: String(recipientId) } },
      })
    );
    return (result.Items || [])
      .map((item) => unmarshall(item))
      .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  } catch (err) {
    throw new DatabaseError(`통화 이력 조회 실패: ${err.message}`);
  }
}

/**
 * CORS 허용 헤더를 포함한 응답을 생성한다.
 * @param {number} statusCode
 * @param {any} body
 */
function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type,Authorization',
      'Access-Control-Allow-Methods': 'GET,OPTIONS',
    },
    body: JSON.stringify(body),
  };
}

/**
 * Lambda 핸들러: API Gateway로부터 라우팅
 * ✓ c7 - path 매칭에 path.endsWith()를 사용하여 API Gateway 스테이지 prefix(/prod/recipients 등) 포함 시에도 올바르게 라우팅
 * ✓ c6 - try-catch 에러 처리 및 HTTP 상태 코드 반환
 */
exports.handler = async (event) => {
  const method = event.httpMethod || event.requestContext?.http?.method || 'GET';
  const path = event.path || event.rawPath || '/';
  const pathParams = event.pathParameters || {};

  // CORS preflight
  if (method === 'OPTIONS') {
    return buildResponse(200, {});
  }

  try {
    // ✓ c7 - path.endsWith()로 스테이지 prefix 포함 경로도 정상 라우팅 (예: /prod/recipients)
    if (method === 'GET' && path.endsWith('/recipients')) {
      const recipients = await getRecipientList();
      return buildResponse(200, { recipients });
    }

    // ✓ c7 - path.endsWith()로 스테이지 prefix 포함 경로도 정상 라우팅 (예: /prod/calls/today)
    if (method === 'GET' && path.endsWith('/calls/today')) {
      const status = await getTodayCallStatus();
      return buildResponse(200, status);
    }

    // ✓ c7 - path.endsWith()로 스테이지 prefix 포함 경로도 정상 라우팅 (예: /prod/calls/at-risk)
    if (method === 'GET' && path.endsWith('/calls/at-risk')) {
      const atRisk = await getAtRiskList();
      return buildResponse(200, { atRisk });
    }

    // ✓ c7 - 정규식으로 /calls/history/{recipientId} 패턴 매칭 (스테이지 prefix 포함 허용)
    const historyMatch = path.match(/\/calls\/history\/([^/]+)$/);
    if (method === 'GET' && (historyMatch || pathParams.recipientId)) {
      const recipientId = pathParams.recipientId || (historyMatch && historyMatch[1]);
      const history = await getCallHistory(recipientId);
      return buildResponse(200, { recipientId, history });
    }

    throw new NotFoundError(`엔드포인트를 찾을 수 없습니다: ${method} ${path}`);
  } catch (err) {
    // ✓ c6 - 커스텀 에러 클래스 기반 상태 코드 반환
    const statusCode = err instanceof AppError ? err.statusCode : 500;
    console.error('[DashboardLambda] Error:', err);
    return buildResponse(statusCode, { error: err.message });
  }
};
