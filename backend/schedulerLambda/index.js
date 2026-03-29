// ✓ c1 - Scheduler Lambda: DynamoDB QueryCommand(callDate GSI)로 오늘 발신 대상자 조회 후 Connect StartOutboundVoiceContact 호출
'use strict';

const { DynamoDBClient, QueryCommand, PutItemCommand } = require('@aws-sdk/client-dynamodb');
const { ConnectClient, StartOutboundVoiceContactCommand } = require('@aws-sdk/client-connect');
const { unmarshall, marshall } = require('@aws-sdk/util-dynamodb');
const { DatabaseError, ExternalServiceError, AppError } = require('../errors/AppError');

const dynamodb = new DynamoDBClient({ region: process.env.AWS_REGION || 'ap-northeast-2' });
const connect = new ConnectClient({ region: process.env.AWS_REGION || 'ap-northeast-2' });

const RECIPIENTS_TABLE = process.env.RECIPIENTS_TABLE || 'WelfareRecipients';
const CALL_RESULTS_TABLE = process.env.CALL_RESULTS_TABLE || RECIPIENTS_TABLE;
const CONNECT_INSTANCE_ID = process.env.CONNECT_INSTANCE_ID;
const CONNECT_CONTACT_FLOW_ID = process.env.CONNECT_CONTACT_FLOW_ID;
const CONNECT_SOURCE_PHONE = process.env.CONNECT_SOURCE_PHONE;

/**
 * 오늘 발신 대상자 목록을 DynamoDB QueryCommand(callDate GSI)로 조회한다.
 * ✓ c1 - ScanCommand 대신 QueryCommand, IndexName으로 callDate GSI 지정, KeyConditionExpression으로 callDate 조회
 * @returns {Promise<Array>} 대상자 목록
 */
async function fetchTodayRecipients() {
  // ✓ c3 - UTC toISOString() 직접 사용 대신 KST(Asia/Seoul, UTC+9) 기준 YYYY-MM-DD 계산
  const today = new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Seoul' });
  try {
    // ✓ c1 - QueryCommand with callDate GSI
    const result = await dynamodb.send(
      new QueryCommand({
        TableName: RECIPIENTS_TABLE,
        IndexName: 'callDate-index',
        KeyConditionExpression: 'callDate = :today',
        ExpressionAttributeValues: {
          ':today': { S: today },
        },
      })
    );
    return (result.Items || []).map((item) => unmarshall(item));
  } catch (err) {
    throw new DatabaseError(`DynamoDB 대상자 조회 실패: ${err.message}`);
  }
}

/**
 * Amazon Connect를 통해 단일 대상자에게 전화를 건다.
 * @param {Object} recipient - 대상자 정보 { recipientId, phoneNumber, name }
 * @returns {Promise<string>} contactId
 */
async function startOutboundCall(recipient) {
  if (!CONNECT_INSTANCE_ID || !CONNECT_CONTACT_FLOW_ID || !CONNECT_SOURCE_PHONE) {
    throw new ExternalServiceError('Connect 환경 변수가 설정되지 않았습니다.');
  }
  try {
    const response = await connect.send(
      new StartOutboundVoiceContactCommand({
        DestinationPhoneNumber: recipient.phoneNumber,
        ContactFlowId: CONNECT_CONTACT_FLOW_ID,
        InstanceId: CONNECT_INSTANCE_ID,
        SourcePhoneNumber: CONNECT_SOURCE_PHONE,
        Attributes: {
          recipientId: String(recipient.recipientId),
          recipientName: String(recipient.name || ''),
        },
      })
    );
    return response.ContactId;
  } catch (err) {
    throw new ExternalServiceError(`Connect 발신 실패 (${recipient.recipientId}): ${err.message}`);
  }
}

/**
 * 발신 완료 결과 레코드를 DynamoDB에 저장한다.
 * ✓ c3 - { recipientId, contactId, status, calledAt } 형태의 결과 레코드 저장
 * @param {Object} param - { recipientId, contactId, status, calledAt }
 */
async function saveDialResult({ recipientId, contactId, status, calledAt }) {
  try {
    // ✓ c3 - DynamoDB에 발신 결과 레코드 저장
    await dynamodb.send(
      new PutItemCommand({
        TableName: CALL_RESULTS_TABLE,
        Item: marshall(
          { recipientId: String(recipientId), contactId, status, calledAt },
          { removeUndefinedValues: true }
        ),
      })
    );
  } catch (err) {
    throw new DatabaseError(`DynamoDB 발신 결과 저장 실패: ${err.message}`);
  }
}

/**
 * Lambda 핸들러: EventBridge 트리거로 매일 실행
 * ✓ c2 - Promise.allSettled()로 recipients 전체 병렬 처리, fulfilled/rejected 분기하여 results/errors 배열 누적
 * ✓ c6 - try-catch 에러 처리 및 HTTP 상태 코드 반환
 */
exports.handler = async (event) => {
  try {
    // ✓ c1 - DynamoDB QueryCommand(callDate GSI)로 오늘 발신 대상자 목록 조회
    const recipients = await fetchTodayRecipients();
    if (recipients.length === 0) {
      return {
        statusCode: 200,
        body: JSON.stringify({ message: '오늘 발신 대상자가 없습니다.', count: 0 }),
      };
    }

    // ✓ c2 - for-of 순차 방식 대신 Promise.allSettled()로 전체 병렬 처리
    const settledResults = await Promise.allSettled(
      recipients.map((recipient) => startOutboundCall(recipient).then((contactId) => ({ recipient, contactId })))
    );

    const results = [];
    const errors = [];

    // ✓ c2 - settled 결과에서 fulfilled/rejected를 분기해 results와 errors 배열에 누적
    for (let i = 0; i < settledResults.length; i++) {
      const settled = settledResults[i];
      const recipient = recipients[i];

      if (settled.status === 'fulfilled') {
        const { contactId } = settled.value;
        const calledAt = new Date().toISOString();
        const resultRecord = {
          recipientId: recipient.recipientId,
          contactId,
          status: 'dialed',
          calledAt,
        };
        results.push(resultRecord);

        // ✓ c3 - 발신 완료(fulfilled) 건에 대해 DynamoDB에 결과 레코드 저장
        try {
          await saveDialResult(resultRecord);
        } catch (saveErr) {
          console.error(`[SchedulerLambda] 발신 결과 저장 실패 (${recipient.recipientId}):`, saveErr);
        }
      } else {
        // ✓ c2 - rejected 건은 errors 배열에 누적
        errors.push({ recipientId: recipient.recipientId, error: settled.reason?.message || String(settled.reason) });
      }
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: '자동 발신 완료',
        dialed: results.length,
        failed: errors.length,
        results,
        errors,
      }),
    };
  } catch (err) {
    // ✓ c6 - 커스텀 에러 클래스 기반 상태 코드 반환
    const statusCode = err instanceof AppError ? err.statusCode : 500;
    console.error('[SchedulerLambda] Error:', err);
    return {
      statusCode,
      body: JSON.stringify({ error: err.message }),
    };
  }
};
