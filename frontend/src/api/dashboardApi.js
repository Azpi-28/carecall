'use strict';

const BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api';

/**
 * 공통 fetch 래퍼 — 에러 응답을 Error로 변환한다.
 */
async function apiFetch(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

/** 대상자 목록 조회 */
export async function fetchRecipients() {
  const data = await apiFetch('/recipients');
  return data.recipients || [];
}

/** 오늘의 통화 현황 조회 */
export async function fetchTodayCallStatus() {
  return apiFetch('/calls/today');
}

/** 위험군 목록 조회 */
export async function fetchAtRiskList() {
  const data = await apiFetch('/calls/at-risk');
  return data.atRisk || [];
}

/** 특정 대상자 통화 이력 조회 */
export async function fetchCallHistory(recipientId) {
  const data = await apiFetch(`/calls/history/${recipientId}`);
  return data.history || [];
}

/** 위험도 수정 */
export async function updateRiskLevel({ recipientId, newRiskLevel, reason }) {
  return apiFetch(`/recipients/${recipientId}/risk-correction`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ newRiskLevel, reason }),
  });
}