// ✓ c5 - 대상자 목록 컴포넌트
import React from 'react';
import { useState } from 'react';
import RiskCorrectionModal from './RiskCorrectionModal';
import { updateRiskLevel } from '../api/dashboardApi';

const RISK_BADGE = {
  위험: { background: '#ff4d4f', color: '#fff' },
  주의: { background: '#faad14', color: '#fff' },
  정상: { background: '#52c41a', color: '#fff' },
};

function RecipientList({ recipients, onSelect }) {
  const [list, setList] = useState(recipients || []);
  const [correcting, setCorrecting] = useState(null);
  const [hoveredId, setHoveredId] = useState(null);

  const handleConfirm = async ({ recipientId, newRiskLevel, reason }) => {
    await updateRiskLevel({ recipientId, newRiskLevel, reason });
    setList((prev) =>
      prev.map((r) =>
        r.recipientId === recipientId ? { ...r, lastRiskLevel: newRiskLevel } : r
      )
    );
  };
  if (!recipients || recipients.length === 0) {
    return <p style={styles.empty}>등록된 대상자가 없습니다.</p>;
  }

  return (
    <div style={styles.container}>
      <h2 style={styles.title}>대상자 목록</h2>
      <table style={styles.table}>
        <thead>
          <tr>
            <th style={styles.th}>이름</th>
            <th style={styles.th}>나이</th>
            <th style={styles.th}>연락처</th>
            <th style={styles.th}>주소</th>
            <th style={styles.th}>담당 복지사</th>
            <th style={styles.th}>최근 위험도</th>
            <th style={styles.th}>이력</th>
          </tr>
        </thead>
        <tbody>
          {list.map((r) => (
            <tr key={r.recipientId} style={styles.tr}>
              <td style={styles.td}>{r.name}</td>
              <td style={styles.td}>{r.age}세</td>
              <td style={styles.td}>{r.phoneNumber}</td>
              <td style={styles.td}>{r.address}</td>
              <td style={styles.td}>{r.assignedWorker || '-'}</td>
              <td
                style={styles.td}
                onMouseEnter={() => setHoveredId(r.recipientId)}
                onMouseLeave={() => setHoveredId(null)}
              >
                <div style={{ display: 'flex', alignItems: 'center' }}>
                  {r.lastRiskLevel ? (
                    <span style={{ ...styles.badge, ...RISK_BADGE[r.lastRiskLevel] }}>
                      {r.lastRiskLevel}
                    </span>
                  ) : (
                    <span style={{ ...styles.badge, background: '#8c8c8c', color: '#fff' }}>미통화</span>
                  )}
                  <span
                    style={{
                      ...styles.editHint,
                      opacity: hoveredId === r.recipientId ? 1 : 0,
                    }}
                    onClick={() => setCorrecting({ recipient: r })}
                  >
                    ✏️
                  </span>
                </div>
              </td>
              <td style={styles.td}>
                <button style={styles.button} onClick={() => onSelect && onSelect(r)}>
                  이력 보기
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {correcting && (
        <RiskCorrectionModal
          recipient={correcting.recipient}
          currentRiskLevel={correcting.recipient.lastRiskLevel}
          onConfirm={handleConfirm}
          onClose={() => setCorrecting(null)}
        />
      )}
    </div>
  );
}

const styles = {
  container: { marginBottom: '32px' },
  title: { fontSize: '18px', fontWeight: 'bold', marginBottom: '12px' },
  table: { width: '100%', borderCollapse: 'collapse' },
  th: { background: '#f0f2f5', padding: '10px 12px', textAlign: 'left', fontWeight: '600', borderBottom: '1px solid #d9d9d9', fontSize: '13px' },
  tr: { borderBottom: '1px solid #f0f0f0' },
  td: { padding: '10px 12px', verticalAlign: 'middle', fontSize: '14px' },
  badge: { padding: '2px 10px', borderRadius: '4px', fontSize: '12px', fontWeight: '600' },
  button: { padding: '4px 12px', fontSize: '12px', cursor: 'pointer', border: '1px solid #1890ff', color: '#1890ff', background: '#fff', borderRadius: '4px' },
  empty: { color: '#999' },
  editHint: {
    fontSize: '13px', cursor: 'pointer',
    transition: 'opacity 0.15s ease',
    userSelect: 'none', marginLeft: '4px',
  },
};

export default RecipientList;
