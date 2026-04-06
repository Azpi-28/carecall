// RiskCorrectionModal.jsx — 위험도 정정 모달
import React, { useState } from 'react';

const RISK_LEVELS = ['정상', '주의', '위험'];

const OPTION_STYLE = {
  정상: { border: '2px solid #52c41a', color: '#389e0d', background: '#f6ffed', selectedBg: '#52c41a' },
  주의: { border: '2px solid #faad14', color: '#d48806', background: '#fffbe6', selectedBg: '#faad14' },
  위험: { border: '2px solid #ff4d4f', color: '#cf1322', background: '#fff1f0', selectedBg: '#ff4d4f' },
};

function RiskCorrectionModal({ recipient, currentRiskLevel, onConfirm, onClose }) {
  const [selected, setSelected] = useState(currentRiskLevel || '정상');
  const [reason, setReason] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleConfirm = async () => {
    if (selected === currentRiskLevel) {
      onClose();
      return;
    }
    setLoading(true);
    setError(null);
    try {
      await onConfirm({ recipientId: recipient.recipientId, newRiskLevel: selected, reason });
      onClose();
    } catch (e) {
      setError(e.message || '정정 중 오류가 발생했습니다.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={styles.overlay}>
      <div style={styles.modal}>
        {/* 헤더 */}
        <div style={styles.header}>
          <div>
            <div style={styles.headerLabel}>위험도 정정</div>
            <div style={styles.headerName}>{recipient.name}</div>
          </div>
          <button style={styles.closeBtn} onClick={onClose} disabled={loading}>✕</button>
        </div>

        {/* 현재 위험도 */}
        <div style={styles.currentRow}>
          <span style={styles.currentLabel}>AI 판단 위험도</span>
          <span style={{
            ...styles.badge,
            background: currentRiskLevel ? OPTION_STYLE[currentRiskLevel]?.selectedBg : '#8c8c8c',
          }}>
            {currentRiskLevel || '미통화'}
          </span>
          <span style={styles.arrow}>→</span>
          <span style={styles.currentLabel}>정정할 위험도</span>
          <span style={{
            ...styles.badge,
            background: OPTION_STYLE[selected]?.selectedBg,
          }}>
            {selected}
          </span>
        </div>

        {/* 위험도 선택 */}
        <div style={styles.optionRow}>
          {RISK_LEVELS.map((level) => {
            const s = OPTION_STYLE[level];
            const isSelected = selected === level;
            return (
              <button
                key={level}
                onClick={() => setSelected(level)}
                style={{
                  ...styles.optionBtn,
                  border: s.border,
                  color: isSelected ? '#fff' : s.color,
                  background: isSelected ? s.selectedBg : s.background,
                  fontWeight: isSelected ? '700' : '500',
                  transform: isSelected ? 'scale(1.04)' : 'scale(1)',
                  boxShadow: isSelected ? `0 2px 8px ${s.selectedBg}55` : 'none',
                }}
                disabled={loading}
              >
                {isSelected && <span style={styles.checkmark}>✓ </span>}
                {level}
              </button>
            );
          })}
        </div>

        {/* 정정 사유 */}
        <div style={styles.reasonSection}>
          <label style={styles.reasonLabel}>정정 사유 <span style={styles.optional}>(선택)</span></label>
          <textarea
            style={styles.textarea}
            placeholder="AI 판단과 다른 이유를 입력해 주세요."
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            rows={3}
            disabled={loading}
          />
        </div>

        {/* 에러 */}
        {error && <p style={styles.error}>⚠ {error}</p>}

        {/* 버튼 */}
        <div style={styles.footer}>
          <button style={styles.cancelBtn} onClick={onClose} disabled={loading}>취소</button>
          <button
            style={{
              ...styles.confirmBtn,
              opacity: loading ? 0.7 : 1,
              cursor: loading ? 'not-allowed' : 'pointer',
            }}
            onClick={handleConfirm}
            disabled={loading}
          >
            {loading ? '저장 중...' : '정정 완료'}
          </button>
        </div>
      </div>
    </div>
  );
}

const styles = {
  overlay: {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 2000,
  },
  modal: {
    background: '#fff', borderRadius: '12px', padding: '28px 32px',
    width: '440px', boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
  },
  header: {
    display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '20px',
  },
  headerLabel: { fontSize: '12px', color: '#8c8c8c', marginBottom: '4px', letterSpacing: '0.5px' },
  headerName: { fontSize: '20px', fontWeight: '700', color: '#1a1a1a' },
  closeBtn: {
    background: 'none', border: 'none', fontSize: '18px', cursor: 'pointer',
    color: '#999', padding: '4px', lineHeight: 1,
  },
  currentRow: {
    display: 'flex', alignItems: 'center', gap: '10px',
    background: '#f7f8fa', borderRadius: '8px', padding: '12px 16px',
    marginBottom: '20px', flexWrap: 'wrap',
  },
  currentLabel: { fontSize: '12px', color: '#666' },
  badge: {
    padding: '3px 10px', borderRadius: '4px', fontSize: '12px',
    fontWeight: '700', color: '#fff',
  },
  arrow: { fontSize: '16px', color: '#bbb' },
  optionRow: { display: 'flex', gap: '10px', marginBottom: '20px' },
  optionBtn: {
    flex: 1, padding: '12px 0', borderRadius: '8px', fontSize: '15px',
    cursor: 'pointer', transition: 'all 0.15s ease',
  },
  checkmark: { fontSize: '13px' },
  reasonSection: { marginBottom: '20px' },
  reasonLabel: { display: 'block', fontSize: '13px', color: '#444', marginBottom: '6px', fontWeight: '600' },
  optional: { fontWeight: '400', color: '#aaa', fontSize: '12px' },
  textarea: {
    width: '100%', padding: '10px 12px', borderRadius: '6px',
    border: '1px solid #d9d9d9', fontSize: '13px', color: '#333',
    resize: 'vertical', boxSizing: 'border-box', outline: 'none',
    fontFamily: 'inherit',
  },
  error: { color: '#ff4d4f', fontSize: '13px', marginBottom: '12px' },
  footer: { display: 'flex', gap: '10px', justifyContent: 'flex-end' },
  cancelBtn: {
    padding: '8px 20px', border: '1px solid #d9d9d9', borderRadius: '6px',
    background: '#fff', cursor: 'pointer', fontSize: '14px', color: '#595959',
  },
  confirmBtn: {
    padding: '8px 24px', border: 'none', borderRadius: '6px',
    background: '#1890ff', color: '#fff', fontWeight: '600',
    fontSize: '14px',
  },
};

export default RiskCorrectionModal;
