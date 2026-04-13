-- Contract Payment Terms Table
-- Stores payment terms (due days) for each contract

CREATE TABLE IF NOT EXISTS contract_payment_terms (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    contract_number VARCHAR(100) NOT NULL,
    payment_days INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    
    UNIQUE(user_id, contract_number)
);

CREATE INDEX IF NOT EXISTS idx_contract_terms_user ON contract_payment_terms(user_id);
CREATE INDEX IF NOT EXISTS idx_contract_terms_contract ON contract_payment_terms(contract_number);

COMMENT ON TABLE contract_payment_terms IS 'Payment terms (due days from invoice date) per contract';
COMMENT ON COLUMN contract_payment_terms.payment_days IS 'Number of days from invoice date until payment is due (0 = immediate)';

-- Sample data (optional - remove if you want to start empty)
-- INSERT INTO contract_payment_terms (user_id, contract_number, payment_days, created_by) VALUES
-- ('demo_user', 'PБПRV/1', 30, 'system'),
-- ('demo_user', 'П25/30/188', 45, 'system'),
-- ('demo_user', '79', 60, 'system')
-- ON CONFLICT (user_id, contract_number) DO NOTHING;
