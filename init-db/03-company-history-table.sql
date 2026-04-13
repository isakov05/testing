-- ============================================================================
-- COMPANY HISTORY TABLE MIGRATION
-- ============================================================================
-- This migration adds a table to store historical changes to company data
-- from the MyOrg API history endpoint.
-- Endpoint: GET /api/entity/inn/{inn}/history
-- Date: 2025-11-22
-- ============================================================================

-- ============================================================================
-- COMPANY_HISTORY TABLE
-- ============================================================================
-- Stores historical changes to company information, tracking both the 
-- "before" and "after" states of company data.
-- ============================================================================

CREATE TABLE IF NOT EXISTS company_history (
    id SERIAL PRIMARY KEY,

    -- Reference fields
    inn VARCHAR(20) NOT NULL,

    -- Change tracking
    change_id VARCHAR(100),  -- Generated from inn + created_at
    created_at TIMESTAMP NOT NULL,  -- When the change occurred

    -- Before state (stored as JSONB for flexibility)
    before_state JSONB,
    
    -- After state (stored as JSONB for flexibility)
    after_state JSONB,

    -- Extracted key fields from "before" state for easier querying
    before_name VARCHAR(500),
    before_status INTEGER,
    before_activity_state INTEGER,
    before_director VARCHAR(500),
    before_registration_date DATE,
    before_opf VARCHAR(100),
    before_oked VARCHAR(100),

    -- Extracted key fields from "after" state for easier querying
    after_name VARCHAR(500),
    after_status INTEGER,
    after_activity_state INTEGER,
    after_director VARCHAR(500),
    after_registration_date DATE,
    after_opf VARCHAR(100),
    after_oked VARCHAR(100),

    -- Metadata
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate history records
    UNIQUE(inn, created_at, change_id)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_company_history_inn ON company_history(inn);
CREATE INDEX IF NOT EXISTS idx_company_history_created_at ON company_history(created_at);
CREATE INDEX IF NOT EXISTS idx_company_history_change_id ON company_history(change_id);
CREATE INDEX IF NOT EXISTS idx_company_history_before_state ON company_history USING GIN (before_state);
CREATE INDEX IF NOT EXISTS idx_company_history_after_state ON company_history USING GIN (after_state);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_company_history_inn_created_at ON company_history(inn, created_at DESC);

-- Comments
COMMENT ON TABLE company_history IS 'Historical changes to company information from MyOrg API - endpoint: /api/entity/inn/{inn}/history';
COMMENT ON COLUMN company_history.before_state IS 'Complete company data before the change (JSONB)';
COMMENT ON COLUMN company_history.after_state IS 'Complete company data after the change (JSONB)';
COMMENT ON COLUMN company_history.change_id IS 'Unique identifier for this change (generated from inn + created_at)';

-- ============================================================================
-- TRIGGER FOR UPDATED_AT TIMESTAMP
-- ============================================================================

-- Reuse existing function or create if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'update_updated_at_column') THEN
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $func$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $func$ LANGUAGE plpgsql;
    END IF;
END $$;

-- Create trigger for company_history
DROP TRIGGER IF EXISTS update_company_history_updated_at ON company_history;
CREATE TRIGGER update_company_history_updated_at
    BEFORE UPDATE ON company_history
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE 'Company history table migration completed successfully!';
    RAISE NOTICE 'Created table: company_history';
    RAISE NOTICE 'This table stores historical changes to company data from MyOrg API';
END $$;

