-- ============================================================================
-- company ADDITIONAL DATA TABLES MIGRATION
-- ============================================================================
-- This migration adds 8 new tables to store additional company API data
-- collected via the bulk data collection script.
-- Date: 2025-11-20
-- ============================================================================
--
-- Tables created:
--   1. company_deals - Company deals and transactions
--   2. company_court_cases - Legal cases involving companies
--   3. company_connections - Related companies and connections
--   4. company_liabilities - Company financial liabilities
--   5. company_licenses - Business licenses and permits
--   6. company_founder_connections - Connections through founders/directors
--   7. company_ratings - Company credit ratings and scores
--   8. company_collaterals - Collateral and pledge information
--
-- ============================================================================

-- ============================================================================
-- 1. company DEALS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS company_deals (
    id SERIAL PRIMARY KEY,

    -- Reference fields
    inn VARCHAR(20) NOT NULL,
    company_id INTEGER NOT NULL,

    -- Deal identification
    deal_id VARCHAR(100),
    deal_type VARCHAR(50),  -- 'customer' or 'provider'
    deal_year INTEGER,
    deal_quarter INTEGER,

    -- Counterparty information
    counterparty_inn VARCHAR(20),
    counterparty_name VARCHAR(500),
    counterparty_company_id INTEGER,

    -- Financial information
    deal_amount DECIMAL(15,2),
    deal_count INTEGER,

    -- Complete raw data from API
    raw_data JSONB NOT NULL,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate deals
    UNIQUE(inn, company_id, deal_id)
);

CREATE INDEX IF NOT EXISTS idx_company_deals_inn ON company_deals(inn);
CREATE INDEX IF NOT EXISTS idx_company_deals_company_id ON company_deals(company_id);
CREATE INDEX IF NOT EXISTS idx_company_deals_type ON company_deals(deal_type);
CREATE INDEX IF NOT EXISTS idx_company_deals_counterparty_inn ON company_deals(counterparty_inn);
CREATE INDEX IF NOT EXISTS idx_company_deals_year ON company_deals(deal_year);
CREATE INDEX IF NOT EXISTS idx_company_deals_raw_data ON company_deals USING GIN (raw_data);

COMMENT ON TABLE company_deals IS 'Company deals and transactions from company API - endpoint: /api/deal/{company_id}';

-- ============================================================================
-- 2. company COURT CASES TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS company_court_cases (
    id SERIAL PRIMARY KEY,

    -- Reference fields
    inn VARCHAR(20) NOT NULL,

    -- Case identification
    case_number VARCHAR(200),
    case_id VARCHAR(100),
    case_type VARCHAR(100),
    case_category VARCHAR(200),

    -- Court information
    court_name VARCHAR(500),
    court_region VARCHAR(200),

    -- Case details
    plaintiff_name VARCHAR(500),
    plaintiff_inn VARCHAR(20),
    defendant_name VARCHAR(500),
    defendant_inn VARCHAR(20),

    -- Case status and dates
    case_status VARCHAR(100),
    filing_date DATE,
    decision_date DATE,

    -- Financial information
    claim_amount DECIMAL(15,2),
    decision_amount DECIMAL(15,2),

    -- Complete raw data from API
    raw_data JSONB NOT NULL,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate cases
    UNIQUE(inn, case_number, case_id)
);

CREATE INDEX IF NOT EXISTS idx_company_court_inn ON company_court_cases(inn);
CREATE INDEX IF NOT EXISTS idx_company_court_case_type ON company_court_cases(case_type);
CREATE INDEX IF NOT EXISTS idx_company_court_status ON company_court_cases(case_status);
CREATE INDEX IF NOT EXISTS idx_company_court_plaintiff_inn ON company_court_cases(plaintiff_inn);
CREATE INDEX IF NOT EXISTS idx_company_court_defendant_inn ON company_court_cases(defendant_inn);
CREATE INDEX IF NOT EXISTS idx_company_court_filing_date ON company_court_cases(filing_date);
CREATE INDEX IF NOT EXISTS idx_company_court_raw_data ON company_court_cases USING GIN (raw_data);

COMMENT ON TABLE company_court_cases IS 'Legal cases involving companies from company API - endpoint: /api/court/inn/{inn}';

-- ============================================================================
-- 3. company CONNECTIONS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS company_connections (
    id SERIAL PRIMARY KEY,

    -- Reference fields
    inn VARCHAR(20) NOT NULL,
    company_id INTEGER NOT NULL,

    -- Connection identification
    connection_id VARCHAR(100),
    connection_type VARCHAR(100),  -- 'founder', 'director', 'related_company', etc.

    -- Connected entity information
    connected_inn VARCHAR(20),
    connected_name VARCHAR(500),
    connected_company_id INTEGER,
    connected_uuid VARCHAR(100),

    -- Connection details
    relationship VARCHAR(200),
    ownership_percentage DECIMAL(5,2),
    position VARCHAR(200),
    is_active BOOLEAN DEFAULT TRUE,

    -- Dates
    start_date DATE,
    end_date DATE,

    -- Complete raw data from API
    raw_data JSONB NOT NULL,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate connections
    UNIQUE(inn, company_id, connection_id)
);

CREATE INDEX IF NOT EXISTS idx_company_connections_inn ON company_connections(inn);
CREATE INDEX IF NOT EXISTS idx_company_connections_company_id ON company_connections(company_id);
CREATE INDEX IF NOT EXISTS idx_company_connections_type ON company_connections(connection_type);
CREATE INDEX IF NOT EXISTS idx_company_connections_connected_inn ON company_connections(connected_inn);
CREATE INDEX IF NOT EXISTS idx_company_connections_is_active ON company_connections(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_company_connections_raw_data ON company_connections USING GIN (raw_data);

COMMENT ON TABLE company_connections IS 'Company connections and relationships from company API - endpoint: /api/entity/{company_id}/connection';

-- ============================================================================
-- 4. company LIABILITIES TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS company_liabilities (
    id SERIAL PRIMARY KEY,

    -- Reference fields
    inn VARCHAR(20) NOT NULL,
    company_id INTEGER NOT NULL,

    -- Liability identification
    liability_id VARCHAR(100),
    liability_type VARCHAR(100),
    liability_category VARCHAR(200),

    -- Creditor information
    creditor_name VARCHAR(500),
    creditor_inn VARCHAR(20),

    -- Financial information
    liability_amount DECIMAL(15,2),
    outstanding_amount DECIMAL(15,2),
    currency VARCHAR(10),

    -- Status and dates
    liability_status VARCHAR(100),
    start_date DATE,
    due_date DATE,
    last_payment_date DATE,

    -- Additional details
    collateral_description TEXT,
    interest_rate DECIMAL(5,2),

    -- Complete raw data from API
    raw_data JSONB NOT NULL,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate liabilities
    UNIQUE(inn, company_id, liability_id)
);

CREATE INDEX IF NOT EXISTS idx_company_liabilities_inn ON company_liabilities(inn);
CREATE INDEX IF NOT EXISTS idx_company_liabilities_company_id ON company_liabilities(company_id);
CREATE INDEX IF NOT EXISTS idx_company_liabilities_type ON company_liabilities(liability_type);
CREATE INDEX IF NOT EXISTS idx_company_liabilities_status ON company_liabilities(liability_status);
CREATE INDEX IF NOT EXISTS idx_company_liabilities_creditor_inn ON company_liabilities(creditor_inn);
CREATE INDEX IF NOT EXISTS idx_company_liabilities_due_date ON company_liabilities(due_date);
CREATE INDEX IF NOT EXISTS idx_company_liabilities_raw_data ON company_liabilities USING GIN (raw_data);

COMMENT ON TABLE company_liabilities IS 'Company financial liabilities from company API - endpoint: /api/entity/liability/{company_id}';

-- ============================================================================
-- 5. company LICENSES TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS company_licenses (
    id SERIAL PRIMARY KEY,

    -- Reference fields
    inn VARCHAR(20) NOT NULL,
    company_id INTEGER NOT NULL,

    -- License identification
    license_id VARCHAR(100),
    license_number VARCHAR(200),
    license_type VARCHAR(200),
    license_category VARCHAR(200),

    -- Issuing authority
    issuing_authority VARCHAR(500),
    issuing_region VARCHAR(200),

    -- License details
    license_name TEXT,
    license_description TEXT,
    activity_type TEXT,

    -- Status and dates
    license_status VARCHAR(100),
    issue_date DATE,
    expiry_date DATE,
    last_renewal_date DATE,

    -- Additional information
    is_active BOOLEAN DEFAULT TRUE,
    scope_of_license TEXT,

    -- Complete raw data from API
    raw_data JSONB NOT NULL,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate licenses
    UNIQUE(inn, company_id, license_number, license_id)
);

CREATE INDEX IF NOT EXISTS idx_company_licenses_inn ON company_licenses(inn);
CREATE INDEX IF NOT EXISTS idx_company_licenses_company_id ON company_licenses(company_id);
CREATE INDEX IF NOT EXISTS idx_company_licenses_type ON company_licenses(license_type);
CREATE INDEX IF NOT EXISTS idx_company_licenses_status ON company_licenses(license_status);
CREATE INDEX IF NOT EXISTS idx_company_licenses_is_active ON company_licenses(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_company_licenses_expiry_date ON company_licenses(expiry_date);
CREATE INDEX IF NOT EXISTS idx_company_licenses_raw_data ON company_licenses USING GIN (raw_data);

COMMENT ON TABLE company_licenses IS 'Business licenses and permits from company API - endpoint: /api/entity/license/{company_id}';

-- ============================================================================
-- 6. company FOUNDER CONNECTIONS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS company_founder_connections (
    id SERIAL PRIMARY KEY,

    -- Reference fields
    inn VARCHAR(20) NOT NULL,
    director_uuid VARCHAR(100) NOT NULL,

    -- Founder/Director information
    founder_name VARCHAR(500),
    founder_inn VARCHAR(20),
    founder_type VARCHAR(100),  -- 'individual' or 'legal_entity'

    -- Connected company information
    connected_company_inn VARCHAR(20),
    connected_company_name VARCHAR(500),
    connected_company_id INTEGER,
    connected_company_uuid VARCHAR(100),

    -- Connection details
    position VARCHAR(200),
    ownership_percentage DECIMAL(5,2),
    is_current BOOLEAN DEFAULT TRUE,

    -- Dates
    appointment_date DATE,
    termination_date DATE,

    -- Complete raw data from API
    raw_data JSONB NOT NULL,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate connections
    UNIQUE(inn, director_uuid, connected_company_inn)
);

CREATE INDEX IF NOT EXISTS idx_company_founder_conn_inn ON company_founder_connections(inn);
CREATE INDEX IF NOT EXISTS idx_company_founder_conn_director_uuid ON company_founder_connections(director_uuid);
CREATE INDEX IF NOT EXISTS idx_company_founder_conn_founder_inn ON company_founder_connections(founder_inn);
CREATE INDEX IF NOT EXISTS idx_company_founder_conn_company_inn ON company_founder_connections(connected_company_inn);
CREATE INDEX IF NOT EXISTS idx_company_founder_conn_is_current ON company_founder_connections(is_current) WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_company_founder_conn_raw_data ON company_founder_connections USING GIN (raw_data);

COMMENT ON TABLE company_founder_connections IS 'Founder/director connections to other companies from company API - endpoint: /api/entity/connection/founder/{director_uuid}';

-- ============================================================================
-- 7. company RATINGS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS company_ratings (
    id SERIAL PRIMARY KEY,

    -- Reference fields
    inn VARCHAR(20) NOT NULL,
    company_id INTEGER NOT NULL,

    -- Rating identification
    rating_id VARCHAR(100),
    rating_type VARCHAR(100),
    rating_agency VARCHAR(200),

    -- Rating details
    rating_score DECIMAL(5,2),
    rating_grade VARCHAR(50),
    rating_category VARCHAR(100),
    credit_limit DECIMAL(15,2),

    -- Risk assessment
    risk_level VARCHAR(50),
    risk_category VARCHAR(100),
    default_probability DECIMAL(5,2),

    -- Financial metrics
    liquidity_ratio DECIMAL(10,4),
    solvency_ratio DECIMAL(10,4),
    profitability_ratio DECIMAL(10,4),

    -- Rating validity
    rating_date DATE,
    valid_from DATE,
    valid_until DATE,
    is_current BOOLEAN DEFAULT TRUE,

    -- Additional information
    rating_outlook VARCHAR(50),  -- 'positive', 'stable', 'negative'
    rating_notes TEXT,

    -- Complete raw data from API
    raw_data JSONB NOT NULL,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate ratings
    UNIQUE(inn, company_id, rating_id, rating_date)
);

CREATE INDEX IF NOT EXISTS idx_company_ratings_inn ON company_ratings(inn);
CREATE INDEX IF NOT EXISTS idx_company_ratings_company_id ON company_ratings(company_id);
CREATE INDEX IF NOT EXISTS idx_company_ratings_type ON company_ratings(rating_type);
CREATE INDEX IF NOT EXISTS idx_company_ratings_grade ON company_ratings(rating_grade);
CREATE INDEX IF NOT EXISTS idx_company_ratings_risk_level ON company_ratings(risk_level);
CREATE INDEX IF NOT EXISTS idx_company_ratings_is_current ON company_ratings(is_current) WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_company_ratings_date ON company_ratings(rating_date);
CREATE INDEX IF NOT EXISTS idx_company_ratings_raw_data ON company_ratings USING GIN (raw_data);

COMMENT ON TABLE company_ratings IS 'Company credit ratings and scores from company API - endpoint: /api/entity/rating/{company_id}';

-- ============================================================================
-- 8. company COLLATERALS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS company_collaterals (
    id SERIAL PRIMARY KEY,

    -- Reference fields
    inn VARCHAR(20) NOT NULL,

    -- Collateral identification
    collateral_id VARCHAR(100),
    collateral_number VARCHAR(200),
    collateral_type VARCHAR(200),
    collateral_category VARCHAR(200),

    -- Asset information
    asset_description TEXT,
    asset_type VARCHAR(200),
    asset_location TEXT,

    -- Financial information
    collateral_value DECIMAL(15,2),
    assessed_value DECIMAL(15,2),
    currency VARCHAR(10),

    -- Pledge details
    pledgee_name VARCHAR(500),
    pledgee_inn VARCHAR(20),
    pledgor_name VARCHAR(500),
    pledgor_inn VARCHAR(20),

    -- Status and dates
    collateral_status VARCHAR(100),
    registration_date DATE,
    expiry_date DATE,
    release_date DATE,
    is_active BOOLEAN DEFAULT TRUE,

    -- Registration details
    registration_number VARCHAR(200),
    registration_authority VARCHAR(500),

    -- Additional information
    secured_obligation TEXT,
    priority_rank INTEGER,

    -- Complete raw data from API
    raw_data JSONB NOT NULL,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate collaterals
    UNIQUE(inn, collateral_number, collateral_id)
);

CREATE INDEX IF NOT EXISTS idx_company_collaterals_inn ON company_collaterals(inn);
CREATE INDEX IF NOT EXISTS idx_company_collaterals_type ON company_collaterals(collateral_type);
CREATE INDEX IF NOT EXISTS idx_company_collaterals_status ON company_collaterals(collateral_status);
CREATE INDEX IF NOT EXISTS idx_company_collaterals_pledgee_inn ON company_collaterals(pledgee_inn);
CREATE INDEX IF NOT EXISTS idx_company_collaterals_pledgor_inn ON company_collaterals(pledgor_inn);
CREATE INDEX IF NOT EXISTS idx_company_collaterals_is_active ON company_collaterals(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_company_collaterals_reg_date ON company_collaterals(registration_date);
CREATE INDEX IF NOT EXISTS idx_company_collaterals_raw_data ON company_collaterals USING GIN (raw_data);

COMMENT ON TABLE company_collaterals IS 'Collateral and pledge information from company API - endpoint: /api/collateral/{inn}';

-- ============================================================================
-- TRIGGERS FOR UPDATED_AT TIMESTAMPS
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

-- Create triggers for all new tables
DROP TRIGGER IF EXISTS update_company_deals_updated_at ON company_deals;
CREATE TRIGGER update_company_deals_updated_at
    BEFORE UPDATE ON company_deals
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_company_court_cases_updated_at ON company_court_cases;
CREATE TRIGGER update_company_court_cases_updated_at
    BEFORE UPDATE ON company_court_cases
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_company_connections_updated_at ON company_connections;
CREATE TRIGGER update_company_connections_updated_at
    BEFORE UPDATE ON company_connections
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_company_liabilities_updated_at ON company_liabilities;
CREATE TRIGGER update_company_liabilities_updated_at
    BEFORE UPDATE ON company_liabilities
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_company_licenses_updated_at ON company_licenses;
CREATE TRIGGER update_company_licenses_updated_at
    BEFORE UPDATE ON company_licenses
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_company_founder_connections_updated_at ON company_founder_connections;
CREATE TRIGGER update_company_founder_connections_updated_at
    BEFORE UPDATE ON company_founder_connections
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_company_ratings_updated_at ON company_ratings;
CREATE TRIGGER update_company_ratings_updated_at
    BEFORE UPDATE ON company_ratings
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_company_collaterals_updated_at ON company_collaterals;
CREATE TRIGGER update_company_collaterals_updated_at
    BEFORE UPDATE ON company_collaterals
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE 'company additional tables migration completed successfully!';
    RAISE NOTICE 'Created 8 new tables:';
    RAISE NOTICE '  - company_deals';
    RAISE NOTICE '  - company_court_cases';
    RAISE NOTICE '  - company_connections';
    RAISE NOTICE '  - company_liabilities';
    RAISE NOTICE '  - company_licenses';
    RAISE NOTICE '  - company_founder_connections';
    RAISE NOTICE '  - company_ratings';
    RAISE NOTICE '  - company_collaterals';
END $$;
