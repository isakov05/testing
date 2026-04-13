-- ============================================================================
-- FULL DATABASE MIGRATION - FLOTT FINANCIAL ANALYTICS
-- ============================================================================
-- This file consolidates ALL schema changes in the correct order
-- Run this on a fresh database to set up everything
-- Date: 2025-11-13
-- ============================================================================

-- ============================================================================
-- STEP 1: Core Schema (Users, Invoices, Bank Transactions, etc.)
-- ============================================================================

-- USERS TABLE (Authentication)
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

COMMENT ON TABLE users IS 'User authentication and profile information';

-- INVOICES TABLE (both incoming and outgoing)
CREATE TABLE IF NOT EXISTS invoices (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,

    -- Core invoice fields
    document_number VARCHAR(100),
    document_date DATE,
    invoice_type VARCHAR(10) NOT NULL CHECK (invoice_type IN ('IN', 'OUT')),

    -- Parties involved
    seller_inn VARCHAR(20),
    seller_name VARCHAR(500),
    seller_branch_code VARCHAR(50),
    seller_branch_name VARCHAR(500),
    buyer_inn VARCHAR(20),
    buyer_name VARCHAR(500),
    buyer_branch_code VARCHAR(50),
    buyer_branch_name VARCHAR(500),

    -- Financial information
    supply_value DECIMAL(15,2),
    vat_amount DECIMAL(15,2),
    total_amount DECIMAL(15,2),

    -- Additional fields
    status VARCHAR(100),
    contract_number VARCHAR(100),
    contract_date DATE,
    document_type VARCHAR(100),
    document_kind VARCHAR(100),
    send_date DATE,
    note TEXT,

    -- File tracking
    source_filename VARCHAR(255),
    upload_batch_id UUID,

    -- Metadata
    uploaded_by VARCHAR(100),
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_invoices_user_id ON invoices(user_id);
CREATE INDEX IF NOT EXISTS idx_invoices_type ON invoices(invoice_type);
CREATE INDEX IF NOT EXISTS idx_invoices_date ON invoices(document_date);
CREATE INDEX IF NOT EXISTS idx_invoices_seller_inn ON invoices(seller_inn);
CREATE INDEX IF NOT EXISTS idx_invoices_buyer_inn ON invoices(buyer_inn);
CREATE INDEX IF NOT EXISTS idx_invoices_document_number ON invoices(document_number);
CREATE INDEX IF NOT EXISTS idx_invoices_upload_batch ON invoices(upload_batch_id);

COMMENT ON TABLE invoices IS 'Stores both incoming and outgoing invoices with full details';
COMMENT ON COLUMN invoices.invoice_type IS 'IN for incoming invoices (purchases), OUT for outgoing invoices (sales)';

-- INVOICE ITEMS TABLE (product-level details)
CREATE TABLE IF NOT EXISTS invoice_items (
    id SERIAL PRIMARY KEY,
    invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
    user_id VARCHAR(100) NOT NULL,

    -- Item identification
    item_number INTEGER,
    item_note TEXT,
    catalog_code TEXT,

    -- Quantity and pricing
    quantity DECIMAL(15,3),
    unit_of_measure VARCHAR(50),
    unit_price DECIMAL(15,2),

    -- Financial calculations
    supply_value DECIMAL(15,2),
    excise_rate DECIMAL(5,2),
    excise_amount DECIMAL(15,2),
    vat_rate DECIMAL(5,2),
    vat_amount DECIMAL(15,2),
    total_amount DECIMAL(15,2),

    -- Additional fields
    marking VARCHAR(100),
    origin_country VARCHAR(100),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_invoice_items_invoice_id ON invoice_items(invoice_id);
CREATE INDEX IF NOT EXISTS idx_invoice_items_user_id ON invoice_items(user_id);

COMMENT ON TABLE invoice_items IS 'Product-level line items for each invoice';

-- BANK TRANSACTIONS TABLE
CREATE TABLE IF NOT EXISTS bank_transactions (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,

    -- Transaction details
    transaction_date DATE,
    document_number VARCHAR(100),
    document_date DATE,
    processing_date DATE,

    -- Counterparty information
    counterparty_inn VARCHAR(20),
    counterparty_name VARCHAR(500),
    counterparty_account VARCHAR(50),
    counterparty_bank_code VARCHAR(20),

    -- Financial information
    amount DECIMAL(15,2),
    debit_amount DECIMAL(15,2),
    credit_amount DECIMAL(15,2),
    transaction_type VARCHAR(20) CHECK (transaction_type IN ('Incoming', 'Outgoing', 'Unknown')),

    -- Transaction details
    payment_purpose TEXT,
    contract_number VARCHAR(100),

    -- File tracking
    source_filename VARCHAR(255),
    upload_batch_id UUID,

    -- Metadata
    uploaded_by VARCHAR(100),
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bank_trans_user_id ON bank_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_bank_trans_date ON bank_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_bank_trans_inn ON bank_transactions(counterparty_inn);
CREATE INDEX IF NOT EXISTS idx_bank_trans_type ON bank_transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_bank_trans_upload_batch ON bank_transactions(upload_batch_id);

COMMENT ON TABLE bank_transactions IS 'Bank statement transactions';
COMMENT ON COLUMN bank_transactions.transaction_type IS 'Incoming (credit/receipt), Outgoing (debit/payment), or Unknown';

-- RECONCILIATION RECORDS TABLE (AR and AP)
CREATE TABLE IF NOT EXISTS reconciliation_records (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,

    -- Counterparty information
    counterparty_inn VARCHAR(20),
    counterparty_name VARCHAR(500),

    -- Outstanding amount
    outstanding_amount DECIMAL(15,2),

    -- Record type
    record_type VARCHAR(10) NOT NULL CHECK (record_type IN ('IN', 'OUT')),

    -- Report information
    report_date DATE,

    -- File tracking
    source_filename VARCHAR(255),
    upload_batch_id UUID,

    -- Metadata
    uploaded_by VARCHAR(100),
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_recon_user_id ON reconciliation_records(user_id);
CREATE INDEX IF NOT EXISTS idx_recon_type ON reconciliation_records(record_type);
CREATE INDEX IF NOT EXISTS idx_recon_inn ON reconciliation_records(counterparty_inn);
CREATE INDEX IF NOT EXISTS idx_recon_upload_batch ON reconciliation_records(upload_batch_id);

COMMENT ON TABLE reconciliation_records IS 'Accounts receivable and payable reconciliation data';
COMMENT ON COLUMN reconciliation_records.record_type IS 'IN for Accounts Receivable (customers owe us), OUT for Accounts Payable (we owe suppliers)';

-- UPLOAD BATCHES TABLE (track file uploads)
CREATE TABLE IF NOT EXISTS upload_batches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(100) NOT NULL,

    -- File information
    filename VARCHAR(255) NOT NULL,
    file_size BIGINT,
    data_type VARCHAR(50) NOT NULL CHECK (data_type IN ('invoice_in', 'invoice_out', 'bank_statement', 'reconciliation_in', 'reconciliation_out')),

    -- Processing information
    records_count INTEGER DEFAULT 0,
    duplicates_skipped INTEGER DEFAULT 0,
    errors_count INTEGER DEFAULT 0,
    processing_status VARCHAR(20) DEFAULT 'processing' CHECK (processing_status IN ('processing', 'completed', 'failed')),
    error_message TEXT,

    -- Metadata
    uploaded_by VARCHAR(100),
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_upload_batches_user_id ON upload_batches(user_id);
CREATE INDEX IF NOT EXISTS idx_upload_batches_data_type ON upload_batches(data_type);
CREATE INDEX IF NOT EXISTS idx_upload_batches_upload_date ON upload_batches(upload_date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_upload_batches_unique_file ON upload_batches(user_id, filename, data_type);

COMMENT ON TABLE upload_batches IS 'Tracks file upload history and processing status - prevents duplicate file uploads';

-- ============================================================================
-- STEP 2: Functions and Triggers
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers for updated_at
DROP TRIGGER IF EXISTS update_invoices_updated_at ON invoices;
CREATE TRIGGER update_invoices_updated_at
    BEFORE UPDATE ON invoices
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_bank_transactions_updated_at ON bank_transactions;
CREATE TRIGGER update_bank_transactions_updated_at
    BEFORE UPDATE ON bank_transactions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- STEP 3: Add company_inn to users table (Migration 03)
-- ============================================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name='users' AND column_name='company_inn'
    ) THEN
        ALTER TABLE users ADD COLUMN company_inn VARCHAR(20);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_users_company_inn ON users(company_inn);
COMMENT ON COLUMN users.company_inn IS 'Company Tax Identification Number (INN) for MyOrg integration';

-- ============================================================================
-- STEP 4: Company Info Table (Migration 02)
-- ============================================================================

CREATE TABLE IF NOT EXISTS company_info (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    -- Basic Company Information
    inn VARCHAR(20) UNIQUE NOT NULL,
    company_name VARCHAR(500) NOT NULL,
    status VARCHAR(100),
    status_description VARCHAR(500),

    -- Registration Details
    registration_number VARCHAR(100),
    registration_date DATE,
    registration_center VARCHAR(500),

    -- Financial and Legal Info
    statutory_fund BIGINT,
    director_name VARCHAR(500),
    is_small_business BOOLEAN DEFAULT FALSE,
    enterprise_category VARCHAR(200),
    taxation_type VARCHAR(200),

    -- Statistical Codes
    oked_code VARCHAR(50),
    oked_description TEXT,
    opf_code VARCHAR(50),
    opf_description TEXT,
    soogu_code VARCHAR(50),
    soogu_description TEXT,
    soato_code VARCHAR(50),
    soato_description TEXT,

    -- Contact Information
    region VARCHAR(200),
    city VARCHAR(200),
    street_address TEXT,
    email VARCHAR(200),
    phone VARCHAR(100),

    -- Verification Status
    is_verified BOOLEAN DEFAULT FALSE,

    -- Metadata
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Ensure one company per user
    UNIQUE(user_id)
);

CREATE INDEX IF NOT EXISTS idx_company_info_user_id ON company_info(user_id);
CREATE INDEX IF NOT EXISTS idx_company_info_inn ON company_info(inn);
CREATE INDEX IF NOT EXISTS idx_company_info_status ON company_info(status);

COMMENT ON TABLE company_info IS 'Stores company information from MyOrg API integration';

-- ============================================================================
-- STEP 5: Add JSONB and additional fields to company_info (Migration 04 & 05)
-- ============================================================================

DO $$
BEGIN
    -- Add raw_data JSONB column
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='company_info' AND column_name='raw_data'
    ) THEN
        ALTER TABLE company_info ADD COLUMN raw_data JSONB;
    END IF;

    -- UUID from MyOrg
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='company_info' AND column_name='uuid'
    ) THEN
        ALTER TABLE company_info ADD COLUMN uuid VARCHAR(100);
    END IF;

    -- Company ID from MyOrg core
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='company_info' AND column_name='company_id'
    ) THEN
        ALTER TABLE company_info ADD COLUMN company_id INTEGER;
    END IF;

    -- Additional fields
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='company_info' AND column_name='trust') THEN
        ALTER TABLE company_info ADD COLUMN trust INTEGER;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='company_info' AND column_name='score') THEN
        ALTER TABLE company_info ADD COLUMN score INTEGER;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='company_info' AND column_name='itpark') THEN
        ALTER TABLE company_info ADD COLUMN itpark BOOLEAN DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='company_info' AND column_name='is_bankrupt') THEN
        ALTER TABLE company_info ADD COLUMN is_bankrupt BOOLEAN DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='company_info' AND column_name='is_abuse_vat') THEN
        ALTER TABLE company_info ADD COLUMN is_abuse_vat BOOLEAN DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='company_info' AND column_name='is_large_taxpayer') THEN
        ALTER TABLE company_info ADD COLUMN is_large_taxpayer BOOLEAN DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='company_info' AND column_name='village_code') THEN
        ALTER TABLE company_info ADD COLUMN village_code VARCHAR(50);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='company_info' AND column_name='village_name') THEN
        ALTER TABLE company_info ADD COLUMN village_name VARCHAR(200);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='company_info' AND column_name='relevance_date') THEN
        ALTER TABLE company_info ADD COLUMN relevance_date DATE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='company_info' AND column_name='activity_state') THEN
        ALTER TABLE company_info ADD COLUMN activity_state INTEGER;
    END IF;
END $$;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_company_info_raw_data ON company_info USING GIN (raw_data);
CREATE INDEX IF NOT EXISTS idx_company_info_uuid ON company_info(uuid);
CREATE INDEX IF NOT EXISTS idx_company_info_company_id ON company_info(company_id);
CREATE INDEX IF NOT EXISTS idx_company_info_itpark ON company_info(itpark) WHERE itpark = TRUE;
CREATE INDEX IF NOT EXISTS idx_company_info_is_large_taxpayer ON company_info(is_large_taxpayer) WHERE is_large_taxpayer = TRUE;
CREATE INDEX IF NOT EXISTS idx_company_info_is_bankrupt ON company_info(is_bankrupt) WHERE is_bankrupt = TRUE;

-- Trigger for company_info
CREATE OR REPLACE FUNCTION update_company_info_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_company_info_timestamp ON company_info;
CREATE TRIGGER trigger_update_company_info_timestamp
    BEFORE UPDATE ON company_info
    FOR EACH ROW
    EXECUTE FUNCTION update_company_info_timestamp();

-- Helper functions
CREATE OR REPLACE FUNCTION get_company_founders(company_uuid VARCHAR)
RETURNS TABLE (
    name TEXT,
    percentage NUMERIC,
    is_individual BOOLEAN,
    person_type TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        (founder->>'name')::TEXT as name,
        (founder->>'percentage')::NUMERIC as percentage,
        CASE WHEN (founder->>'is_individual')::INT = 1 THEN TRUE ELSE FALSE END as is_individual,
        (founder->>'person_type')::TEXT as person_type
    FROM company_info,
         jsonb_array_elements(raw_data->'founders') AS founder
    WHERE uuid = company_uuid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_business_metrics(company_uuid VARCHAR)
RETURNS TABLE (
    total_deals INTEGER,
    customer_deals INTEGER,
    provider_deals INTEGER,
    total_licenses INTEGER,
    total_buildings INTEGER,
    total_cadastres INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COALESCE((raw_data->'deals'->'customer'->>'total')::INTEGER, 0) +
        COALESCE((raw_data->'deals'->'provider'->>'total')::INTEGER, 0) as total_deals,
        COALESCE((raw_data->'deals'->'customer'->>'total')::INTEGER, 0) as customer_deals,
        COALESCE((raw_data->'deals'->'provider'->>'total')::INTEGER, 0) as provider_deals,
        COALESCE((raw_data->'licenses'->>'total')::INTEGER, 0) as total_licenses,
        COALESCE((raw_data->'buildings'->>'total')::INTEGER, 0) as total_buildings,
        COALESCE((raw_data->'cadastres'->>'total')::INTEGER, 0) as total_cadastres
    FROM company_info
    WHERE uuid = company_uuid;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- STEP 6: Viewed Companies Cache Table (Migration 06)
-- ============================================================================

CREATE TABLE IF NOT EXISTS viewed_companies (
    id SERIAL PRIMARY KEY,

    -- Basic Company Information
    inn VARCHAR(20) UNIQUE NOT NULL,
    company_name VARCHAR(500) NOT NULL,
    uuid VARCHAR(100),
    company_id INTEGER,
    status VARCHAR(100),
    status_description VARCHAR(500),
    activity_state INTEGER,

    -- Registration Details
    registration_number VARCHAR(100),
    registration_date DATE,
    registration_center VARCHAR(500),

    -- Financial and Legal Info
    statutory_fund BIGINT,
    director_name VARCHAR(500),
    is_small_business BOOLEAN DEFAULT FALSE,
    enterprise_category VARCHAR(200),
    taxation_type VARCHAR(200),

    -- Statistical Codes
    oked_code VARCHAR(50),
    oked_description TEXT,
    opf_code VARCHAR(50),
    opf_description TEXT,
    soogu_code VARCHAR(50),
    soogu_description TEXT,
    soato_code VARCHAR(50),
    soato_description TEXT,

    -- Contact Information
    region VARCHAR(200),
    city VARCHAR(200),
    street_address TEXT,
    email VARCHAR(200),
    phone VARCHAR(100),

    -- Village/MFY
    village_code VARCHAR(50),
    village_name VARCHAR(200),

    -- Additional MyOrg fields
    trust INTEGER,
    score INTEGER,
    itpark BOOLEAN DEFAULT FALSE,
    is_bankrupt BOOLEAN DEFAULT FALSE,
    is_abuse_vat BOOLEAN DEFAULT FALSE,
    is_large_taxpayer BOOLEAN DEFAULT FALSE,
    relevance_date DATE,

    -- Verification Status
    is_verified BOOLEAN DEFAULT FALSE,

    -- Extended data from all 6 APIs stored as JSONB
    raw_data JSONB,

    -- Cache metadata
    last_fetched TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fetch_count INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_viewed_companies_inn ON viewed_companies(inn);
CREATE INDEX IF NOT EXISTS idx_viewed_companies_last_fetched ON viewed_companies(last_fetched);
CREATE INDEX IF NOT EXISTS idx_viewed_companies_company_id ON viewed_companies(company_id);

-- Trigger for viewed_companies
CREATE OR REPLACE FUNCTION update_viewed_company_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_fetched = CURRENT_TIMESTAMP;
    NEW.fetch_count = OLD.fetch_count + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_viewed_company_timestamp ON viewed_companies;
CREATE TRIGGER trigger_update_viewed_company_timestamp
    BEFORE UPDATE ON viewed_companies
    FOR EACH ROW
    EXECUTE FUNCTION update_viewed_company_timestamp();

COMMENT ON TABLE viewed_companies IS 'Cache for companies viewed via "View Details" feature - shared across all users';

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

-- Verify all tables exist
DO $$
DECLARE
    table_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE';

    RAISE NOTICE 'Migration completed successfully!';
    RAISE NOTICE 'Total tables created: %', table_count;
END $$;
