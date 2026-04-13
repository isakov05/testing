-- ============================================================================
-- UNIVERSAL COMPANY INFO MIGRATION
-- ============================================================================
-- This migration converts company_info to a universal company database
-- by removing user associations and consolidating with viewed_companies.
-- Date: 2025-11-20
-- ============================================================================
--
-- Changes:
--   1. Drop user_id column from company_info (removes foreign key and UNIQUE constraint)
--   2. Drop viewed_companies table (no longer needed)
--   3. Result: company_info becomes universal company database indexed by INN
--
-- ============================================================================

-- ============================================================================
-- STEP 1: Remove user_id from company_info
-- ============================================================================

DO $$
BEGIN
    -- Check if user_id column exists before dropping
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name='company_info' AND column_name='user_id'
    ) THEN
        -- Drop the user_id column
        -- This automatically drops the foreign key constraint and UNIQUE(user_id) constraint
        ALTER TABLE company_info DROP COLUMN user_id;
        RAISE NOTICE 'Dropped user_id column from company_info table';
    ELSE
        RAISE NOTICE 'user_id column does not exist in company_info table - skipping';
    END IF;
END $$;

-- ============================================================================
-- STEP 2: Drop viewed_companies table
-- ============================================================================

DROP TABLE IF EXISTS viewed_companies CASCADE;

COMMENT ON TABLE company_info IS 'Universal company information database from MyOrg API - indexed by INN. Any user can query company data using INN.';

-- ============================================================================
-- STEP 3: Verify indexes for INN-based lookups
-- ============================================================================

-- Ensure INN index exists and is unique (primary lookup key)
DROP INDEX IF EXISTS idx_company_info_inn;
CREATE UNIQUE INDEX idx_company_info_inn ON company_info(inn);

-- Ensure other important indexes exist
CREATE INDEX IF NOT EXISTS idx_company_info_company_id ON company_info(company_id);
CREATE INDEX IF NOT EXISTS idx_company_info_uuid ON company_info(uuid);
CREATE INDEX IF NOT EXISTS idx_company_info_status ON company_info(status);

-- GIN index for JSONB raw_data
CREATE INDEX IF NOT EXISTS idx_company_info_raw_data ON company_info USING GIN (raw_data);

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

DO $$
DECLARE
    company_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO company_count FROM company_info;

    RAISE NOTICE '====================================================================';
    RAISE NOTICE 'Universal company_info migration completed successfully!';
    RAISE NOTICE '====================================================================';
    RAISE NOTICE 'Changes made:';
    RAISE NOTICE '  - Removed user_id column from company_info';
    RAISE NOTICE '  - Dropped viewed_companies table';
    RAISE NOTICE '  - company_info is now a universal company database';
    RAISE NOTICE '';
    RAISE NOTICE 'Current company_info stats:';
    RAISE NOTICE '  - Total companies: %', company_count;
    RAISE NOTICE '  - Primary lookup: INN (unique index)';
    RAISE NOTICE '====================================================================';
END $$;
