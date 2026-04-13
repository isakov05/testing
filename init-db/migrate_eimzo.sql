-- Database migration script for E-IMZO authentication
-- This script adds E-IMZO related columns to the users table

-- Add E-IMZO columns to users table
ALTER TABLE users
ADD COLUMN IF NOT EXISTS inn VARCHAR(20),
ADD COLUMN IF NOT EXISTS pinfl VARCHAR(20),
ADD COLUMN IF NOT EXISTS eimzo_certificate_serial VARCHAR(100),
ADD COLUMN IF NOT EXISTS eimzo_cn VARCHAR(255),
ADD COLUMN IF NOT EXISTS auth_method VARCHAR(20) DEFAULT 'password';

-- Make password nullable (for E-IMZO-only users)
ALTER TABLE users ALTER COLUMN password DROP NOT NULL;

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_users_inn ON users(inn);
CREATE INDEX IF NOT EXISTS idx_users_pinfl ON users(pinfl);
CREATE INDEX IF NOT EXISTS idx_users_eimzo_serial ON users(eimzo_certificate_serial);
CREATE INDEX IF NOT EXISTS idx_users_auth_method ON users(auth_method);

-- Update existing users to set auth_method to 'password'
UPDATE users
SET auth_method = 'password'
WHERE auth_method IS NULL AND password IS NOT NULL;

-- Update existing users without password to set auth_method to NULL
UPDATE users
SET auth_method = NULL
WHERE auth_method IS NULL AND password IS NULL;

-- Add comments to columns
COMMENT ON COLUMN users.inn IS 'Tax Identification Number from E-IMZO certificate';
COMMENT ON COLUMN users.pinfl IS 'Personal Identification Number from E-IMZO certificate';
COMMENT ON COLUMN users.eimzo_certificate_serial IS 'E-IMZO certificate serial number';
COMMENT ON COLUMN users.eimzo_cn IS 'Common Name from E-IMZO certificate';
COMMENT ON COLUMN users.auth_method IS 'Authentication method: password, eimzo, or both';
