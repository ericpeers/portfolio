-- Organizations (structure ready; no members required yet)
CREATE TABLE IF NOT EXISTS dim_organization (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(256) NOT NULL,
    created_at TIMESTAMPTZ  DEFAULT NOW()
);

-- Role enum — constrained set, consistent with other enum types in the schema
-- USER     — standard account; can manage their own portfolios only
-- ORG_ADMIN — organization administrator; can move/reassign portfolios among members of their org
-- ADMIN     — system administrator; full access, approves new accounts
CREATE TYPE user_role AS ENUM ('USER', 'ORG_ADMIN', 'ADMIN');

-- Extend dim_user with auth columns
ALTER TABLE dim_user
    ADD COLUMN IF NOT EXISTS organization_id BIGINT      REFERENCES dim_organization(id),
    ADD COLUMN IF NOT EXISTS passwd          VARCHAR(256),
    ADD COLUMN IF NOT EXISTS is_approved     BOOLEAN     NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS updated_at      TIMESTAMPTZ DEFAULT NOW();

-- Migrate role from varchar(50) to user_role enum.
-- The default must be dropped first because PostgreSQL cannot automatically
-- cast a string literal default to the new enum type.
ALTER TABLE dim_user ALTER COLUMN role DROP DEFAULT;
ALTER TABLE dim_user ALTER COLUMN role TYPE user_role USING role::user_role;
ALTER TABLE dim_user ALTER COLUMN role SET DEFAULT 'USER';

-- Email uniqueness (create_tables.sql has no constraint)
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'dim_user_email_unique' AND table_name = 'dim_user'
    ) THEN
        ALTER TABLE dim_user ADD CONSTRAINT dim_user_email_unique UNIQUE (email);
    END IF;
END $$;

-- Bootstrap: existing user (id=1) becomes approved ADMIN
UPDATE dim_user SET is_approved = TRUE, role = 'ADMIN' WHERE id = 1;
