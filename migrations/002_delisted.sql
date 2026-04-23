ALTER TABLE dim_security ADD COLUMN IF NOT EXISTS delisted boolean NOT NULL DEFAULT false;
