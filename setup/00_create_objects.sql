-- One-time setup for spec-driven-pipelines on Databricks (Free Edition).
-- Run in the SQL editor, or: databricks sql ... / a notebook %sql cell.
CREATE CATALOG IF NOT EXISTS spec_driven;
CREATE SCHEMA  IF NOT EXISTS spec_driven.bronze;
CREATE SCHEMA  IF NOT EXISTS spec_driven.silver;
CREATE SCHEMA  IF NOT EXISTS spec_driven.landing;
CREATE VOLUME  IF NOT EXISTS spec_driven.landing.raw;   -- /Volumes/spec_driven/landing/raw/
