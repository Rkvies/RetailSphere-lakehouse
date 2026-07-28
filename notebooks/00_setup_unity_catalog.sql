-- Run once, in a SQL notebook cell or the SQL editor, before the first
-- pipeline run. Community Edition UC workspaces typically pre-provision
-- a default catalog (often named after your workspace or "workspace") -
-- adjust CATALOG_NAME below to whatever `SHOW CATALOGS` returns for you.

SHOW CATALOGS;

CREATE CATALOG IF NOT EXISTS retail_lakehouse;
USE CATALOG retail_lakehouse;

CREATE SCHEMA IF NOT EXISTS lakehouse;
USE SCHEMA lakehouse;

-- One volume for raw landing files, one for the Delta tables themselves.
-- (Delta tables COULD also be registered as managed UC tables rather
-- than path-based Delta - see the note in running_on_databricks.md
-- about that alternative and why we're sticking with path-based here
-- for consistency with the rest of the project's design.)
CREATE VOLUME IF NOT EXISTS landing;
CREATE VOLUME IF NOT EXISTS lakehouse_data;

-- Verify:
LIST '/Volumes/retail_lakehouse/lakehouse/landing';
