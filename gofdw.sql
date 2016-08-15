-- create wrapper with validator and handler
CREATE OR REPLACE FUNCTION gofdw_validator (text[], oid)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION gofdw_handler ()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER gofdw
VALIDATOR gofdw_validator HANDLER gofdw_handler;

