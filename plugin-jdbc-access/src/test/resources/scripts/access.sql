-- Reference schema for Access database tests.
-- Note: this file is not used directly; database initialization is done programmatically
-- in test classes to avoid Access SQL syntax limitations with H2's RunScript executor.

-- Access/UCanAccess supported types: INTEGER, VARCHAR, DOUBLE, BIT (boolean), DATETIME
-- Note: Access does not support CREATE TABLE IF NOT EXISTS or DROP TABLE IF EXISTS
CREATE TABLE access_types (
    id INTEGER,
    text_column VARCHAR(255),
    int_column INTEGER,
    d VARCHAR(255)
);

INSERT INTO access_types (id, text_column, int_column, d) VALUES (1, 'Sample Text', 42, NULL);
