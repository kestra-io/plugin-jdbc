DROP TABLE IF EXISTS pl_store_distribute;

CREATE TABLE pl_store_distribute
(
    id serial NOT NULL,
    year_month numeric(6,0) NOT NULL,
    store_code numeric NOT NULL,
    is_building numeric(1,0) DEFAULT 0,
    is_cgm_visible numeric(1,0) DEFAULT 0,
    is_validate numeric(1,0) DEFAULT 0,
    is_publish numeric(1,0) DEFAULT 0,
    update_date timestamp(0) without time zone DEFAULT now(),
    update_ldap character varying(80),
    CONSTRAINT store_distribute_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS pgsql_types;

DROP TYPE IF EXISTS inventory_item;

/* Composite Types */
CREATE TYPE inventory_item AS (
    name            text,
    supplier_id     integer,
    price           numeric
);



CREATE TABLE pgsql_types (
 concert_id serial NOT NULL,
 available boolean not null,
 a CHAR(4) not null,
 b VARCHAR(30) not null,
 c TEXT not null,
 d VARCHAR(10),
 play_time smallint not null,
 library_record BIGINT not null,
 -- money_type money not null,
 floatn_test float8 not null,
 double_test double precision not null,
 real_test real not null,
 numeric_test numeric not null,
 date_type DATE not null,
 time_type TIME not null,
 timez_type TIME WITH TIME ZONE not null,
 timestamp_type TIMESTAMP not null,
 timestampz_type TIMESTAMP WITH TIME ZONE not null,
 interval_type INTERVAL not null,
 pay_by_quarter integer[] not null,
 schedule text[][] not null,
 json_type JSON not null,
 jsonb_type JSONB not null,
 item inventory_item not null,
 blob_type bytea not null,
 tsvector_col TSVECTOR not null
);


-- Insert
INSERT INTO pgsql_types
            (concert_id,
             available,
             a,
             b,
             c,
			 d,
             play_time,
             library_record,
             -- money_type,
             floatn_test,
             double_test,
             real_test,
             numeric_test,
             date_type,
             time_type,
             timez_type,
             timestamp_type,
             timestampz_type,
             interval_type,
             pay_by_quarter,
             schedule,
             json_type,
             jsonb_type,
             item,
             blob_type,
             tsvector_col)
VALUES     ( DEFAULT,
             true,
             'four',
             'This is a varchar',
             'This is a text column data',
			 NULL,
             32767,
             9223372036854775807,
             -- 999999.999,
             9223372036854776000,
             9223372036854776000,
             9223372036854776000,
             2147483645.1234,
             '2030-12-25',
             '04:05:30',
             '04:05:06 PST',
             '2004-10-19 10:23:54.999999',
             '2004-10-19 10:23:54.250+04',
             '10 years 4 months 5 days 10 seconds',
             '{100,200,300}',
             '{{meeting,lunch},{training,presentation}}',
             '{"color":"red","value":"#f00"}',
             '{"color":"blue","value":"#0f0"}',
             Row('fuzzy dice', 42, 1.99),
             '\xDEADBEEF',
              to_tsvector('english', 'fuzzy dice quick brown fox jumps over lazy dog'));


-- Insert
INSERT INTO pgsql_types
            (concert_id,
             available,
             a,
             b,
             c,
			 d,
             play_time,
             library_record,
             -- money_type,
             floatn_test,
             double_test,
             real_test,
             numeric_test,
             date_type,
             time_type,
             timez_type,
             timestamp_type,
             timestampz_type,
             interval_type,
             pay_by_quarter,
             schedule,
             json_type,
             jsonb_type,
             item,
             blob_type,
             tsvector_col)
VALUES     ( DEFAULT,
             true,
             'four',
             'This is a varchar',
             'This is a text column data',
			 NULL,
             32767,
             9223372036854775807,
             -- 999999.999,
             9223372036854776000,
             9223372036854776000,
             9223372036854776000,
             2147483645.1234,
             '2030-12-25',
             '04:05:30',
             '04:05:06 PST',
             '2004-10-19 10:23:54.999999',
             '2004-10-19 10:23:54.250+04',
             '10 years 4 months 5 days 10 seconds',
             '{100,200,300}',
             '{{meeting,lunch},{training,presentation}}',
             '{"color":"red","value":"#f00"}',
             '{"color":"blue","value":"#0f0"}',
             Row('fuzzy dice', 42, 1.99),
             '\xDEADBEEF',
              to_tsvector('english', 'fuzzy dice quick brown fox jumps over lazy dog'));
