
DROP TABLE IF EXISTS pgsql_types;
DROP TABLE IF EXISTS pgsql_nosql;
DROP TABLE IF EXISTS namedInsert;

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
 blob_type bytea not null
);


CREATE TABLE pgsql_nosql (
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
 blob_type bytea not null
);


CREATE TABLE namedInsert (
 id integer,
 name VARCHAR,
 address VARCHAR
 );
