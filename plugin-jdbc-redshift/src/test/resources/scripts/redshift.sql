DROP TABLE IF EXISTS pgsql_types;

CREATE TABLE pgsql_types (
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
    pay_by_quarter super encode zstd not null,
    schedule super encode zstd not null/*,
    hllsketch_type hllsketch not null*/
);


-- Insert
INSERT INTO pgsql_types
(
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
    pay_by_quarter,
    schedule/*,
    hllsketch_type*/
)
VALUES
(
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
    json_parse('[100,200,300]'),
    json_parse('[{"type": "meeting", "name": "lunch"},{"type": "training", "name": "presentation"}]')/*,
    '{"logm":15,"sparse":{"indices":[4878,9559,14523],"values":[1,2,1]}}'*/
);

