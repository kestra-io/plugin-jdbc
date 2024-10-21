DROP TABLE IF EXISTS employee;

CREATE TABLE employee (
    employee_id SERIAL,
    first_name VARCHAR(30),
    last_name VARCHAR(30),
    age SMALLINT,
    PRIMARY KEY (employee_id)
);

INSERT INTO employee (first_name, last_name, age)
VALUES
    ( 'John', 'Doe', 45),
    ( 'Bryan', 'Grant', 33),
    ( 'Jude', 'Philips', 25),
    ( 'Michael', 'Page', 62);

DROP TABLE IF EXISTS laptop;

CREATE TABLE laptop
(
    laptop_id SERIAL,
    brand VARCHAR(30),
    model VARCHAR(30),
    cpu_frequency REAL,
    PRIMARY KEY (laptop_id)
);
INSERT INTO laptop (brand, model, cpu_frequency)
VALUES
    ('Apple', 'MacBookPro M1 13', 2.2),
    ('Apple', 'MacBookPro M3 16', 1.5),
    ('LG', 'Gram', 1.95),
    ('Lenovo', 'ThinkPad', 1.05);


/* Table for testing transactionnal queries */
DROP TABLE IF EXISTS test_transaction;
CREATE TABLE test_transaction
(
    id SERIAL,
    name VARCHAR(30) NOT NULL,
    PRIMARY KEY (id)
);