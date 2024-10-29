CREATE DATABASE IF NOT EXISTS kestra;

-- Create table employee
DROP TABLE IF EXISTS employee;

CREATE TABLE employee (
    firstName String,
    lastName String,
    age Int8,
    employee_id Int64
)
ENGINE = MergeTree()
ORDER BY (employee_id)
SETTINGS index_granularity = 8192;

INSERT INTO employee (firstName, lastName, age, employee_id)
VALUES
    ('John', 'Doe', 45, 1),
    ('Bryan', 'Grant', 33, 2),
    ('Jude', 'Philips', 25, 3),
    ('Michael', 'Page', 62, 4);

-- Create table laptop
DROP TABLE IF EXISTS laptop;

CREATE TABLE laptop
(
    brand String,
    model String,
    cpu_frequency Decimal(3, 2),
    laptop_id Int64
) ENGINE = MergeTree()
ORDER BY (laptop_id)
SETTINGS index_granularity = 8192;

INSERT INTO laptop (brand, model, cpu_frequency, laptop_id)
VALUES
    ('Apple', 'MacBookPro M1 13', 2.20, 1),
    ('Apple', 'MacBookPro M3 16', 1.50, 2),
    ('LG', 'Gram', 1.95, 3),
    ('Lenovo', 'ThinkPad', 1.05, 4);