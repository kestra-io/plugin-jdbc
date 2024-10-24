
-- Create table employee
DROP TABLE IF EXISTS employee;

CREATE TABLE employee (
                          employee_id INTEGER PRIMARY KEY,
                          firstName TEXT,
                          lastName TEXT,
                          age INTEGER
);

INSERT INTO employee (employee_id, firstName, lastName, age)
VALUES
    (1, 'John', 'Doe', 45),
    (2, 'Bryan', 'Grant', 33),
    (3, 'Jude', 'Philips', 25),
    (4, 'Michael', 'Page', 62);


-- Create table laptop
DROP TABLE IF EXISTS laptop;

CREATE TABLE laptop
(
    laptop_id INTEGER PRIMARY KEY,
    brand TEXT,
    model TEXT,
    cpu_frequency REAL
);

INSERT INTO laptop (laptop_id, brand, model, cpu_frequency)
VALUES
    (1, 'Apple', 'MacBookPro M1 13', 2.2),
    (2, 'Apple', 'MacBookPro M3 16', 1.5),
    (3, 'LG', 'Gram', 1.95),
    (4, 'Lenovo', 'ThinkPad', 1.05);