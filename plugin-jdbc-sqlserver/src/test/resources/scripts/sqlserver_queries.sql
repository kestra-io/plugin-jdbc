-- Create table employee
DROP TABLE IF EXISTS employee;

CREATE TABLE employee (
      firstName VARCHAR(100),
      lastName VARCHAR(100),
      age INT
);

INSERT INTO employee (firstName, lastName, age)
VALUES
    ('John', 'Doe', 45),
    ('Bryan', 'Grant', 33),
    ('Jude', 'Philips', 25),
    ('Michael', 'Page', 62);


-- Create table laptop
DROP TABLE IF EXISTS laptop;

CREATE TABLE laptop
(
    brand VARCHAR(100),
    model VARCHAR(100),
    cpu_frequency REAL
);

INSERT INTO laptop (brand, model, cpu_frequency)
VALUES
    ('Apple', 'MacBookPro M1 13', 2.2),
    ('Apple', 'MacBookPro M3 16', 1.5),
    ('LG', 'Gram', 1.95),
    ('Lenovo', 'ThinkPad', 1.05);