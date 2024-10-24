-- Create table employee
DROP TABLE IF EXISTS DB2INST1.employee;

CREATE TABLE DB2INST1.employee (
      id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      firstName VARCHAR(200),
      lastName VARCHAR(200),
      age INT
);

INSERT INTO DB2INST1.employee (firstName, lastName, age)
VALUES
    ('John', 'Doe', 45),
    ('Bryan', 'Grant', 33),
    ('Jude', 'Philips', 25),
    ('Michael', 'Page', 62);


-- Create table laptop
DROP TABLE IF EXISTS DB2INST1.laptop;

CREATE TABLE DB2INST1.laptop
(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    brand VARCHAR(200),
    model VARCHAR(200),
    cpu_frequency DOUBLE
);

INSERT INTO DB2INST1.laptop (brand, model, cpu_frequency)
VALUES
    ('Apple', 'MacBookPro M1 13', 2.2),
    ('Apple', 'MacBookPro M3 16', 1.5),
    ('LG', 'Gram', 1.95),
    ('Lenovo', 'ThinkPad', 1.05);