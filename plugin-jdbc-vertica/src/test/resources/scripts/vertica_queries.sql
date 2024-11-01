-- Create table employee
DROP TABLE IF EXISTS employee;

CREATE TABLE employee (
  firstName VARCHAR,
  lastName VARCHAR,
  age INTEGER
);

INSERT INTO employee(firstName, lastName, age) VALUES ('John', 'Doe', 45);
INSERT INTO employee(firstName, lastName, age) VALUES ('Bryan', 'Grant', 33);
INSERT INTO employee(firstName, lastName, age) VALUES ('Jude', 'Philips', 25);
INSERT INTO employee(firstName, lastName, age) VALUES ('Michael', 'Page', 62);


-- Create table laptop
DROP TABLE IF EXISTS laptop;

CREATE TABLE laptop
(
    brand VARCHAR,
    model VARCHAR,
    cpu_frequency DOUBLE PRECISION
);

INSERT INTO laptop (brand, model, cpu_frequency) VALUES ('Apple', 'MacBookPro M1 13', 2.2);
INSERT INTO laptop (brand, model, cpu_frequency) VALUES ('Apple', 'MacBookPro M3 16', 1.5);
INSERT INTO laptop (brand, model, cpu_frequency) VALUES ('LG', 'Gram', 1.95);
INSERT INTO laptop (brand, model, cpu_frequency) VALUES ('Lenovo', 'ThinkPad', 1.05);