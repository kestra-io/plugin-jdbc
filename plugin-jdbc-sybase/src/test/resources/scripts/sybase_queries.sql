IF EXISTS(select 1 from sysobjects where name='employee' and type='U') DROP TABLE employee;

CREATE TABLE employee (
  employee_id INT IDENTITY PRIMARY KEY,
  firstName VARCHAR(30),
  lastName VARCHAR(30),
  age INT
);

INSERT INTO employee (firstName, lastName, age) VALUES ('John', 'Doe', 45);
INSERT INTO employee (firstName, lastName, age) VALUES ('Bryan', 'Grant', 33);
INSERT INTO employee (firstName, lastName, age) VALUES ('Jude', 'Philips', 25);
INSERT INTO employee (firstName, lastName, age) VALUES ('Michael', 'Page', 62);

IF EXISTS(select 1 from sysobjects where name='laptop' and type='U') DROP TABLE laptop;

CREATE TABLE laptop
(
    laptop_id INT IDENTITY PRIMARY KEY,
    brand VARCHAR(30),
    model VARCHAR(30),
    cpu_frequency REAL
);

INSERT INTO laptop (brand, model, cpu_frequency) VALUES ('Apple', 'MacBookPro M1 13', 2.2);
INSERT INTO laptop (brand, model, cpu_frequency) VALUES ('Apple', 'MacBookPro M3 16', 1.5);
INSERT INTO laptop (brand, model, cpu_frequency) VALUES ('LG', 'Gram', 1.95);
INSERT INTO laptop (brand, model, cpu_frequency) VALUES ('Lenovo', 'ThinkPad', 1.05);