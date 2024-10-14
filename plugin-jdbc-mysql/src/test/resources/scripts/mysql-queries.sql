USE kestra;
DROP TABLE IF EXISTS employee;

CREATE TABLE employee (employee_id SERIAL NOT NULL, firstName VARCHAR(30), lastName VARCHAR(30), PRIMARY KEY (employee_id));

INSERT INTO employee (employee_id, firstName, lastName)
VALUES
    (1, 'John', 'Doe'),
    (2, 'Bryan', 'Grant');

DROP TABLE IF EXISTS laptop;

CREATE TABLE laptop
(
    laptop_id SERIAL NOT NULL,
    brand VARCHAR(30),
    model VARCHAR(30),
    PRIMARY KEY (laptop_id)
);

INSERT INTO laptop (laptop_id, brand, model)
VALUES
    (1, 'Apple', 'MacBookPro M1 16'),
    (2, 'Lenovo', 'ThinkPad');