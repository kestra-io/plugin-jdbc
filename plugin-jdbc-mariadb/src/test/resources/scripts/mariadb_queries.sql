USE kestra;

DROP TABLE IF EXISTS employee;

CREATE TABLE employee (
    employee_id SERIAL NOT NULL,
    firstName VARCHAR(30),
    lastName VARCHAR(30),
    age INT,
    PRIMARY KEY (employee_id)
);

INSERT INTO employee (employee_id, firstName, lastName, age)
VALUES
    (1, 'John', 'Doe', 45),
    (2, 'Bryan', 'Grant', 33),
    (3, 'Jude', 'Philips', 25),
    (4, 'Michael', 'Page', 62);

DROP TABLE IF EXISTS laptop;

CREATE TABLE laptop
(
    laptop_id SERIAL NOT NULL,
    brand VARCHAR(30),
    model VARCHAR(30),
    cpu_frequency FLOAT,
    PRIMARY KEY (laptop_id)
);
INSERT INTO laptop (laptop_id, brand, model, cpu_frequency)
VALUES
    (1, 'Apple', 'MacBookPro M1 13', 2.2),
    (2, 'Apple', 'MacBookPro M3 16', 1.5),
    (3, 'LG', 'Gram', 1.95),
    (4, 'Lenovo', 'ThinkPad', 1.05);


/* Table for testing transactionnal queries */
DROP TABLE IF EXISTS test_transaction;
CREATE TABLE test_transaction
(
    id MEDIUMINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(30) NOT NULL,
    PRIMARY KEY (id)
);