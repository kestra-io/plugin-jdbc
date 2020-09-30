# Kestra JDBC Plugins

<p align="center">
  <img width="460" src="https://github.com/kestra-io/kestra/raw/master/ui/src/assets/logo.svg?sanitize=true"  alt="Kestra workflow orchestrator" />
</p>

> Tasks to interact with SQL Databases via JDBC

## Tasks 

### MySQL
* `org.kestra.task.jdbc.mysql.Query`: Execute an sql query against a MySql database

### PostgreSQL
* `org.kestra.task.jdbc.postgresql.Query`: Execute an sql query against a PostgreSQL database

### Documentation

See plugin documentation for information about supported types for each database.

## Structure
* `jdbc`: contains common code regarding interaction with sql databases
* `mysql`: specific to mysql, mainly for managing datatype transformation (database -> jdbc)
* `postgres`: specific to postresql, mainly for managing datatype transformation (database -> jdbc)

## Packaging

A `fat` jar can be generated using the `shadowJar` plugin for `mysql` and `postgres` modules.
This `fat` jar contains all required dependencies to be used in a Kestra environment.

## License
Apache 2.0 © [Nigh Tech](https://nigh.tech)
