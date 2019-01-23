---
layout: page
---

# Docs

- [Docs](#docs)
- [1. Introduction](#1-introduction)
- [2. Basic Usage](#2-basic-usage)
  - [2.1 Replication Mode](#21-replication-mode)
  - [2.2 Controlling Parallelism](#22-controlling-parallelism)
- [3. Command Line Arguments](#3-command-line-arguments)
  - [3.1 Using Options Files to Pass Arguments](#31-using-options-files-to-pass-arguments)
  - [3.2 Connecting to a Database Server](#32-connecting-to-a-database-server)
  - [3.3 Selecting the Data to Replicate](#33-selecting-the-data-to-replicate)
  - [3.4 Free-form Query Replications](#34-free-form-query-replications)
- [4. Notes for specific connectors](#4-notes-for-specific-connectors)
  - [4.1 CSV files Connector](#41-csv-files-connector)
    - [4.1.2 Extra parameters](#412-extra-parameters)
    - [4.1.3 Replication Mode](#413-replication-mode)
  - [4.2 Oracle Connector](#42-oracle-connector)
  - [4.3 PostgreSQL Connector](#43-postgresql-connector)
  - [4.4 Denodo Connector](#44-denodo-connector)

{::comment}
    3.7. Controlling transaction isolation
    3.11. Large Objects
    ?Performance considerations
    Sink analyze
    
    6. Example Invocations
{:/comment}

# 1. Introduction

ReplicaDB is primarily a command line, portable and cross-platform tool for data replication between a source and a sink databases. Its main objective is performance, implementing all the specific DataBase engine techniques to achieve the best performance for each of them, either as source or as sink.

ReplicaDB follows the Convention over configuration design, so the user will introduce the minimum parameters necessary for the replication process, the rest will be default.

# 2. Basic Usage

With ReplicaDB, you can _replicate_ data between relational databases and non replational databases. The input to the replication process is a database table, or custom query. ReplicaDB will read the source table row-by-row and the output of this replication process is table in the sink database containing a copy of the source table. The replication process is performed in parallel.

By default, ReplicaDB will truncate the sink table before populating it with data, unless `--sink-disable-truncate false` is indicated.

<br>
## 2.1 Replication Mode

ReplicaDB implements two replication modes: `complete` and` incremental`. The main difference between them is:

- The `complete` mode makes a complete replica of the source table, of all its data, from source to sink. In `complete` mode, only` INSERT` is done in the sink table without worrying about the primary keys. ReplicaDB will perform the following actions on a `complete` replication:

    - Truncate the sink table with the `TRUNCATE TABLE` statement
    - Copy the data in parallel from the source table to the sink table.

<br>
- The `incremental` mode performs an incremental replication of the data from the source table to the sink table. In the `incremental` mode, the` INSERT or UPDATE` or `UPSERT` technique is used in the sink table. To do this and to allow the copy of the data in parallel, it is necessary to create a staging table in the sink database. ReplicaDB will perform the following actions in an `incremental` replication:

    - Automatically create the staging table in the sink database.
    - Truncate the staging table.
    - Copy the data in parallel from the source table to the sink staging table.
    - Gets the primary keys of the sink table
    - Execute the `UPSERT` sentence between the sink staging table and the sink table. This statement will depend on the Database Vendor, it can be for example `INSERT ... ON CONFLICT ... DO UPDATE` in PostgreSQL or` MERGE INTO ... `in Oracle.
    - Drop the sink staging table.

<br>
## 2.2 Controlling Parallelism    

ReplicaDB replicate data in parallel from most database sources. You can specify the number of job tasks (parallel processes) to use to perform the replication by using the `-j` or `--jobs` argument. Each of these arguments takes an integer value which corresponds to the degree of parallelism to employ. By default, four tasks are used. Some databases may see improved performance by increasing this value to 8 or 16. Do not increase the degree of parallism higher than that which your database can reasonably support. Connecting 100 concurrent clients to your database may increase the load on the database server to a point where performance suffers as a result.


# 3. Command Line Arguments 

ReplicaDB ships with a help tool. To display a list of all available options, type the following command:

```bash
$ replicadb --help
usage: replicadb [OPTIONS]
...
```

**Table 1. Common arguments**

{:.table}

| Argument                            | Description                                                                           | Default            |
| ----------------------------------- | ------------------------------------------------------------------------------------- | ------------------ |
| `--fetch-size <fetch-size>`         | Number of entries to read from database at once.                                      | `5000`             |
| `-h`,`--help`                       | Print this help screen                                                                |                    |
| `-j`,`--jobs <n>`                   | Use n jobs to replicate in parallel.                                                  | `4`                |
| `--mode <mode>`                     | Specifies the replication mode. The allowed values are complete or incremental        | `complete`         |
| `--options-file <file-path>`        | Options file path location                                                            |                    |
| `--sink-columns <col,col,col...>`   | Sink database table columns to be populated                                           | `--source-columns` |
| `--sink-connect <jdbc-uri>`         | Sink database JDBC connect string                                                     | required           |
| `--sink-disable-escape`             | Escape srings before populating to the table of the sink database.                    | `false`            |
| `--sink-disable-truncate`           | Disable the truncation of the sink database table before populate.                    | `false`            |
| `--sink-password <password>`        | Sink database authentication password                                                 |                    |
| `--sink-staging-schema`             | Scheme name on the sink database, with right permissions for creating staging tables. | `PUBLIC`           |
| `--sink-staging-table`              | Qualified name of the sink staging table. The table must exist in the sink database.  |                    |
| `--sink-table <table-name>`         | Sink database table to populate                                                       | `--source-table`   |
| `--sink-user <username>`            | Sink database authentication username                                                 |                    |
| `--source-columns <col,col,col...>` | Source database table columns to be extracted                                         | `*`                |
| `--source-connect <jdbc-uri>`       | Source database JDBC connect string                                                   | required           |
| `--source-password <password>`      | Source databse authentication password                                                |                    |
| `--source-query <statement>`        | SQL statement to be executed in the source database                                   |                    |
| `--source-table <table-name>`       | Source database table to read                                                         |                    |
| `--source-user <username>`          | Source database authentication username                                               |                    |
| `--source-where <where clause>`     | Source database WHERE clause to use during extraction                                 |                    |
| `-v`,`--verbose`                    | Print more information while working                                                  |                    |
| `--version`                         | Show implementation version and exit                                                  |                    |


<br>
## 3.1 Using Options Files to Pass Arguments

When using ReplicaDB, the command line options that do not change from invocation to invocation can be put in an options file for convenience. An options file is a Java properties text file where each line identifies an option. Option files allow specifying a single option on multiple lines by using the back-slash character at the end of intermediate lines. Also supported are comments within option files that begin with the hash character. Comments must be specified on a new line and may not be mixed with option text. All comments and empty lines are ignored when option files are expanded. 

Option files can be specified anywhere on the command line. Command line argunents override those in the options file. To specify an options file, simply create an options file in a convenient location and pass it to the command line via `--options-file` argument.

For example, the following ReplicaDB invocation for replicate a full table into PostgreSQL can be specified alternatively as shown below:

```bash
$ replicadb --source-connect jdbc:postgresql://localhost/osalvador \
--source-table TEST \
--sink-connect jdbc:postgresql://remotehost/testdb \
--sink-user=testusr \
--sink-table TEST \
--mode complete
```

```bash
$ replicadb --options-file /users/osalvador/work/import.txt -j 4
```

where the options file `/users/osalvador/work/import.txt` contains the following:

```properties
source.connect=jdbc:postgresql://localhost/osalvador
source.table=TEST

sink.connect=jdbc:postgresql://remotehost/testdb
sink.user=testusr
sink.table=TEST

mode=complete
```
<br>
**Using environment variables in options file**

If you are familiar with Ant or Maven, you have most certainly already encountered the variables (like `${token}`) that are automatically expanded when the configuration file is loaded. ReplicaDB supports this feature as well,  here is an example: 

```properties
source.connect=jdbc:postgresql://${PGHOST}$/${PGDATABASE}
source.user=${PGUSER}
source.password=${PGPASSWORD}
source.table=TEST
```


Variables are interpolated from system properties. ReplicaDB will search for a system property with the given name and replace the variable by its value. This is a very easy means for accessing the values of system properties in the options configuration file.

Note that if a variable cannot be resolved, e.g. because the name is invalid or an unknown prefix is used, it won't be replaced, but is returned as is including the dollar sign and the curly braces.

<br>
## 3.2 Connecting to a Database Server

ReplicaDB is designed to replicate tables between databases. To do so, you must specify a _connect string_ that describes how to connect to the database. The _connect string_ is similar to a URL, and is communicated to ReplicaDB with the `--source-connect` or `--sink-connect` arguments. This describes the server and database to connect to; it may also specify the port. For example:

```bash
$ replicadb --source-connect jdbc:mysql://database.example.com/employees
```

This string will connect to a MySQL database named `employees` on the host `database.example.com`.

You might need to authenticate against the database before you can access it. You can use the `--source-username` or `--sink-username` to supply a username to the database.

ReplicaDB provides couple of different ways to supply a password, secure and non-secure, to the database which is detailed below.

<br>
**Specifying extra JDBC parameters**

When connecting to a database using JDBC, you can optionally specify extra JDBC parameters **only** via options file. The contents of this properties are parsed as standard Java properties and passed into the driver while creating a connection.

You can specify these parameters for both the source and sink databases. ReplicaDB will retrieve all the parameters that start with `source.connect.parameter.` or` sink.connect.parameter.` followed by the name of the specific parameter of the database engine.

Examples:

```properties
# Source JDBC connection parameters
# source.connect.parameter.[prameter_name]=parameter_value
# Example for Oracle
source.connect.parameter.oracle.net.tns_admin=${TNS_ADMIN}
source.connect.parameter.oracle.net.networkCompression=on
source.connect.parameter.defaultRowPrefetch=5000
```

```properties
# Sink JDBC connection parameters
# sink.connect.parameter.[prameter_name]=parameter_value
# Example for PostgreSQL
sink.connect.parameter.ApplicationName=ReplicaDB
sink.connect.parameter.reWriteBatchedInserts=true
```


<br>
**Secure way of supplying password to the database**

To supply a password securely, the options file must be used using the `--options-file` argument. For example:

```bash
$ replicadb --source-connect jdbc:mysql://database.example.com/employees \
--source-username boss --options-file ./conf/empoloyee.conf
```

where the options file `./conf/empoloyee.conf` contains the following:

```properties
source-password=myEmployeePassword
```

**Unsecure way of supplying password to the database**

```bash
$ replicadb --source-connect jdbc:mysql://database.example.com/employees \
--source-username boss --options-file myEmployeePassword
```

<br>
## 3.3 Selecting the Data to Replicate

ReplicaDB typically replciate data in a table-centric fashion. Use the `--source-table` argument to select the table to replicate. For example, `--source-table employees`. This argument can also identify a `VIEW` or other table-like entity in a database.

By default, all columns within a table are selected for replication. You can select a subset of columns and control their ordering by using the `--source-columns` argument. This should include a comma-delimited list of columns to replicate. For example: `--source-columns "name,employee_id,jobtitle"`.

You can control which rows are replicated by adding a SQL `WHERE` clause to the statement. By default, ReplicaDB generates statements of the form `SELECT <column list> FROM <table name>`. You can append a `WHERE` clause to this with the `--sourece-where` argument. For example: `--source-where "id > 400"`. Only rows where the `id` column has a value greater than 400 will be replicated.

<br>
## 3.4 Free-form Query Replications

ReplicaDB can also replicate the result set of an arbitrary SQL query. Instead of using the `--sourece-table`, `--sourece-columns` and `--source-where` arguments, you can specify a SQL statement with the `--sourece-query` argument.

For example:

```bash
$ replicadb --source-query 'SELECT a.*, b.* FROM a JOIN b on (a.id == b.id)'
```


# 4. Notes for specific connectors

- 4.1 [CSV files Connector](#41-csv-files-connector)
- 4.2 [Oracle Connector](#42-oracle-connector)
- 4.3 [PostgreSQL Connector](#43-postgresql-connector)
- 4.4 [Denodo Connector](#44-denodo-connector)

<br>
## 4.1 CSV files Connector

To define a CSV file as a sink, set the `sink-connect` parameter to `file:/...`.

```properties
# Windows Paths
sink.connect=file:/C:/Users/osalvador/Downloads/file.csv
## or
sink.connect=file://C:\\Users\\osalvador\\Downloads\\file.csv

# Unix Paths
sink.connect=file:///Users/osalvador/Downloads/file.csv
```

The CSV file connector is [RFC 4180](http://tools.ietf.org/html/rfc4180) compliant whenever you disable the default escape with `--sink-disable-scape` as argument or on the `options-file`:

```properties
sink.disable.escape=true
```

### 4.1.2 Extra parameters

The CSV connector supports the following extra parameters that can only be defined as extra connection parameters in the `options-file`:

{:.table}

| Argument            | Description                                                                                | Default |
| ------------------- | ------------------------------------------------------------------------------------------ | ------- |
| `FieldSeparator`    | Sets the field separator character                                                         | `,`     |
| `TextDelimiter`     | Sets a field enclosing character                                                           | `"`     |
| `LineDelimiter`     | Sets the end-of-line character                                                             | `\n`    |
| `AlwaysDelimitText` | Sets whether the text should always be delimited                                           | `false` |
| `Header`            | Sets whether the first line of the file should be the header, with the names of the fields | `false` |


**Extra parameters with default values**

```
sink.connect.parameter.FieldSeparator=,
sink.connect.parameter.TextDelimiter="
sink.connect.parameter.LineDelimiter=\n
sink.connect.parameter.AlwaysDelimitText=false
sink.connect.parameter.Header=false
```

### 4.1.3 Replication Mode

Unlike in a database, the replication mode for a CSV file as sink has a slight difference:

- **complete**: Create a new file. If the file exists it is overwritten with the new data.
- **incremental**: Add the new data to the existing file. If the file does not exist, it creates it.

> The `Header` parameter is not supported on `incremental` mode.

**Example**

```properties
sink.connect=file:/C:/Users/Oscar/Downloads/articulos.csv

sink.connect.parameter.FieldSeparator=
sink.connect.parameter.TextDelimiter=
sink.connect.parameter.LineDelimiter=
sink.connect.parameter.AlwaysDelimitText=
sink.connect.parameter.Header=

sink.disable.escape=true
```

<br>
## 4.2 Oracle Connector

```properties
sink.connect=jdbc:oracle:thin:@MY_DATABASE_SID
## or
sink.connect=jdbc:oracle:thin:@host:port:sid

sink.user=orauser
sink.password=orapassword
sink.table=schema.table_name

source.connect.parameter.oracle.net.tns_admin=${TNS_ADMIN}
sink.connect.parameter.oracle.net.networkCompression=off
```

XMLType as CSV
```
#,    a.contenido.getclobval() contenido \
```


<br>
## 4.3 PostgreSQL Connector

sink.connect.parameter.ApplicationName=ReplicaDB

El rendimiento aumenta si se corre replicadb en la maquina donde está postgresql

```properties
source.connect=jdbc:postgresql://host:port/db
source.user=pguser
source.password=pgpassword
source.table=schema.table_name
```

<br>
## 4.4 Denodo Connector

Only source. Autocmmit is true. 

```properties
source.connect=jdbc:vdb://host:port/db
source.user=vdbuser
source.password=vdbpassword
source.table=schema.table_name
```