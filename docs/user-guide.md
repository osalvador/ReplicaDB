---
layout: page
---

# User Guide

1. [Introduction](#1-Introduction)
2. [Basic Usage](#2-Basic-Usage)
    - 2.1 [Replication Mode](#21-Replication-Mode)
    - 2.2 [Controlling Parallelism](#22-controlling-parallelism)    
3. [Command Line Arguments](#3-Command-Line-Arguments)
    - 3.1 [Using Options Files to Pass Arguments](#31-using-options-files-to-pass-arguments)
    - 3.2 [Connecting to a Database Server](#32-connecting-to-a-database-server)
    - 3.3 [Selecting the Data to Replicate](#33-selecting-the-data-to-replicate)
    - 3.4 [Free-form Query Replications](#34-free-form-query-replications)    

{::comment}

- Basic parameters
    + replication mode
        - Complete
        - Incremental
    + control del paralelismo
- Connecting to a Database Server
    + Source Database Server parameters
    + Sink Database server parameters
    + Connection parameters solo en el fichero de options files


- 3.6. Incremental Imports
    staging table
    staging schema

    
    3.7. Controlling transaction isolation
    3.11. Large Objects
    ?Performance considerations
    Sink analyze
    
    6. Example Invocations
{:/comment}

# 1. Introduction

ReplicaDB es principalmente una herramienta de linea de comandos, portable y nultiplataforma para la replicacion de datos entre un origen (source) y un destino (sink). Su principal objetivo es el rendimiento, implementando todas las técnias especificas de motor de BD para lograr el mayor rendimiento para cada una de ellas, ya sea como Source o como Sink. 

ReplicaDB sigue un modelo de convención sobre configuración, por lo que se solicitará al usuario los mínimos parametros necesarios para su funcionamiento, el resto se tomarán por defecto. 

# 2. Basic Usage

With ReplicaDB, you can _replicate_ data between relational databases and non replational databases. The input to the replication process is a database table, or custom query. ReplicaDB will read the source table row-by-row and the output of this replication process is table in the sink database containing a copy of the source table. The replication process is performed in parallel.

Por defecto, ReplicaDB truncará la sink table antes de poblarla de datos, a no ser que se indique el parámetro `--sink-disable-truncate=false`. 

<br>
## 2.1 Replication Mode

ReplicaDB implementa dos modos de replicación: `complete` y `incremental`. La principal diferencia entre ambos es: 
    - El modo `complete` tiene como objetivo realizar una replica de la source table completa, de todos sus datos, desde Source hasta Sink. En el modo `complete` solo se realiza `INSERT` en la sink table sin preocuparse de las claves primarias. ReplicaDB realizará las siguientes acciones en una replicación `complete`: 
        - Truncará la sink tabla con la sentencia `TRUNCATE TABLE` 
        - Copiará los datos en paralelo de la source table a la sink table.
    - En cambio el modo `incremental` tiene como objetivo realizar una replica incremental de los datos de la tabla Source a la tabla Sink. En el modo `ìncremental` se utiliza la técnica `INSERT or UPDATE` aka `UPSERT` en la sink table. Para ello y además permitir la copia de los datos en paralelo, es necesario crear una staging table in the sink database. ReplicaDB realizará las siguientes acciones en una replicación `incremental`: 
        - Creará automáticamente la tabla de staging in the sink database.
        - Truncará la tabla de staging. 
        - Copiará los datos en paralelo de la source table a la sink staging table.
        - Recuperará las claves primarias de la sink table
        - Ejecutará la sentencia `UPSERT` entre la sink staging table y la sink table. Esta sentencia dependerá del Database Vendor, puede ser por ejemplo `INSERT ... ON CONFLICT ... DO UPDATE` en PostgreSQL o `MERGE INTO ...` en Oracle. 
        - Borrará la tabla de staging. 

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

| Argument                                 | Description                                                                       | Default | 
|------------------------------------------|-----------------------------------------------------------------------------------|---------| 
|  `-h`,`--help`                           | Print this help screen | |
|  `-j`,`--jobs <n>`                       | Use n jobs to replicate in parallel. | `4`| 
|     `--mode <mode>`                      | Specifies the replication mode. The allowed values are complete or incremental | `complete` | 
|     `--options-file <file-path>`         | Options file path location | | 
|     `--sink-analyze`                     | Analyze sink database table after populate. | `true` | 
|     `--sink-columns <col,col,col...>`    | Sink database table columns to be populated | `--source-columns` |
|     `--sink-connect <jdbc-uri>`          | Sink database JDBC connect string | required | 
|     `--sink-disable-escape`              | Escape srings before populating to the table of the sink database. | `false` | 
|     `--sink-disable-index`               | Disable sink database table indexes before populate. | `false` | 
|     `--sink-disable-truncate`            | Disable the truncation of the sink database table before populate. | `false` | 
|     `--sink-password <password>`         | Sink database authentication password | | 
|    `--sink-staging-schema`               | Scheme name on the sink database, with right permissions for creating staging tables. | _specific `public` Database Vendor schema_ |
|    `--sink-staging-table`                | Qualified name of the sink staging table. The table must exist in the sink database. | |
|     `--sink-table <table-name>`          | Sink database table to populate | `--source-table` |
|     `--sink-user <username>`             | Sink database authentication username | | 
|     `--source-columns <col,col,col...>`  | Source database table columns to be extracted | `*` |
|     `--source-connect <jdbc-uri>`        | Source database JDBC connect string | required | 
|     `--source-password <password>`       | Source databse authentication password | | 
|     `--source-query <statement>`         | SQL statement to be executed in the source database | |
|     `--source-table <table-name>`        | Source database table to read | |
|     `--source-user <username>`           | Source database authentication username | |
|     `--source-where <where clause>`      | Source database WHERE clause to use during extraction | | 
| `-v`,`--verbose`                         | Print more information while working | |
|     `--version`                          | Show implementation version and exit | |


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
