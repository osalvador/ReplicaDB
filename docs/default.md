---
layout: default
---

# Default

1. [Introduction](#1-Introduction)
2. [Basic Usage](#2-Basic-Usage)
3. [Command Line Arguments](#3-Command-Line-Arguments )
    1. [Using Options Files to Pass Arguments](#31-using-options-files-to-pass-arguments)
    2. [Connecting to a Database Server](#32-connecting-to-a-database-server)
    3. [Selecting the Data to Import](#33-selecting-the-data-to-import)
    4. [Free-form Query Imports](#34-free-form-query-imports)
    5. [Controlling Parallelism](#35-controlling-parallelism)
4. [Compatible Databases](#4-compatible-databases)

{::comment}
    3.7. Controlling transaction isolation
    3.9. Incremental Imports
    3.10. File Formats
    3.11. Large Objects
    3.15. Additional Import Configuration Properties
    6. Example Invocations
{:/comment}

## 1. Introduction

## 2. Basic Usage

With ReplicaDB, you can _replicate_ data between relational databases and non replational databases. The input to the replication process is a database table, or custom query. For replational databases, ReplicaDB will read the table row-by-row. The output of this replication process is table in the sink database containing a copy of the source table. The replication process is performed in parallel.


## 3. Command Line Arguments 

ReplicaDB ships with a help tool. To display a list of all available options, type the following command:

```
$ replicadb --help
usage: replicadb [OPTIONS]
...
```

**Table 1. Common arguments**

{:.table}

| Argument                                 | Description | 
|------------------------------------------|--------------------------------------------------------------------------------------| 
|  `-h`,`--help`                           | Print this help screen |
|  `-j`,`--jobs <n>`                       | Use n jobs to replicate in parallel. |
|     `--mode <mode>`                      | Specifies the replication mode. The allowed values are complete or incremental |
|     `--options-file <file-path>`         | Options file path location |
|     `--sink-analyze`                     | Analyze sink database table after populate. |
|     `--sink-columns <col,col,col...>`    | Sink database table columns to be populated |
|     `--sink-connect <jdbc-uri>`          | Sink database JDBC connect string |
|     `--sink-disable-escape`              | Escape srings before populating to the table of the sink database. |
|     `--sink-disable-index`               | Disable sink database table indexes before populate. |
|     `--sink-disable-truncate`            | Disable the truncation of the sink database table before populate. |
|     `--sink-password <password>`         | Sink database authentication password |
|     `--sink-table <table-name>`          | Sink database table to populate |
|     `--sink-user <username>`             | Sink database authentication username |
|     `--source-check-column <column>`     | Specify the column to be examined when determining which rows to be replicated |
|     `--source-columns <col,col,col...>`  | Source database table columns to be extracted |
|     `--source-connect <jdbc-uri>`        | Source database JDBC connect string |
|     `--source-last-value <value>`        | Specifies the maximum value of the source-check-column from the previous replication |
|     `--source-password <password>`       | Source databse authentication password |
|     `--source-query <statement>`         | SQL statement to be executed in the source database |
|     `--source-table <table-name>`        | Source database table to read |
|     `--source-user <username>`           | Source database authentication username |
|     `--source-where <where clause>`      | Source database WHERE clause to use during extraction |
| `-v`,`--verbose`                         | Print more information while working |
|     `--version`                          | Show implementation version and exit |
