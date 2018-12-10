[![Build Status](https://travis-ci.org/osalvador/ReplicaDB.svg?branch=master)](https://travis-ci.org/osalvador/ReplicaDB) [![GitHub license](https://img.shields.io/github/license/osalvador/ReplicaDB.svg)](https://github.com/osalvador/ReplicaDB/blob/master/LICENSE) [![Twitter](https://img.shields.io/twitter/url/https/github.com/osalvador/ReplicaDB.svg?style=social)](https://twitter.com/intent/tweet?text=Wow:&url=https%3A%2F%2Fgithub.com%2Fosalvador%2FReplicaDB)

# [![replicadb-logo png](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/replicadb-logo.png)](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/replicadb-logo.png)

ReplicaDB is open source tool for database replication tool designed for efficiently transferring bulk data between relational and NoSQL databases.

ReplicaDB helps offload certain tasks, such as ETL processing, for efficient execution at a much lower cost. Actualy, ReplicaDB only works with Oracle and Postgres.

  
ReplicaDB is **Cross Platform**; you can replicate data across different platforms, with compatibility for many databases. You can use **Parallel data transfer** for faster performance and optimal system utilization.


## Installation

**System Requirements**

ReplicaDB is written in Java and requires a Java Runtime Environment (JRE) Standard Edition (SE) or Java Development Kit (JDK) Standard Edition (SE) version 8.0 or above. The minimum operating system requirements are:

*   Java SE Runtime Environment 8 or above    
*   Memory - 64 (MB) available

**Install**

Just download [latest](https://github.com/osalvador/ReplicaDB/releases) release and unzip it. 

```bash
replicadb$ wget https://github.com/osalvador/ReplicaDB/releases/download/v0.1.1/ReplicaDB-0.1.1.tar.gz
replicadb$ tar -xvzf ReplicaDB-0.1.1.tar.gz
x bin/
x bin/configure-replicadb
...
replicadb$ ./bin/replicadb --help
usage: replicadb [OPTIONS]
...

```

**JDBC Drivers**

You can use ReplicaDB with any JDBC-compliant database. First, download the appropriate JDBC driver for the type of database you want to use, and install the `.jar` file in the `$REPLICADB_HOME/lib` directory on your client machine. Each driver `.jar` file also has a specific driver class which defines the entry-point to the driver. 


## Usage example

### Oracle to PostgreSQL

Source and Sink tables must exists. 

```
$ replicadb --mode=complete -j=1 \
--source-connect=jdbc:oracle:thin:@$ORAHOST:$ORAPORT:$ORASID \
--source-user=$ORAUSER \
--source-password=$ORAPASS \
--source-table=dept \
--sink-connect=jdbc:postgresql://$PGHOST/osalvador \
--sink-table=dept
2018-12-07 16:01:23,808 INFO  ReplicaTask:36: Starting TaskId-0
2018-12-07 16:01:24,650 INFO  SqlManager:197: TaskId-0: Executing SQL statement: SELECT /*+ NO_INDEX(dept)*/ * FROM dept where ora_hash(rowid,0) = ?
2018-12-07 16:01:24,650 INFO  SqlManager:204: TaskId-0: With args: 0,
2018-12-07 16:01:24,772 INFO  ReplicaDB:89: Total process time: 1302ms
```

[![ReplicaDB-Ora2PG.gif](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/ReplicaDB-Ora2PG.gif)](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/ReplicaDB-Ora2PG.gif)

### PostgreSQL to Oracle


```
$ replicadb --mode=complete -j=1 \
--sink-connect=jdbc:oracle:thin:@$ORAHOST:$ORAPORT:$ORASID \
--sink-user=$ORAUSER \
--sink-password=$ORAPASS \
--sink-table=dept \
--source-connect=jdbc:postgresql://$PGHOST/osalvador \
--source-table=dept \
--source-columns=dept.*
2018-12-07 16:10:35,334 INFO  ReplicaTask:36: Starting TaskId-0
2018-12-07 16:10:35,440 INFO  SqlManager:197: TaskId-0: Executing SQL statement:  WITH int_ctid as (SELECT (('x' || SUBSTR(md5(ctid :: text), 1, 8)) :: bit(32) :: int) ictid  from dept), replicadb_table_stats as (select min(ictid) as min_ictid, max(ictid) as max_ictid from int_ctid )SELECT dept.* FROM dept, replicadb_table_stats WHERE  width_bucket((('x' || substr(md5(ctid :: text), 1, 8)) :: bit(32) :: int), replicadb_table_stats.min_ictid, replicadb_table_stats.max_ictid, 1)  >= ?
2018-12-07 16:10:35,441 INFO  SqlManager:204: TaskId-0: With args: 1,
2018-12-07 16:10:35,552 INFO  ReplicaDB:89: Total process time: 1007ms
```

## ReplicaDB User Guide

1. Introduction
2. Basic Usage
3. Command Line Arguments
    1. Using Options Files to Pass Arguments
    2. Connecting to a Database Server
    3. Selecting the Data to Import
    4. Free-form Query Imports
    5. Controlling Parallelism        
    <!--3.7. Controlling transaction isolation
    3.9. Incremental Imports
    3.10. File Formats
    3.11. Large Objects
    3.15. Additional Import Configuration Properties-->


### 1. Introduction

### 2. Basic Usage

### 3. Command Line Arguments 

ReplicaDB ships with a help tool. To display a list of all available options, type the following command:

```
$ replicadb --help
usage: replicadb [OPTIONS]
...
```

**Table 1. Common arguments**

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


#### 3.1 Using Options Files to Pass Arguments

When using ReplicaDB, the command line options that do not change from invocation to invocation can be put in an options file for convenience. An options file is a Java properties text file where each line identifies an option. Option files allow specifying a single option on multiple lines by using the back-slash character at the end of intermediate lines. Also supported are comments within option files that begin with the hash character. Comments must be specified on a new line and may not be mixed with option text. All comments and empty lines are ignored when option files are expanded. 

Option files can be specified anywhere on the command line. Command line argunents override those in the options file. To specify an options file, simply create an options file in a convenient location and pass it to the command line via `--options-file` argument.

For example, the following ReplicaDB invocation for import can be specified alternatively as shown below:

```
$ replicadb --source-connect jdbc:postgresql://localhost/osalvador \
--source-table TEST \
--sink-connect jdbc:postgresql://remotehost/testdb \
--sink-user=testusr \
--sink-table TEST \
--mode complete
```

```
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

##### Using environment variables in options file

If you are familiar with Ant or Maven, you have most certainly already encountered the variables (like `${token}`) that are automatically expanded when the configuration file is loaded. ReplicaDB supports this feature as well,  here is an example: 

```properties
source.connect=jdbc:postgresql://${PGHOST}$/${PGDATABASE}
source.user=${PGUSER}
source.password=${PGPASSWORD}
source.table=TEST
```


Variables are interpolated from system properties. ReplicaDB will search for a system property with the given name and replace the variable by its value. This is a very easy means for accessing the values of system properties in the options configuration file.

Note that if a variable cannot be resolved, e.g. because the name is invalid or an unknown prefix is used, it won't be replaced, but is returned as is including the dollar sign and the curly braces.


#### 3.2 Connecting to a Database Server

TODO: 

#### 3.2 Selecting the Data to Import

TODO: 

#### 3.2 Free-form Query Imports

TODO: 

#### 3.2Controlling Parallelism    

TODO: 

## Contributing
  
1. Fork it (https://github.com/osalvador/ReplicaDB)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request