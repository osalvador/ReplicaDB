[![Build Status](https://travis-ci.org/osalvador/ReplicaDB.svg?branch=master)](https://travis-ci.org/osalvador/ReplicaDB) [![GitHub license](https://img.shields.io/github/license/osalvador/ReplicaDB.svg)](https://github.com/osalvador/ReplicaDB/blob/master/LICENSE) [![Twitter](https://img.shields.io/twitter/url/https/github.com/osalvador/ReplicaDB.svg?style=social)](https://twitter.com/intent/tweet?text=Wow:&url=https%3A%2F%2Fgithub.com%2Fosalvador%2FReplicaDB)

![replicadb-logo](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/replicadb-logo.png)

ReplicaDB is open source tool for database replication designed for efficiently transferring bulk data between relational and NoSQL databases.

ReplicaDB helps offload certain tasks, such as ETL or ELT processing, for efficient execution at a much lower cost. Actualy, ReplicaDB  works with Oracle, Postgres, SQL Server, Denodo, CSV on local files or on Amazon S3 and Kafka. 
  
ReplicaDB is **Cross Platform**; you can replicate data across different platforms, with compatibility for many databases. You can use **Parallel data transfer** for faster performance and optimal system utilization.

<br>

![ReplicaDB-Conceptual](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/ReplicaDB-Conceptual.png)


# Why another database replication software

Because I have not found any tool that covers my needs:

- Open Source.
- Java based cross-platform solution.
- Any database engine SQL, NoSQL or other persistent stores like CSV, Amazon S3 or Kafka. 
- Simple architecture, just a command line tool that can run on any server (including my laptop), without any remote agents in the databases.
- Good performance for a large amount of data. 
- I do not need streaming replication, or a pure change data capture (CDC) system that requires installation in the source database.

I have reviewed and tested other open source tools and none of them meets all the above requirements:

- **SymetricDS**: It was the best option of all, but I was looking for a smaller solution, mainly focused on performance. SymmetricDS is intrusive since installs database triggers that capture data changes in a data capture table. This table requires maintenance. SymmetricDS is more like a CDC system based on triggers.  
- **Sqoop**: Sqoop is what I was looking for, but oh! it is only valid for Hadoop.
- **Kettel** and **Talend**: Both are very complete ETL tools, but for each of the different source and sink tables that I have to replicate, I should do a custom development


# Installation

## System Requirements

ReplicaDB is written in Java and requires a Java Runtime Environment (JRE) Standard Edition (SE) or Java Development Kit (JDK) Standard Edition (SE) version 8.0 or above. The minimum operating system requirements are:

*   Java SE Runtime Environment 8 or above    
*   Memory - 128 (MB) available

## Install

Just download [latest](https://github.com/osalvador/ReplicaDB/releases) release and unzip it. 

```bash
$ curl -o ReplicaDB-0.6.2.tar.gz -L "https://github.com/osalvador/ReplicaDB/releases/download/v0.6.2/ReplicaDB-0.6.2.tar.gz"
$ tar -xvzf ReplicaDB-0.6.2.tar.gz
$ ./bin/replicadb --help
```

## JDBC Drivers

You can use ReplicaDB with any JDBC-compliant database. First, download the appropriate JDBC driver for the type of database you want to use, and install the `.jar` file in the `$REPLICADB_HOME/lib` directory on your client machine. Each driver `.jar` file also has a specific driver class which defines the entry-point to the driver. 


# Usage example

## Oracle to PostgreSQL

Source and Sink tables must exists. 

```bash
$ replicadb --mode=complete -j=1 \
--source-connect=jdbc:oracle:thin:@${ORAHOST}:${ORAPORT}:${ORASID} \
--source-user=${ORAUSER} \
--source-password=${ORAPASS} \
--source-table=dept \
--sink-connect=jdbc:postgresql://${PGHOST}/osalvador \
--sink-table=dept
2018-12-07 16:01:23,808 INFO  ReplicaTask:36: Starting TaskId-0
2018-12-07 16:01:24,650 INFO  SqlManager:197: TaskId-0: Executing SQL statement: SELECT /*+ NO_INDEX(dept)*/ * FROM dept where ora_hash(rowid,0) = ?
2018-12-07 16:01:24,650 INFO  SqlManager:204: TaskId-0: With args: 0,
2018-12-07 16:01:24,772 INFO  ReplicaDB:89: Total process time: 1302ms
```

![ReplicaDB-Ora2PG.gif](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/ReplicaDB-Ora2PG.gif)

## PostgreSQL to Oracle

```bash
$ replicadb --mode=complete -j=1 \
--sink-connect=jdbc:oracle:thin:@${ORAHOST}:${ORAPORT}:${ORASID} \
--sink-user=${ORAUSER} \
--sink-password=${ORAPASS} \
--sink-table=dept \
--source-connect=jdbc:postgresql://${PGHOST}/osalvador \
--source-table=dept \
--source-columns=dept.*
2018-12-07 16:10:35,334 INFO  ReplicaTask:36: Starting TaskId-0
2018-12-07 16:10:35,440 INFO  SqlManager:131 TaskId-0: Executing SQL statement: SELECT  * FROM dept OFFSET ?
2018-12-07 16:10:35,441 INFO  SqlManager:204: TaskId-0: With args: 0,
2018-12-07 16:10:35,550 INFO  OracleManager:98 Inserting data with this command: INSERT INTO /*+APPEND_VALUES*/ ....
2018-12-07 16:10:35,552 INFO  ReplicaDB:89: Total process time: 1007ms
```

# Compatible Databases

| Database Vendor | Source | Sink Complete | Sink Incremental |
|-----------------|:---------:|:---------------:|:------------------:|
| Oracle          | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark:|
| PostgreSQL      | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark:|
| Denodo          | :heavy_check_mark: | N/A | N/A |  
| CSV             | :heavy_multiplication_x: | :heavy_check_mark: | :heavy_check_mark: | 
| SQL Server      | :heavy_check_mark: | :heavy_multiplication_x: | :heavy_multiplication_x: |
| Kafka           | :heavy_multiplication_x: | N/A | :heavy_check_mark: |  
| Amazon S3       | :heavy_multiplication_x: | :heavy_check_mark: | N/A |  


# Roadmap

1. MongoDB as source.
2. MySQL; source and sink.
3. SQL Server; source and sink.
4. CSV file as source. 
5. Kafka as source.


# Contributing
  
1. Fork it (https://github.com/osalvador/ReplicaDB)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request
