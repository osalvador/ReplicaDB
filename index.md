---
layout: page
homepage: true
---

[![Build Status](https://travis-ci.org/osalvador/ReplicaDB.svg?branch=master)](https://travis-ci.org/osalvador/ReplicaDB) [![GitHub license](https://img.shields.io/github/license/osalvador/ReplicaDB.svg)](https://github.com/osalvador/ReplicaDB/blob/master/LICENSE) [![Twitter](https://img.shields.io/twitter/url/https/github.com/osalvador/ReplicaDB.svg?style=social)](https://twitter.com/intent/tweet?text=Wow:&url=https%3A%2F%2Fgithub.com%2Fosalvador%2FReplicaDB)

# [![replicadb-logo png](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/replicadb-logo.png)](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/replicadb-logo.png)


ReplicaDB is open source tool for database replication designed for efficiently transferring bulk data between relational and NoSQL databases.

ReplicaDB helps offload certain tasks, such as ETL processing, for efficient execution at a much lower cost. Actualy, ReplicaDB only works with Oracle and Postgres.

  
ReplicaDB is **Cross Platform**; you can replicate data across different platforms, with compatibility for many databases. You can use **Parallel data transfer** for faster performance and optimal system utilization.


## Installation

### System Requirements

ReplicaDB is written in Java and requires a Java Runtime Environment (JRE) Standard Edition (SE) or Java Development Kit (JDK) Standard Edition (SE) version 8.0 or above. The minimum operating system requirements are:

*   Java SE Runtime Environment 8 or above    
*   Memory - 64 (MB) available

### Install

Just download [latest](https://github.com/osalvador/ReplicaDB/releases) release and unzip it. 

```bash
replicadb$ wget https://github.com/osalvador/ReplicaDB/releases/download/v0.1.2/ReplicaDB-0.1.2.tar.gz
replicadb$ tar -xvzf ReplicaDB-0.1.2.tar.gz
x bin/
x bin/configure-replicadb
...

replicadb$ ./bin/replicadb --help
usage: replicadb [OPTIONS]
...
```

### JDBC Drivers

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

1. [Introduction](#1-Introduction)
2. [Basic Usage](#2-Basic-Usage)
3. [Command Line Arguments](#3-Command-Line-Arguments )
    1. [Using Options Files to Pass Arguments](#31-using-options-files-to-pass-arguments)
    2. [Connecting to a Database Server](#32-connecting-to-a-database-server)
    3. [Selecting the Data to Import](#33-selecting-the-data-to-import)
    4. [Free-form Query Imports](#34-free-form-query-imports)
    5. [Controlling Parallelism](#35-controlling-parallelism)
    <!-- 3.7. Controlling transaction isolation
    3.9. Incremental Imports
    3.10. File Formats
    3.11. Large Objects
    3.15. Additional Import Configuration Properties
    6. Example Invocations -->
4. [Compatible Databases](#4-compatible-databases)

### 1. Introduction

### 2. Basic Usage

With ReplicaDB, you can _replicate_ data between relational databases and non replational databases. The input to the replication process is a database table, or custom query. For replational databases, ReplicaDB will read the table row-by-row. The output of this replication process is table in the sink database containing a copy of the source table. The replication process is performed in parallel.


### 3. Command Line Arguments 

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

ReplicaDB is designed to replicate tables between databases. To do so, you must specify a _connect string_ that describes how to connect to the database. The _connect string_ is similar to a URL, and is communicated to ReplicaDB with the `--source-connect` or `--sink-connect` arguments. This describes the server and database to connect to; it may also specify the port. For example:

```
$ replicadb --source-connect jdbc:mysql://database.example.com/employees
```

This string will connect to a MySQL database named `employees` on the host `database.example.com`.

You might need to authenticate against the database before you can access it. You can use the `--source-username` or `--sink-username` to supply a username to the database.

ReplicaDB provides couple of different ways to supply a password, secure and non-secure, to the database which is detailed below.

**Secure way of supplying password to the database**

To supply a password securely, the options file must be used using the `--options-file` argument. For example:

```
$ replicadb --source-connect jdbc:mysql://database.example.com/employees \
--source-username boss --options-file ./conf/empoloyee.conf
```

where the options file `./conf/empoloyee.conf` contains the following:

```properties
source-password=myEmployeePassword
```

**Unsecure way of supplying password to the database**

```
$ replicadb --source-connect jdbc:mysql://database.example.com/employees \
--source-username boss --options-file myEmployeePassword
```


#### 3.3 Selecting the Data to Import

ReplicaDB typically imports data in a table-centric fashion. Use the `--source-table` argument to select the table to replicate. For example, `--source-table employees`. This argument can also identify a `VIEW` or other table-like entity in a database.

By default, all columns within a table are selected for replication. You can select a subset of columns and control their ordering by using the `--source-columns` argument. This should include a comma-delimited list of columns to import. For example: `--source-columns "name,employee_id,jobtitle"`.

You can control which rows are imported by adding a SQL `WHERE` clause to the import statement. By default, ReplicaDB generates statements of the form `SELECT <column list> FROM <table name>`. You can append a `WHERE` clause to this with the `--sourece-where` argument. For example: `--source-where "id > 400"`. Only rows where the `id` column has a value greater than 400 will be imported.

#### 3.4 Free-form Query Imports

ReplicaDB can also replicate the result set of an arbitrary SQL query. Instead of using the `--sourece-table`, `--sourece-columns` and `--source-where` arguments, you can specify a SQL statement with the `--sourece-query` argument.

For example:

```
$ replicadb --source-query 'SELECT a.*, b.* FROM a JOIN b on (a.id == b.id)'
```


#### 3.5 Controlling Parallelism    

ReplicaDB replicate data in parallel from most database sources. You can specify the number of job tasks (parallel processes) to use to perform the replication by using the `-j` or `--jobs` argument. Each of these arguments takes an integer value which corresponds to the degree of parallelism to employ. By default, four tasks are used. Some databases may see improved performance by increasing this value to 8 or 16. Do not increase the degree of parallism higher than that which your database can reasonably support. Connecting 100 concurrent clients to your database may increase the load on the database server to a point where performance suffers as a result.


### 4. Compatible Databases

{:.table}

| Database Vendor | Source | Sink | 
|----------------|------|--------|
| Oracle           | :white_check_mark: | :white_check_mark: | 
| PostgreSQL       |:white_check_mark: | :white_check_mark: | 


## Contributing
  
1. Fork it (https://github.com/osalvador/ReplicaDB)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request



# Overview

> This is a [Jekyll theme](https://github.com/allejo/jekyll-docs-theme) based on [mistic100's modification](https://github.com/mistic100/jekyll-bootstrap-doc) of the official Bootstrap documentation from a few years back.

Jekyll Docs Theme is provided as a theme for writing documentation for your projects instead of having a single large README file or several markdown files stored in a not so user-friendly manner.

This theme is still in development but is kept fairly stable; just note, there are a lot things yet to come.

# Installation

Add this line to your Jekyll site's Gemfile:

```ruby
gem "jekyll-docs-theme"
```

And add this line to your Jekyll site's _config.yml:

```yaml
theme: jekyll-docs-theme
```

And then execute:

```
$ bundle
```

Or install it yourself as:

```
$ gem install jekyll-bootstrap-doc
```

<div class="alert alert-warning" markdown="1">
**Warning:** Custom [themes are not supported on GitHub Pages](https://pages.github.com/themes/) at the time of writing this, so you may either build your site on another platform or simply fork this repo and build upon it as you would any other theme.
</div>

# Configuration Options

A sample [`_config.yml`](https://github.com/allejo/jekyll-docs-theme/blob/master/docs/_config.yml) file is available with all of the available fields; documentation and more information for each of those fields is available below.

## Project

The project object can be specified with information related to the software this; this information will appear on the homepage's jumbotron area.

```yaml
project:
  version: 1.0.0
  download_url: https://github.com/USER/PROJECT/releases
```

{:.table}
| field | description |
| ----- | ----------- |
| `version` | The current version of the software |
| `download_url` | The URL to the current download |

## Licenses

The license object accepts four fields regarding information about the licensing of your software and documentation.

```yaml
license:
  software: MIT License
  software_url: http://opensource.org/licenses/MIT

  docs: CC BY 3.0
  docs_url: http://creativecommons.org/licenses/by/3.0/
```

{:.table}
| field | description |
| ----- | ----------- |
| `software` | The license the software is distributed under |
| `software_url` | A URL to the license text for the license specified in `software` |
| `docs` | The license this documentation is distributed under |
| `docs_url` | A URL to the license text for the license specified in `docs` |

## Links

The links object has two subobjects, `header` and `footer`; both of these objects accept an array of elements with a `title` and `url`. The links defined in the `header` object will appear in the navigation of the website and the links in the `footer` will appear at the bottom of the website.

```yaml
links:
  header:
    - title: GitHub
      url: https://github.com/allejo/jekyll-docs-theme
  footer:
    - title: GitHub
      url: https://github.com/allejo/jekyll-docs-theme
    - title: Issues
      url: https://github.com/allejo/jekyll-docs-theme/issues?state=open
```

{:.table}
| field | description |
| ----- | ----------- |
| `title` | The textual representation of the URL |
| `url` | The URL of the link |

## UI

The ui object will contain all the settings in regards to the aesthetics of the website

```yaml
ui:
  header:
    color1: "#080331"
    color2: "#673051"
    trianglify: true
```

{:.table}
| field | description |
| ----- | ----------- |
| `color1` & `color2` | The two colors that will create the gradient of the page header |
| `trianglify` | When set to true, the page header will be a generated triangular pattern |

## Analytics

```yaml
analytics:
    google: UA-123456-1
```

{:.table}
| field | description |
| ----- | ----------- |
| `google` | The unique identifier for Google Analytics; typically looks like `U-123456-1`

## Social

Options for configuring buttons to "like", "tweet" or "star" this site with the respective social media websites.

```yaml
social:
  github:
    user: allejo
    repo: jekyll-docs-theme
  twitter:
    enabled: false
    via:
    hash:
    account:
  facebook:
    enabled: false
    profileUrl:
```
