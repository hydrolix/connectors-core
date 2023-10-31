# Hydrolix Connectors Core

## Overview

`connectors-core` is a library that collects Hydrolix-specific, but platform-agnostic code to be 
used for building connectors for various big data platforms to read Hydrolix data.   

## Components

### hdx_reader
An operating mode of the `turbine_cmd` binary, launched by `HdxPartitionReader` as a child process to read Hydrolix
partitions. Packaged in the JAR, [not open source](#proprietary)!

### Hydrolix Cluster
A preexisting Hydrolix cluster; must be [version 3.40.5](https://docs.hydrolix.io/changelog/9-may-2023-v3404) or later.
The connector must be able to access the Hydrolix API (typically on port 443) and the Clickhouse Native protocol
(typically on port 8088 or 9440).

#### API
The connector talks to the Hydrolix API at query planning time using a REST client to authenticate, and to retrieve
database, table and column metadata. The connector does not use the API for query execution.

#### Query Head
The connector talks to the Hydrolix query head at query planning time using the Clickhouse JDBC driver to retrieve
partition and column index metadata. The connector does not use the query head for query execution.

## Feature Set
### Query Optimizations
The library enables support for the following query optimizations:

#### Partition Pruning
When the query has suitable predicates based on the timestamp and/or shard key, we can use them to eliminate partitions
from consideration based on each partition’s min/max timestamps and shard key. In some cases this can be extremely
effective, especially in high-selectivity queries (e.g. timestamp in a narrow range).

#### Predicate Pushdown
Suitable predicates that do simple comparisons between indexed fields and literals are evaluated by the low-level
`turbine_cmd hdx_reader` using Hydrolix indexes. Note that `hdx_reader` only applies block-level filters, so these
predicates still need to be evaluated by a query engine after scanning. Also note that due to a (hopefully) temporary
implementation restriction, only predicates on string-typed columns can be pushed down for block filtering; any other
predicates need to be evaluated by the query engine that uses this library (e.g. Spark) post-scanning.

#### Column Pruning
When queries only reference a subset of columns (e.g. `a`, `b` and `c` in `SELECT a, b WHERE c='foo'`), we only read the
columns that are referenced.

#### Aggregate Pushdown
For queries that _only_ contain the following aggregates, no other selected expressions, and no `GROUP BY` or `WHERE`
clauses, we exclusively use partition metadata to answer such queries very quickly.
* `COUNT(*)`
* `MIN(<primary key field>)`
* `MAX(<primary key field>)`

### Unsupported Features
#### Writing Data
This library only provides read-only functionality; any attempt to execute DDL or DML queries will result in an error.

#### Dictionary Tables
(see [roadmap](#dictionary-tables-1) item)

## Licenses

### Apache 2.0
The following are released under the [Apache 2.0 license](./licenses/Apache_License_2.0.txt):
* All files in [src/main/scala](./src/main/scala)
* All files in [src/main/java](./src/main/java)
* All files in [src/test/scala](./src/test/scala)
* All files in [scripts](./scripts)
* All files in [doc](./doc)
* [src/main/resources/logback.xml](src/main/resources/logback.xml)
* [build.sbt](./build.sbt)
* [project/plugins.sbt](./project/plugins.sbt)
* [project/build.properties](./project/build.properties)

### Proprietary
* All files made available in this repository that are not identified above as being licensed under the Apache 2.0
  license, including without limitation [`turbine_cmd`](./src/main/resources/linux-x86-64/turbine_cmd), may be used only
  by users that have entered into a separate written agreement with us that contains licenses to use our software and
  such use is subject to the terms of that separate written agreement.

### Other
Dependencies are used under a variety of open source licenses; see [NOTICE.md](./NOTICE.md)

## System Requirements

### JVM
This library requires a minimum Java version of 11; later versions might work. Java 8 definitely
doesn't.

### Scala
This library is cross-built for Scala 2.12 and 2.13. You don't need to install Scala yourself, the build
system will take care of it.

### Operating System
Currently, connectors based on this library will only run on recent AMD64/x86_64 Linux distros. Ubuntu 22.x, 23.x and 
Fedora 38 work fine; Ubuntu 20.x definitely doesn't work; other distros MIGHT work. It DOES NOT work on macOS, because 
it uses a native binary built from the C++ source tree, which can only target Linux at this time.

You can *build* the connector on macOS, it just won't run. Sorry.

## Building

1. Install [SBT](https://scala-sbt.org/) however you prefer. Your Linux distro might have it packaged already.
2. ```
   git clone git@github.com:hydrolix/connectors-core.git hydrolix-connectors-core && cd hydrolix-connectors-core
   ```
3. Run `sbt -J-Xmx4g +publishLocal` to compile and build the connector jar files (for Scala 2.12 and 2.13)
4. If the build succeeds, the jars can be found at:
   * [./target/scala-2.12/hydrolix-connectors-core_2.12-0.3.2-SNAPSHOT.jar](./target/scala-2.12/hydrolix-connectors-core_2.12-0.3.2-SNAPSHOT.jar).
   * [./target/scala-2.13/hydrolix-connectors-core_2.13-0.3.2-SNAPSHOT.jar](./target/scala-2.13/hydrolix-connectors-core_2.13-0.3.2-SNAPSHOT.jar).
    
## Roadmap

### Dictionary Tables
Map [Hydrolix dictionaries](https://docs.hydrolix.io/docs/dictionaries-user-defined-functions) to tables, so they can be queried more naturally using `JOIN`s

### Performance

#### Additional Aggregate Pushdown
We already run queries that only contain `COUNT(*)`, `MIN(timestamp)` and/or `MAX(timestamp)` with no `GROUP BY` or
`WHERE` purely from the catalog, with no partition scanning at all. We could add aggregate pushdown for queries with
some narrowly specified types of `GROUP BY` or `WHERE` clauses as well.

### Integrations

#### Secret Management Integration
Currently, connectors built from this library need to be directly supplied with credentials to access Hydrolix clusters 
and cloud storage. We should add integrations to retrieve credentials from various secret stores, e.g.:
* Kubernetes Secrets
* AWS Secrets Manager
* GCP Secret Manager

## Changelog

### 1.0.0
Initial public release!