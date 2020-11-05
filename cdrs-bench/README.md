# cdrs-benchmark


## About benchmark

Benchmark will execute the specified amount of CQL statements. 

The time used by the benchmark will be measured using Linux's command `time`.


## Parameters

There are few parameters defined before running the benchmark. Most important ones are:

- number of operations
- concurrency - how many operations can run at the same time
- workload (write, read or mixed - specified below)

Other parameters specify the configuration of the database, and how the tasks are handled.

- replication factor - keyspace parameter, specified below.
- consistency level - consistency level used for reads and writes.
- compression - compression algorithm used in the database.

### Schema

Benchmark should operate on keyspace created with this schema:

    CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': (replication factor)}

where (replication factor) is a parameter passed to the benchmark.

All of the operations are running on the same table with this schema:

    CREATE TABLE ks.t (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)

#### Creating schema

Running benchmark with parameter --prepare prepares schema.

This allows to only measure the time spent on the request.

### Workloads

Let `T` = number of operations.

Benchmark can run in one of the three workloads:

- write - each request is a write:

      INSERT INTO ks_rust_scylla_bench.t (pk, v1, v2) VALUES (?, ?, ?)

  Where the parameters `pk`, `v1` i `v2` for `pk` should be `2 * pk` i `3 * pk`.

  Here, the `pk` should be `pk ∈ [0, T)`. The order doesn't matter, there should be written for each possible value of `pk`.

- read - each request is a write::

      SELECT v1, v2 FROM ks_rust_scylla_bench.t WHERE pk = ?

  Here, the `pk` should be `pk ∈ [0, T)`. The order doesn't matter, there should be read for each possible value of `pk`.

- mixed - each operation should be either write or read specified above.

