RockDB proof of concept
=======================

Usage
---

### Executing tests
`sbt test`

### Starting the benchmark
`sbt "run <DBPath> <N> <M>"`

Where DBPath is a path to test db (will create if missing). N is a number of initial dummy records to put in DB. M of them will be created with same int pair prefix.

