## What is QPROF
``qprof`` is a Shell/SQL tool to assess and measure query perfomance in Vertica. It can be used for DQL (SELECT) or DML (INSERT/UPDATE/... ) statements. 

## How to install QPROF
### Prerequisites
You will need:
- ``qprof`` script from https://github/mfelici/qprof
- ``vsql`` (standard Vertica SQL client)
- ``bash``
- "dbadmin" access to your Vertica cluster

### Install QPROF
Just copy ``qprof`` somewhere on your system and make it executable (``chmod u+x qprof.sh``)

## How to use QPROF
### Before using QPROF
Be sure to set the correct ``vsql`` environment using the following variables:

- ``VSQL_USER``. **Please Note** user must have dbadmin ROLE to run ``qprof``
- ``VSQL_PASSWORD``
- ``VSQL_HOST``
- ``VSQL_DATABASE``
- ``VSQL_PORT``

### QPROF command line switches
- ``-f script_file`` to set the file contining the DQL/DML statement to be profiled. This option has NO default values and is alternative to ``-t ... -s ...`` (see below)
- ``-o output_file`` to set the output filename (default is ``qprof.out``)
- ``-gz`` to gzip output file (default OFF)
- ``-rp resource_pool`` to set the resource pool used to run the query (default NONE - query will be executed using the standard resource pool for dbadmin)
- `-cc`` to clear Linux ad Vertica caches before running the query (default OFF)
- ``-t transaction_id -s statement_id`` in case the query was profiled before running QPROF. This option is alternative to ``-f script_file``
- ``-u user`` to define Vertica user (default $VSQL_USER)
- ``-p password`` to define Vertica password (default $VSQL_PASSWORD)

### Examples
**Quick and easy way**. Will profile the execution of the statement in ``query.sql`` and save the output in ``qprof.out``:
```sh
$ qprof.sh -f query.sql
```
**Running qprof on a previously profiled query**. In this case qprof will gather the information without running the query. Suppose you already profiled your query
```sh
$ vsql -c "profile SELECT COUNT(*) FROM public.a"
Timing is on.
Null display is "(null)".
NOTICE 4788:  Statement is being profiled
HINT:  Select * from v_monitor.execution_engine_profiles where transaction_id=45035996273715365 and statement_id=1;
NOTICE 3557:  Initiator memory for query: [on pool general: 5891 KB, minimum: 5891 KB]
NOTICE 5077:  Total memory required by query: [5891 KB]
 COUNT 
-------
     1
(1 row)

Time: First fetch (1 row): 20.526 ms. All rows formatted: 20.717 ms
```
In this case you have to pass ``qprof`` the transaction and statement ids using the ``-t ... -s ...`` options:
```
$ ./qprof-0.5a.sh -t 45035996273715365 -s 1 -o myprof.out 
```
### Expected qprof output
This is the ``qprof`` expected output:
```
$ ./qprof-0.5a.sh -f q.sql
[qprof] Running query q.sql (result set redirected to /dev/null)
[qprof] Analyzing query profile. Output to qprof.out
    Step 00: Vertica version
    Step 01: Query text
    Step 02: Query duration
    Step 03: Query execution steps
    Step 04: Resource Acquisition
    Step 05: Query execution plan
    Step 06: Query plan profile
    Step 07: Query consumption
    Step 08: Elapsed & memory allocated by node, path_id and activity
    Step 09: Elapsed, exec_time and I/O by node, activity & path_id
    Step 10: Query events
    Step 11: Suggested Action Summary
    Step 12: CPU Time by node and path_id
    Step 13: Threads by node and path_id
    Step 14: Query execution report
    Step 15: Transaction locks
    Step 16: Projection Data Distribution
    Step 17: Query execution profile counters extraction in CSV format
    Step 18: Getting Vertica non-default configuration parameters
    Step 19: Getting RP configuration
    Step 20: Getting Cluster configuration
    Step 21: Getting Projection Definition and Statistics
```
### Warning
When we ask ``qprof`` to profile the query  with ``-f script_file`` the **result set in output** will be redirected to ``/dev/null`` **of the system where qprof is executed**.

## How to interpret QPROF's output
Not in scope for this guide (at this point in time). 
