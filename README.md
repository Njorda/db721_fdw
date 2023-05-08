# FDW for db721

This repo will describe how to build a forigen data wrapper in rust using [pgxr](https://github.com/tcdi/pgrx) with the goal do to do project 1 in [CMU 15-721 Advanced databases](https://15721.courses.cs.cmu.edu/spring2023/project1.html), which is an amazing course. 

# What is a Foreign data wrapper(FDW)?

Postgres exposeses a standard way to interact with forgein data sources such as Parquet, Orc or any external data source of interest. This is done through what is called [hooks](https://github.com/taminomara/psql-hooks). In short hooks allow any one to extend the functionality through adding functional code at runtime. Hooks are a pointer to a function of a specific type, which is initially set to null. The documentation on FDW can be found [here](https://www.postgresql.org/docs/current/postgres-fdw.html). 



# How to run it

```bash
cargo pgrx run
```

```sql
DROP EXTENSION db721_fdw CASCADE;
CREATE EXTENSION db721_fdw;
CREATE FOREIGN DATA WRAPPER db721_fdw HANDLER db721_fdw_handler VALIDATOR db721_fdw_validator;
CREATE SERVER db721_server FOREIGN DATA WRAPPER db721_fdw;
CREATE FOREIGN TABLE db721_fdw_table (id text, data int) SERVER db721_server;
```

# How to get the logs

To tail the logs: 
```bash
tail -f  ~/.pgx/15.log
```

 make sure to use the correct postgres version(as as used for test instance)

 # Development

- Add the options stuff now
- Read the corresponding files
- Then do the Predicate pushdown
- Then do the Projection pushdown 

## fdw_private 
Is used to pass information between the different executions steps. This is where we wills store information such as:
- file of interest from the options
- filters used to only read certain blocks
- filters used to only read certain columns

# Resources

[Really good blog post that heavily inspired this one, FDW using c](https://www.dolthub.com/blog/2022-01-26-creating-a-postgres-foreign-data-wrapper/)
[Postgres docs, great to add but not enough on its own for me](https://www.postgresql.org/docs/9.2/fdw-callbacks.html)
[Superbase wrappers, great to have a implementation to compare with and see the correct pgxr syntax](https://github.com/supabase/wrappers)


# How does it really work. 

The first step of a FDW is the `control` file which describes the postgres extensions and meta data about it. Using [pgrx](https://github.com/tcdi/pgrx) this is created automatically. This will be loaded when we run `CREATE EXTENSION db721_fdw`. A foreign data wrapper in postgres needs to have two things: 
- A `handler`, which will return a struct of function pointers that will implement the FDW api. These function pointers will then be used by postgres when executing the FDW. 
- A `validator`, optional wich will be called with the options for validation. 

The first step in understanding how the a FDW works is to look at the struct returned by the `handler`, the definition can be found [here](https://github.com/postgres/postgres/blob/REL_14_STABLE/src/include/foreign/fdwapi.h#L194). Dont be frigthen by the sheer nbr of function points avilable most will not be needed, at lest to start with. We will in this case focuse on the following: 

```rust
fdwroutine.GetForeignRelSize = Some(hello_get_foreign_rel_size);
fdwroutine.GetForeignPaths = Some(hello_get_foreign_paths);
fdwroutine.GetForeignPlan = Some(hello_get_foreign_plan);
fdwroutine.ExplainForeignScan = Some(hello_explain_foreign_scan);
fdwroutine.BeginForeignScan = Some(hello_begin_foreign_scan);
fdwroutine.IterateForeignScan = Some(hello_iterate_foreign_scan);
fdwroutine.ReScanForeignScan = Some(hello_re_scan_foreign_scan);
fdwroutine.EndForeignScan = Some(hello_end_foreign_scan);
fdwroutine.AnalyzeForeignTable = Some(hello_analyze_foreign_table);
```

the next part of the implmentation will be broken up based upon the [path of a query](https://www.postgresql.org/docs/14/query-path.html) in postgres. The steps a query has to pass to obtain results in postgress(FDW also obey to this):

1) Connection from an application program to the Postgres server. A query is transmitted to the sever and waits for a results to be sent back by the server. 
2) Syntax is checked byt the parser and creates a query tree. 
3) The rewrite system looks for rules to apply to the query tree. One examples is this it the realization of views. A query against a view is converted to a query against the based table based upon the view definition.
4) The planner/Optimizer takes the query tree and creates a query plan base upon which the execution will be made. 
5) The executor recursivly goes through the plan tree and retrives the rows represented by the plan.

### Path and plans

The planing phase consists of GetForeignRelSize, GetForeignPaths and GetForeignPlan and is executed in that order. The purpose of each step is the following: 
 - [GetForeignRelSize](https://www.postgresql.org/docs/9.2/fdw-callbacks.html) should update the baserel rows and potentially also width. This will later be used by the optimizer. If not the correct values are set it could lead to potential miss optimization. This is also the place where we will handle the [FDW options](https://www.postgresql.org/docs/current/postgres-fdw.html#id-1.11.7.47.11). In order to send information to the next step of the planing we will store the information inside `ForeignScan node` using the `void *fdw_private` that is provided by postgres. `fdw_private` will not be touched by anything else and is it is free to store anything of interest within it. 
 - GetForeignPaths](https://www.postgresql.org/docs/9.2/fdw-callbacks.html) describes the paths to access the data. In our case there will only be one. Each paths should include a cost estimate. This will be used by the optimizer to find the optimal path. This is set on the `baserel->pathlist`.
 - [GetForeignPlan](https://www.postgresql.org/docs/9.2/fdw-callbacks.html) ir responsible for creating a `ForeignScan *` for the given `ForeignPath *`. As input the optimizer has selected the best access path(in our case there will only be one). Here we will also be able to pass information on to the next group of steps of the processing, [Begin, Iterate, End](# Begin, Iterate, End) where we will execute the plan, using the `void *fdw_state`. However `void *fdw_state` is a list so if the information from `void *fdw_private` should be propagate id needs to be reformated.

### Begin, Iterate, End

When a plan is created the next step is to execute on the plan, this happens in three
- [BeginForeignScan](https://www.postgresql.org/docs/9.2/fdw-callbacks.html) should do any initialization that is needed before the scan. Information from the planing state can be accessed through `ForeignScanState` and the underlying `ForeignScan` which contains `fdw_private` which is provided through the previous planing and specifically `GetForeignPlan`. To pass information further the `fdw_state` on the `ForeignScanState` and `fdw_state` can be used. 
- [IterateForeignScan](https://www.postgresql.org/docs/9.2/fdw-callbacks.html) should fetch one row(only), if all data is returned NULL should be returned marking the end. `ScanTupleSlot` should be used for the data return. Either a physical or virtual tuple should be returned. The rows returned must match the table definition of the FDW table. If projection pushdown is used these values should be set to NULL. 
-[EndForeignScan](https://www.postgresql.org/docs/9.2/fdw-callbacks.html)

# Current work

Pass on the options that we can set. 

### FDW options

When creating a FDW table there is a possibility to add [some options](https://www.postgresql.org/docs/current/postgres-fdw.html#id-1.11.7.47.11).


### How to set up a profile and make sure the grading works. 

## Tricks
- export CARGO_NET_GIT_FETCH_WITH_CLI=true
- export PGRX_BUILD_VERBOSE=true