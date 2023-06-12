# FDW for db721

This repo will describe how to build a foreign data wrapper(FDW) in rust using [pgxr](https://github.com/tcdi/pgrx) with the goal do to do project 1 in [CMU 15-721 Advanced databases](https://15721.courses.cs.cmu.edu/spring2023/project1.html), which is an amazing course. 

# What is a Foreign data wrapper(FDW)?

Postgres exposes a standard way to interact with foreign data sources such as Parquet, Orc or any external data source of interest. This is done through what is called [hooks](https://github.com/taminomara/psql-hooks). In short hooks allow any one to extend the functionality through adding functional code at runtime. Hooks are a pointer to a function of a specific type, which is initially set to null. The documentation on FDW can be found [here](https://www.postgresql.org/docs/current/postgres-fdw.html). 

# How to run it

```bash
cargo pgrx run
```

```sql
CREATE FOREIGN TABLE db721_fdw_table (
    identifier integer
    , farm_name varchar
    , weight_model varchar
    , sex varchar
    , age_weeks real
    , weight_g real
    ,notes varchar)
SERVER db721_server OPTIONS (
    filename '/data/data-chickens.db721'
    , table 'Farm');
```

# How to get the logs

To tail the logs: 
```bash
tail -f  ~/.pgx/15.log
```

 make sure to use the correct postgres version(as used for test instance)

# Resources

[Really good blog post that heavily inspired this one, FDW using c](https://www.dolthub.com/blog/2022-01-26-creating-a-postgres-foreign-data-wrapper/)
[Postgres docs, great to add but not enough on its own for me](https://www.postgresql.org/docs/9.2/fdw-callbacks.html)
[Superbase wrappers, great to have a implementation to compare with and see the correct pgxr syntax](https://github.com/supabase/wrappers)
[Mapping of data types between rust and postgres](https://docs.rs/sqlx/latest/sqlx/postgres/types/index.html)

# Development

The main goal, exepct for having a functional foreign data wrapper is to implement: 
- Predicate pushdown: skip accessing blocks if the block statistics don't pass the predicate
- Projection pushdown: only access the columns that the query requests

Since I/O is the main bottle neck for OLAP systems in general and specifically this case when there is no cache what so ever, it should be faster if we can skip blocks or columns.

# How does it really work. 

The first step of a FDW is the `control` file which describes the postgres extensions and metadata about it. Using [pgrx](https://github.com/tcdi/pgrx) this is created automatically. This will be loaded when we run `CREATE EXTENSION db721_fdw`. A foreign data wrapper in postgres needs to have two things: 
- A `handler`, which will return a struct of function pointers that will implement the FDW api. These function pointers will then be used by postgres when executing the FDW. 
- A `validator`, optional wich will be called with the options for validation. 

The first step in understanding how the a FDW works is to look at the struct returned by the `handler`, the definition can be found [here](https://github.com/postgres/postgres/blob/REL_14_STABLE/src/include/foreign/fdwapi.h#L194). However most will not have to be implemented. We will in this case focuse on the following: 

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

the next part of the implementation will be broken up based upon the [path of a query](https://www.postgresql.org/docs/14/query-path.html) in postgres. The steps a query has to pass to obtain results in postgres(FDW also obey to this):

1) Connection from an application program to the Postgres server. A query is transmitted to the sever and waits for a results to be sent back by the server. 
2) Syntax is checked byt the parser and creates a query tree. 
3) The rewrite system looks for rules to apply to the query tree. One examples is this it the realization of views. A query against a view is converted to a query against the based table based upon the view definition.
4) The planner/Optimizer takes the query tree and creates a query plan base upon which the execution will be made. 
5) The executor recursively goes through the plan tree and retrieve the rows represented by the plan.

### Path and plans

The planing phase for a FDW consists of `GetForeignRelSize`, `GetForeignPaths` and `GetForeignPlan` and is executed in that order. The purpose of each step is the following: 
 - [GetForeignRelSize](https://www.postgresql.org/docs/9.2/fdw-callbacks.html) should update the baserel rows and potentially also width. This will later be used by the optimizer. If not the correct values are set it could lead to potential miss optimization. This is also the place where we will handle the [FDW options](https://www.postgresql.org/docs/current/postgres-fdw.html#id-1.11.7.47.11). In order to send information to the next step of the planing we will store the information inside `ForeignScan node` using the `void *fdw_private` that is provided by postgres. `fdw_private` will not be touched by anything else and is it is free to store anything of interest within it. 
 - [GetForeignPaths](https://www.postgresql.org/docs/9.2/fdw-callbacks.html) describes the paths to access the data. In our case there will only be one. Each paths should include a cost estimate. This will be used by the optimizer to find the optimal path. This is set on the `baserel->pathlist`.
 - [GetForeignPlan](https://www.postgresql.org/docs/9.2/fdw-callbacks.html) ir responsible for creating a `ForeignScan *` for the given `ForeignPath *`. As input the optimizer has selected the best access path(in our case there will only be one). Here we will also be able to pass information on to the next group of steps of the processing, [Begin, Iterate, End](# Begin, Iterate, End) where we will execute the plan, using the `void *fdw_state`. However `void *fdw_state` is a list so if the information from `void *fdw_private` should be propagate id needs to be reformated.

### Begin, Iterate, End

When a plan is created the next step is to execute on the plan, this happens in three steps
- [BeginForeignScan](https://www.postgresql.org/docs/9.2/fdw-callbacks.html) should do any initialization that is needed before the scan. Information from the planing state can be accessed through `ForeignScanState` and the underlying `ForeignScan` which contains `fdw_private` which is provided through the previous planing and specifically `GetForeignPlan`. To pass information further the `fdw_state` on the `ForeignScanState` can be used. 
- [IterateForeignScan](https://www.postgresql.org/docs/9.2/fdw-callbacks.html) should fetch one row(only), if all data is returned NULL should be returned marking the end. `ScanTupleSlot` should be used for the data return. Either a physical or virtual tuple should be returned. The rows returned must match the table definition of the FDW table. 
-[EndForeignScan](https://www.postgresql.org/docs/9.2/fdw-callbacks.html) end the scan and release resources. It is normally not important to release palloc'd memory, but for example open files and connections to remote servers should be cleaned up.
### FDW options

When creating a FDW table there is a possibility to add [some options](https://www.postgresql.org/docs/current/postgres-fdw.html#id-1.11.7.47.11). This will be utalised in our case to know where the file where the information is stored. 


### Walk over the implementation. 

The first step in building the FDW is to create the handler function. 

```rust 
#[pg_extern]
unsafe fn db721_fdw_handler() -> fdw_handler {
    debug1!("HelloFdw: hello_fdw_handler");

    let mut fdwroutine =
        pgrx::PgBox::<pg_sys::FdwRoutine>::alloc_node(pg_sys::NodeTag_T_FdwRoutine);

    // Set callback functions.
    fdwroutine.GetForeignRelSize = Some(get_foreign_rel_size);
    fdwroutine.GetForeignPaths = Some(get_foreign_paths);
    fdwroutine.GetForeignPlan = Some(get_foreign_plan);
    fdwroutine.ExplainForeignScan = Some(explain_foreign_scan);
    fdwroutine.BeginForeignScan = Some(begin_foreign_scan);
    fdwroutine.IterateForeignScan = Some(iterate_foreign_scan);
    fdwroutine.ReScanForeignScan = Some(re_scan_foreign_scan);
    fdwroutine.EndForeignScan = Some(end_foreign_scan);
    fdwroutine.AnalyzeForeignTable = Some(analyze_foreign_table);
    fdwroutine
}

```
As we can notice it is marked with `#[pg_extern]` which is a attribute marco from the `pgxr` crate for wrapping rust functions with boiler plate for defining and calling conventions between Postgres and Rust. 

The function `fdw_handler` struct has the postgres hooks that are `options` with the defualt is None(Null in Postgres). The next step is to implement these functions one by one. Going down this rabbit hole myself I hope to cover the things I struggled with, both in terms of just getting it to work but also small key points of how postgres works.

Going over the implementation in the order of execution the first step is to implement the `GetForeignRelSize` function. 

```rust
unsafe extern "C" fn get_foreign_rel_size(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
) {
    ...
}
```

As mentioned above, the key goal for `GetForeignRelSize` is to set the nbr of rows(and potentially width) for the optimizer as well as handle the options. However in this first optimization(later blog post will dive down and try to optimize it). We will just set the nbr of rows to 10(arbitrary low nbr).

Postgres offers the possibility to send information between the hooks using the `fdw_private`. We will use this to store the information, during `GetForeignRelSize` we will extract the following and store: 
- selected columns
- conditions of the query
- options

The first step is to create the state where we can store the information using`PgBox` which is a heap-allocated pointer that is allocated by Postgres's memory allocation function. 

```rust
    let mut state = pgrx::PgBox::<FdwState>::alloc0();
```
The state is a rust struct of the `FdwState` which is defined for our use case as:

```rust
pub struct FdwState {
    rownum: usize,
    opts: HashMap<String, String>,
    metadata: Option<Metadata>, 
    filters: HashMap<String, Filter>,
    nulls: Vec<Vec<bool>>,
    tmp_ctx: PgMemoryContexts,
    cols: Vec<ColumnMetadada>,
    tuples: Vec<Vec<Cell>>,
    natts: usize, 
}
```

Some of these fields might not make sense now but will(hopefully) when we go over the different part of the FDW implementation. You are free to add anything of interest or needed for your wrapper. The extracted values(selected columns, conditions, options) are stored in the `state` as

```rust
    state.cols = cols;
    state.opts = ret; 
    state.filters = filters;
    (*baserel).fdw_private = state.into_pg() as *mut std::ffi::c_void;
```

When done with the `GetForeignRelSize` the next step is to continue with the `GetForeignPaths`. We will not spend to much focuse on `GetForeignPaths` since we only have one execution path and thus will just return one plan. Similar for `GetForeignPlan` we will just generate one plan. If several plans where available the optimizer would select the one with the lowest cost, however since we only have one it does not matter to much. `ExplainForeignScan` is also a dummy implementation that is left for future work. The next step of importance is the `BeginForeignScan` which should perform any initialization needed before the scan can start, but not start executing the actual scan(this should be done during the first call to the next step, `IterateForeignScan`). One of the key things we will do during the `BeginForeignScan` is to forward the information from the planing steps to the execution steps. Where the information we set in `GetForeignRelSize` in `fdw_private` is extracted from the `ForeignScanState` and the actual plan to a new `PgBox` that is stored in side the `fdw_state`. This actually took some time for me to get my head around mostly due to mixing up the `fdw_private` for the plan with the `fdw_state` of the `ForeignScan`. However it is just postgres internal how information is passed between the different steps, and we will use the same data type(`FdwState`) to hold the information.  The key part of the implementation is: 

```rust

#[pg_guard]
unsafe extern "C" fn begin_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    eflags: ::std::os::raw::c_int,
) {
    ...

    // We get the state from the plan and convert it to.
    // First we can get the scan state. 
    let scan_state = (*node).ss;
    let plan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
    let list = PgList::<pg_sys::Const>::from_pg((*plan).fdw_private );
    let cst = list.head().unwrap();
    let ptr = i64::from_datum((*cst).constvalue, (*cst).constisnull).unwrap();
    let mut state:PgBox::<FdwState> = PgBox::from_pg(ptr as _);
    ...
    // Save the state
    (*node).fdw_state = state.into_pg() as *mut std::ffi::c_void;
}
```

The most important part of the FDW and the meat of the implementation(at least for this implementation) is the `IterateForeignScan` where we will read the actual data. `IterateForeignScan` is invocated each until a null row is returned indicating that all rows are fetched. This is handled by checking the current row number which is increase for each iteration, checking with the total nbr of rows. 



```rust 
#[pg_guard]
unsafe extern "C" fn iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {

    ...
    
    let mut state: PgBox<FdwState> = PgBox::<FdwState>::from_pg((*node).fdw_state as _);

    ...
    if (*state).rownum >= (*state).tuples.len()   { // https://www.highgo.ca/2021/09/03/implement-foreign-scan-with-fdw-interface-api/
        (*(*slot).tts_ops).clear.expect("missing")(slot);
        return slot;
    }

    ...

}
```

. The first invocation will in our implementation fetch all the data and store it returning a new row for each iteration.

```rust
#[pg_guard]
unsafe extern "C" fn iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {

    ...

    let mut state: PgBox<FdwState> = PgBox::<FdwState>::from_pg((*node).fdw_state as _);

    ...

    if (*state).rownum == 0 {
        let meta = (*state).metadata.clone().unwrap();
        let filters = &(*state).filters;
        let data = meta.fetch_data(filters.clone(), &state.cols);
        let (values, mask) = meta.tuples(&data, (*state).cols.clone(), state.natts);
        (*state).tuples = values;
        (*state).nulls = mask;
    }

    ...

}
```

As describe a new row is returned for each invocation, null values are marked using the `tts_isnull` state on the `TupleTableSlot` that is returned.


```rust 
#[pg_guard]
unsafe extern "C" fn iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    let slot = (*node).ss.ss_ScanTupleSlot;

    ...

    let idx = (*state).rownum.clone();
    let mut row:Vec<Datum> = (*state).tuples[idx].iter().map(|val| val.clone().into_datum().unwrap()).collect();
    (*slot).tts_values = row.as_mut_ptr();
    (*slot).tts_isnull = (*state).nulls[idx].as_mut_ptr();
    pg_sys::ExecStoreVirtualTuple(slot);
    state.rownum += 1;
    old_ctx.set_as_current();
    return slot
```

The main logic in reading the data comes down to parsing the data format, however this is not relevant for implementing a FDW in general and we will not deep dive in to this in this blog post, however there will be a follow up where we try to optimize the code and will take a closer look. `ReScanForeignScan` restart the scan from the beginning and we thus need to se the row nbr to 0 again. `AnalyzeForeignTable` is set to just return false since we will not return any statistics for the FDW at this stage(could also be left to false.)

### How to set up a profile and make sure the grading works. 

## Tricks
- export CARGO_NET_GIT_FETCH_WITH_CLI=true
- export PGRX_BUILD_VERBOSE=true

# Author

Dreaming about becoming a database hacker, one commit a time. 

# Project
- Duckdb vector seach 
- Array compression will matter when it comes to read from disc
- How do I make it concurrent
- I need to learn how to writt c++ it seems like
- Lets do annoy and knn here, should be aweeeesooome
- https://huggingface.co/spaces/SteveDigital/free-fast-youtube-url-video-to-text-using-openai-whisper/blob/main/app.py