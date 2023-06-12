use parser::db721::{Values, FilterType, ColumnMetadada, Cell};
use pgrx::*;
use std::os::raw::c_int;
use std::collections::HashMap;
use std::ffi::CStr;
use std::ptr;
use std::str;
use pgrx::{
    pg_sys::{self, Datum, Oid},
    FromDatum, IntoDatum, PgBuiltInOids, PgOid,
};

use crate::parser::db721::{read_metadata, Metadata, Filter};
pub mod parser;

pg_module_magic!();


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




impl FdwState {
    pub unsafe fn new() -> Self {
        Self { rownum: 0 
            , opts: HashMap::new()
            , metadata: None
            , filters: HashMap::new()
            , nulls: Vec::new()
            , cols: Vec::new()
            , natts: 0
            , tmp_ctx: PgMemoryContexts::CurTransactionContext
            .switch_to(|_| PgMemoryContexts::new("Wrappers temp data")),
            tuples: Vec::new()
        }
    }
}

// HACK: By making an alias of this type, pgrx-macro will use this name
// which is the postgres specified name.
#[allow(non_camel_case_types)]
type oid = pg_sys::Oid;
#[allow(non_camel_case_types)]
type fdw_handler = pgrx::PgBox<pg_sys::FdwRoutine, pgrx::AllocatedByRust>;

#[pg_extern]
fn db721_fdw_validator(_options: Vec<String>, _oid: oid) {
    debug1!("HelloFdw: hello_fdw_validator");
}

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


pub(crate) unsafe fn extract_from_op_expr(
    _root: *mut pg_sys::PlannerInfo,
    baserel_id: pg_sys::Oid,
    baserel_ids: pg_sys::Relids,
    expr: *mut pg_sys::OpExpr,
)-> Option<Filter>{
    let args: PgList<pg_sys::Node> = PgList::from_pg((*expr).args);
    // only deal with binary operator
    let opno = (*expr).opno;
    let op = get_operator(opno);
    log!("Operator: {:?}, ", pgrx::name_data_to_str(&(*op).oprname).to_string());


    log!("The list lenght is: {}, ",args.len());

    // only deal with binary operators
    // Operators that are working on one column.
    if args.len() != 2 {
        return None;
    }

    // Continue here tomorrow
    // This is the place
    if let (Some(mut left), Some(mut right)) = (args.get_ptr(0), args.get_ptr(1)){

        // swap operands if needed
        if is_a(right, pg_sys::NodeTag_T_Var)
            && !is_a(left, pg_sys::NodeTag_T_Var)
            // https://www.postgresql.org/docs/current/catalog-pg-operator.html, Commutator of this operator (zero if none)
            // https://www.postgresql.org/docs/current/xoper-optimization.html
            // if Commutator is 0 then swapping is not possible since there is not opposite operator
            // if it is zero then there is no operator that represent the changed order!
            && (*op).oprcom.as_u32() != 0
        {
            std::mem::swap(&mut left, &mut right);
        }

        // We check here that the left side is a variable and the right is a constant. 
        if is_a(left, pg_sys::NodeTag_T_Var) && is_a(right, pg_sys::NodeTag_T_Const) {
            // Then we do some casting magic here...
            let left = left as *mut pg_sys::Var;
            let right = right as *mut pg_sys::Const;

            // https://www.postgresql.org/docs/7.3/parser-stage.html
            // The field varattno gives the position of the attribute within the relation
            // Check if varno part of baserel_ids
            // baserel_ids table id I guess
            // https://doxygen.postgresql.org/pathnodes_8h_source.html
            if pg_sys::bms_is_member((*left).varno as c_int, baserel_ids) && (*left).varattno >= 1 {
                // Here we get the attribute name
                let field = pg_sys::get_attname(baserel_id, (*left).varattno, false);
                // here we get the constant value from postgress
                // datum in postgres is just a data type for holding information. 

                // Need to write this function and we dotn have cell I belive we want filter instead
                let value = from_polymorphic_datum(
                    (*right).constvalue,
                    (*right).constisnull,
                    (*right).consttype,
                );



                if let Some(value) = value {
                    let filter = match pgrx::name_data_to_str(&(*op).oprname){
                        "=" => FilterType::Equal,
                        ">" => FilterType::Greater,
                        "<" => FilterType::Less,
                        _ => {
                            return None},
                    };

                    let qual = Filter {
                        column: CStr::from_ptr(field).to_str().unwrap().to_string(),
                        filter: filter,
                        value: value,
                    };
                    return Some(qual);
                } else {
                    todo!()
                }
            }
        }
    }
    return None

    //The args will hold the arguments for the operator, I just don't get why we collect them differently...
    // we can now get it out if we like to. 
    // WOOOP WOOOP
}


// Funcion that takes a postgres datum
// https://stackoverflow.com/questions/53543909/what-exactly-is-datum-in-postgresql-c-language-functions

unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, typoid: Oid) -> Option<Values>{
    if is_null {
        return None;
    }
    match PgOid::from(typoid) {
        PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
            None
        }
        PgOid::BuiltIn(PgBuiltInOids::CHAROID) => {
            Some(Values::Int(i8::from_datum(datum, false).unwrap().into()))
        }
        PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
            Some(Values::Int(i16::from_datum(datum, false).unwrap().into()))
        }
        PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
            Some(Values::Float(f32::from_datum(datum, false).unwrap().into()))
        }
        PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
            Some(Values::Int(i32::from_datum(datum, false).unwrap().into()))
        }
        PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
            Some(Values::Float(f64::from_datum(datum, false).unwrap().into()))
        }
        PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
            Some(Values::Int(i64::from_datum(datum, false).unwrap().into()))
        }
        PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
            None
        }
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID) => {
            None
        }
        PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
            None
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
            None
        }
        PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => {
            None
        }
        _ => None,
    }
}


//https://dba.stackexchange.com/questions/232870/are-cataloge-and-relation-cache-in-postgresql-per-connection-or-global-to-the-se
//we get this from cache of the query parameters, I just dont get why
//Is this how postgres really works 
//Very hard to learn
//wow it works in the ned
// we get the values form the cache for the query. 
pub(crate) unsafe fn get_operator(opno: pg_sys::Oid) -> pg_sys::Form_pg_operator {
    let htup = pg_sys::SearchSysCache1(
        pg_sys::SysCacheIdentifier_OPEROID.try_into().unwrap(),
        opno.try_into().unwrap(),
    );
    if htup.is_null() {
        pg_sys::ReleaseSysCache(htup);
        pgrx::error!("cache lookup operator {} failed", opno);
    }
    let op = pg_sys::GETSTRUCT(htup) as pg_sys::Form_pg_operator;
    // Since we do this does that mean that the next time we get the next one?
    // So we can get all the operators, incase there are multiple once?
    pg_sys::ReleaseSysCache(htup);
    op
}

#[pg_guard]
unsafe extern "C" fn get_foreign_rel_size(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
) {

    (*baserel).rows = 10.0;
    (*baserel).fdw_private = std::ptr::null_mut();

    // Allocate a box of memory in postrgress for the  FdwState
    let mut state = pgrx::PgBox::<FdwState>::alloc0();

    // Get information about the conditions of the query
    let conds = PgList::<pg_sys::RestrictInfo>::from_pg((*baserel).baserestrictinfo);
    // Create a hashmap to store the conditions per column
    let mut filters:HashMap<String, Filter> = HashMap::new();
    for cond in conds.iter_ptr() {
        let expr = (*cond).clause as *mut pg_sys::Node;
        // Check that the node is of the type, OpExpr(xpression that uses an operator to compare one or two values of a given type)
        if is_a(expr, pg_sys::NodeTag_T_OpExpr) {
            // Get the operator type and the values of the operation
            let filter = extract_from_op_expr(root, foreigntableid, (*baserel).relids, expr as _);
            // Handle empty since we have an optional
            if let Some(filter) = filter{
                // Add filter condition
                filters.insert(filter.column.clone(),filter);
            }
        } else {
            continue;
        };
    }

    // Handle the foreign data wrapper arguments. First step is to initialize the hash map for all the options
    let mut ret = HashMap::new();
    // Get the foreign table based upon the id
    let ftable = pg_sys::GetForeignTable(foreigntableid);
    // Fetch the options as a list from postgres
    let options: PgList<pg_sys::DefElem> = PgList::from_pg((*ftable).options);
    // Loop over the options to collect them one at a time. 
    for option in options.iter_ptr() {
        let name = CStr::from_ptr((*option).defname);
        let value = CStr::from_ptr(pg_sys::defGetString(option));
        // Insert in to the hash map to store each of them. 
        ret.insert(
            name.to_str().unwrap().to_owned(),
            value.to_str().unwrap().to_owned(),
        );
    }

    // Since we are relying on the option "filename" for the file we check this specifically
    if ret.contains_key("filename"){
        let Some(filename) = ret.get("filename") else {todo!()};
        // We store the filename as metadata.
        state.metadata = Some(read_metadata(filename.to_string()));
        log!("Metadata is set");
    }


    // Handle the selected columns
    let mut cols = Vec::new();
    // Create list mut pointer to list where we can store the selected columns
    let mut col_vars: *mut pg_sys::List = ptr::null_mut();
    // This is based upon: 
    // baserel->reltarget->exprs can be used to determine which columns need to be fetched.
    // But note that it only lists columns that have to be emitted by the ForeignScan plan node
    // not columns that are used in qual evaluation but not output by the query. However we leave this
    // for postgres to handle and not the FDW in this case(except for on the block level). 
    let tgt_list: PgList<pg_sys::Node> = PgList::from_pg((*(*baserel).reltarget).exprs);
    for tgt in tgt_list.iter_ptr() {
        let tgt_cols = pg_sys::pull_var_clause(
            tgt,
            (pg_sys::PVC_RECURSE_AGGREGATES | pg_sys::PVC_RECURSE_PLACEHOLDERS)
                .try_into()
                .unwrap(),
        );
        col_vars = pg_sys::list_union(col_vars, tgt_cols);
    }

    // Reformating, not sure why this is needed to be honest
    let col_vars: PgList<pg_sys::Var> = PgList::from_pg(col_vars);
    // Loop over the pointers and get the variable information
    for var in col_vars.iter_ptr() {
        // Range table entry is the statement given after the from. 
        let rte = pg_sys::planner_rt_fetch((*var).varno as u32, root);
        // The attno states the position a value should have in the tuples we return, based upon the statement. 
        let attno = (*var).varattno;
        // The attribute name
        let attname = pg_sys::get_attname((*rte).relid, attno, true);
        // Check that the attribute name is null, not sure why this could happen though ...
        if !attname.is_null() {
            // generated column is not supported
            if pg_sys::get_attgenerated((*rte).relid, attno) > 0 {
                continue;
            }
            
            // Object identifiers (OIDs) are used internally by PostgreSQL as primary keys for various system tables.
            let type_oid = pg_sys::get_atttype((*rte).relid, attno);
            // Adding the column type
            cols.push(ColumnMetadada {
                name: CStr::from_ptr(attname).to_str().unwrap().to_owned(),
                num: attno as usize,
                type_oid,
            });
        }
    }

    state.cols = cols;
    state.opts = ret; 
    state.filters = filters;
    // baserel->fdw_private is a void pointer that is available for FDW planning functions to store information
    // relevant to the particular foreign table. The core planner does not touch it except to initialize it to NULL 
    // when the RelOptInfo node is created. It is useful for passing information forward from GetForeignRelSize to 
    //GetForeignPaths and/or GetForeignPaths to GetForeignPlan, thereby avoiding recalculation.
    (*baserel).fdw_private = state.into_pg() as *mut std::ffi::c_void;
}


#[pg_guard]
unsafe extern "C" fn get_foreign_paths(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    debug1!("HelloFdw: hello_get_foreign_paths");
    //This function must generate at least one access path (ForeignPath node) for a scan on the foreign table and 
    // must call add_path to add each such path to baserel->pathlist. It's recommended to use create_foreignscan_path 
    // to build the ForeignPath nodes. The function can generate multiple access paths, e.g., a path which has valid 
    // pathkeys to represent a pre-sorted result. Each access path must contain cost estimates, and can contain any 
    // FDW-private information that is needed to identify the specific scan method intended.
    // We generate only one path in this case, with a dummy cost value(will not matter since there is only one
    // if multiple the cheapest one would be selected).
    pg_sys::add_path(
        baserel,
        create_foreignscan_path(
            root,
            baserel,
            std::ptr::null_mut(),
            (*baserel).rows,
            10.0,
            1000.0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        ),
    )
}


unsafe extern "C" fn get_foreign_plan(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
    _best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    mut scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    debug1!("HelloFdw: hello_get_foreign_plan");

    let state = PgBox::<FdwState>::from_pg((*baserel).fdw_private as _);

    // Generate the forigenscan based upon the best path, in this case there will only be one, so there is no best, 
    // based upon the get_foreign_paths.
    // This is needed in order to pass on the filters. 
    scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);
    let mut ret = PgList::new();
    let val = state.into_pg() as i64;
    let cst = pg_sys::makeConst(
        pg_sys::INT8OID,
        -1,
        pg_sys::InvalidOid,
        8,
        val.into_datum().unwrap(),
        false,
        true,
    );
    ret.push(cst);

    pg_sys::make_foreignscan(
        tlist,
        scan_clauses,
        (*baserel).relid,
        std::ptr::null_mut(),
        ret.into_pg(),         
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        outer_plan,
    )
}

#[pg_guard]
extern "C" fn explain_foreign_scan(
    _node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    // This function is not needed and could be null ...
    let hello = std::ffi::CString::new("Hello").expect("invalid");
    let hello_explain = std::ffi::CString::new("Hello Explain Value").expect("invalid");
    unsafe { pg_sys::ExplainPropertyText(hello.as_ptr(), hello_explain.as_ptr(), es) }
}

#[pg_guard]
unsafe extern "C" fn begin_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    eflags: ::std::os::raw::c_int,
) {
    log!("HelloFdw: hello_begin_foreign_scan");

    if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32 != 0 {
        return;
    }

    // We get the state from the plan and convert it to.
    // First we can get the scan state. 
    let scan_state = (*node).ss;
    let plan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
    let list = PgList::<pg_sys::Const>::from_pg((*plan).fdw_private );
    let cst = list.head().unwrap();
    let ptr = i64::from_datum((*cst).constvalue, (*cst).constisnull).unwrap();
    let mut state:PgBox::<FdwState> = PgBox::from_pg(ptr as _);
    state.rownum = 0;

    // Collect the number of attributes in the table, this is not effected by the select
    // but the nbr in the actuall table defintion. 
    let rel = scan_state.ss_currentRelation;
    let tup_desc = (*rel).rd_att;
    let natts = (*tup_desc).natts as usize;
    state.natts = natts;

    // Save the state
    (*node).fdw_state = state.into_pg() as *mut std::ffi::c_void;
}

#[pg_guard]
unsafe extern "C" fn iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    let slot = (*node).ss.ss_ScanTupleSlot;
    if let Some(clear) = (*(*slot).tts_ops).clear {
        clear(slot);
    }
    // Get the state and PgBox;
    let mut state: PgBox<FdwState> = PgBox::<FdwState>::from_pg((*node).fdw_state as _);

    // Use a memory context, not sure if this is actually needed or not ..
    state.tmp_ctx.reset();
    let mut old_ctx = state.tmp_ctx.set_as_current();

    // when this happens we will not load more data, we return one or multiple rows here as I understand
    // we want to limit how often we read the file so his will be relevant. 

    if (*state).rownum == 0 {
        let meta = (*state).metadata.clone().unwrap();
        let filters = &(*state).filters;
        let data = meta.fetch_data(filters.clone(), &state.cols);
        let (values, mask) = meta.tuples(&data, (*state).cols.clone(), state.natts);
        (*state).tuples = values;
        (*state).nulls = mask;
    }

    if (*state).rownum >= (*state).tuples.len()   { // https://www.highgo.ca/2021/09/03/implement-foreign-scan-with-fdw-interface-api/
        (*(*slot).tts_ops).clear.expect("missing")(slot);
        return slot;
    }

    let idx = (*state).rownum.clone();
    let mut row:Vec<Datum> = (*state).tuples[idx].iter().map(|val| val.clone().into_datum().unwrap()).collect();
    (*slot).tts_values = row.as_mut_ptr();
    (*slot).tts_isnull = (*state).nulls[idx].as_mut_ptr();
    pg_sys::ExecStoreVirtualTuple(slot);
    state.rownum += 1;
    old_ctx.set_as_current();
    return slot
}




unsafe extern "C" fn re_scan_foreign_scan(node: *mut pg_sys::ForeignScanState) {
    let state = (*node).fdw_state as *mut FdwState;
    (*state).rownum = 0;
}

extern "C" fn end_foreign_scan(_node: *mut pg_sys::ForeignScanState) {
    debug1!("HelloFdw: hello_end_foreign_scan");
}

extern "C" fn analyze_foreign_table(
    _relation: pg_sys::Relation,
    _func: *mut pg_sys::AcquireSampleRowsFunc,
    totalpages: *mut pg_sys::BlockNumber,
) -> bool {
    debug1!("HelloFdw: hello_analyze_foreign_table");
    unsafe {
        *totalpages = 4;
    }
    true
}

//
// C call stubs (missing from pgrx for pg13)
//
extern "C" {
    fn create_foreignscan_path(
        root: *mut pg_sys::PlannerInfo,
        rel: *mut pg_sys::RelOptInfo,
        target: *mut pg_sys::PathTarget,
        rows: f64,
        startup_cost: pg_sys::Cost,
        total_cost: pg_sys::Cost,
        pathkeys: *mut pg_sys::List,
        required_outer: pg_sys::Relids,
        fdw_outerpath: *mut pg_sys::Path,
        fdw_private: *mut pg_sys::List,
    ) -> *mut pg_sys::Path;
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::*;

    #[cfg(not(feature = "no-schema-generation"))]
    #[pg_test]
    fn test_selecty() {
        // This is already created, why I dont know though ... 
        // Spi::run("CREATE EXTENSION db721_fdw").unwrap();
        Spi::run("CREATE FOREIGN DATA WRAPPER db721_fdw HANDLER db721_fdw_handler VALIDATOR db721_fdw_validator").unwrap();
        Spi::run("CREATE SERVER db721_server FOREIGN DATA WRAPPER db721_fdw").unwrap();
        Spi::run("CREATE FOREIGN TABLE db721_fdw_table ( identifier integer, farm_name varchar, weight_model varchar, sex varchar, age_weeks real, weight_g real,notes varchar) SERVER db721_server OPTIONS (filename '/Users/niklashansson/OpenSource/postgres/cmudb/extensions/db721_fdw/data-chickens.db721', table 'Farm')").unwrap();

        // Check number of rows
        let row = Spi::get_one::<i64>("SELECT COUNT(identifier) FROM db721_fdw_table");
        let exp = Ok(Some(120000));
        assert_eq!(
            row,
            exp
        );
        // Check filtering 

        // Check selecting subsets of columns

        // Can we check time to see it is faser reducing the disk I/O?
        // Just to make sure things are as expected
        // Check both read less columns and check filtering to read less data

    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
