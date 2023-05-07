use pgrx::*;
use std::os::raw::c_int;
use std::collections::HashMap;
use std::ffi::CStr;

pg_module_magic!();

pub struct FdwState {
    rownum: usize,
    opts: HashMap<String, String>,
}

impl FdwState {
    pub fn new() -> Self {
        Self { rownum: 0 , opts: HashMap::new()}
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
    fdwroutine.GetForeignRelSize = Some(hello_get_foreign_rel_size);
    fdwroutine.GetForeignPaths = Some(hello_get_foreign_paths);
    fdwroutine.GetForeignPlan = Some(hello_get_foreign_plan);
    fdwroutine.ExplainForeignScan = Some(hello_explain_foreign_scan);
    fdwroutine.BeginForeignScan = Some(hello_begin_foreign_scan);
    fdwroutine.IterateForeignScan = Some(hello_iterate_foreign_scan);
    fdwroutine.ReScanForeignScan = Some(hello_re_scan_foreign_scan);
    fdwroutine.EndForeignScan = Some(hello_end_foreign_scan);
    fdwroutine.AnalyzeForeignTable = Some(hello_analyze_foreign_table);

    fdwroutine
}


pub(crate) unsafe fn extract_from_op_expr(
    _root: *mut pg_sys::PlannerInfo,
    baserel_id: pg_sys::Oid,
    baserel_ids: pg_sys::Relids,
    expr: *mut pg_sys::OpExpr,
){
    let args: PgList<pg_sys::Node> = PgList::from_pg((*expr).args);
    // only deal with binary operator
    log!("args are: {}", args.len())
    // we can now get it out if we like to. 
    // WOOOP WOOOP
}


#[pg_guard]
unsafe extern "C" fn hello_get_foreign_rel_size(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
) {
    log!("HelloFdw: hello_get_foreign_rel_size");

    (*baserel).rows = 10.0;
    (*baserel).fdw_private = std::ptr::null_mut();

    // Allocate a box with the memory, zero initiallize it now. 
    let mut state = pgrx::PgBox::<FdwState>::alloc0();
    
    let conds = PgList::<pg_sys::RestrictInfo>::from_pg((*baserel).baserestrictinfo);
    for cond in conds.iter_ptr() {
        let expr = (*cond).clause as *mut pg_sys::Node;
        let _extracted = if is_a(expr, pg_sys::NodeTag_T_OpExpr) {
            extract_from_op_expr(root, foreigntableid, (*baserel).relids, expr as _)
        } else if is_a(expr, pg_sys::NodeTag_T_NullTest) {
            extract_from_null_test(foreigntableid, expr as _)
        } else if is_a(expr, pg_sys::NodeTag_T_BoolExpr) {
            extract_from_bool_expr(root, foreigntableid, (*baserel).relids, expr as _)
        } else {
            log!("Other");
            if let Some(stm) = pgrx::nodes::node_to_string(expr) {
                log!("issues");
            }
        };
    }
    log!("DONE HERE NOW");
    let mut ret = HashMap::new();
    let ftable = pg_sys::GetForeignTable(foreigntableid);
    let options: PgList<pg_sys::DefElem> = PgList::from_pg((*ftable).options);
    for option in options.iter_ptr() {
        let name = CStr::from_ptr((*option).defname);
        let value = CStr::from_ptr(pg_sys::defGetString(option));
        ret.insert(
            name.to_str().unwrap().to_owned(),
            value.to_str().unwrap().to_owned(),
        );
    }
    state.opts = ret; 
    // Continue here , maybe as _ is enough ... 
    (*baserel).fdw_private = state.into_pg() as *mut std::ffi::c_void;

}

pub(crate) unsafe fn extract_from_null_test(
    baserel_id: pg_sys::Oid,
    expr: *mut pg_sys::NullTest,
){
    let var = (*expr).arg as *mut pg_sys::Var;
    log!("Null test");
    if !is_a(var as _, pg_sys::NodeTag_T_Var) || (*var).varattno < 1 {
        return;
    }

    let field = pg_sys::get_attname(baserel_id, (*var).varattno, false);

    let opname = if (*expr).nulltesttype == pg_sys::NullTestType_IS_NULL {
        "is".to_string()
    } else {
        "is not".to_string()
    };

    log!("The answere is: {}", opname)
}



pub(crate) unsafe fn extract_from_bool_expr(
    _root: *mut pg_sys::PlannerInfo,
    baserel_id: pg_sys::Oid,
    baserel_ids: pg_sys::Relids,
    expr: *mut pg_sys::BoolExpr,
) {
    let args: PgList<pg_sys::Node> = PgList::from_pg((*expr).args);

    log!("Bool stuff");
    if (*expr).boolop != pg_sys::BoolExprType_NOT_EXPR || args.len() != 1 {
        return;
    }

    let var = args.head().unwrap() as *mut pg_sys::Var;
    if (*var).varattno < 1
        || (*var).vartype != pg_sys::BOOLOID
        || !pg_sys::bms_is_member((*var).varno as c_int, baserel_ids)
    {
        return;
    }

    let field = pg_sys::get_attname(baserel_id, (*var).varattno, false);

    log!("The operator is: =")
}


#[pg_guard]
unsafe extern "C" fn hello_get_foreign_paths(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    debug1!("HelloFdw: hello_get_foreign_paths");
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

unsafe extern "C" fn hello_get_foreign_plan(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
    best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    mut scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    debug1!("HelloFdw: hello_get_foreign_plan");

    scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);
    pg_sys::make_foreignscan(
        tlist,
        scan_clauses,
        (*baserel).relid,
        std::ptr::null_mut(),
        (*best_path).fdw_private,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        outer_plan,
    )
}

#[pg_guard]
extern "C" fn hello_explain_foreign_scan(
    _node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    debug1!("HelloFdw: hello_explain_foreign_scan");

    let hello = std::ffi::CString::new("Hello").expect("invalid");
    let hello_explain = std::ffi::CString::new("Hello Explain Value").expect("invalid");
    unsafe { pg_sys::ExplainPropertyText(hello.as_ptr(), hello_explain.as_ptr(), es) }
}

#[pg_guard]
unsafe extern "C" fn hello_begin_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    eflags: ::std::os::raw::c_int,
) {
    debug1!("HelloFdw: hello_begin_foreign_scan");

    if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32 != 0 {
        return;
    }

    // Continue here as well! 
    // let state = PgBox::<FdwState, AllocatedByPostgres>::PgBox::from_pg((*baserel).fdw_private as _);

    // this one is null heeeeereeee that is an issue for sure. 
    //let mut state: PgBox<FdwState> = PgBox::<FdwState>::from_pg((*node).fdw_state as _);
    // let mut state = FdwState::deserialize_from_list((*plan).fdw_private as _);

    let mut state = pgrx::PgBox::<FdwState>::alloc0();
    state.rownum = 0;
    // Here we add a duckdb query instead ... 
    (*node).fdw_state = state.into_pg() as *mut std::ffi::c_void;
}

#[pg_guard]
unsafe extern "C" fn hello_iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    log!("stuff");
    let slot = (*node).ss.ss_ScanTupleSlot;
    let state = (*node).fdw_state as *mut FdwState;
    // when this happens we will not load more data, we return one or multiple rows here as I understand
    // we want to limit how often we read the file so his will be relevant. 
    if (*state).rownum > 10 { // https://www.highgo.ca/2021/09/03/implement-foreign-scan-with-fdw-interface-api/
        (*(*slot).tts_ops).clear.expect("missing")(slot);
        return slot;
    }

    // for some reason we only get the last one here ... 
    // We only get the last one and that is how it is supposed to be and that is fine ... 
    // We will return one row per fetch as I understand, but we can fetch it all in one go. 
    let rel = (*node).ss.ss_currentRelation;
    let attinmeta = pg_sys::TupleDescGetAttInMetadata((*rel).rd_att);
    let natts = (*(*rel).rd_att).natts;

    let size = std::mem::size_of::<*const ::std::os::raw::c_char>() * natts as usize;
    let values = pg_sys::palloc0(size) as *mut *const ::std::os::raw::c_char;
    let slice = std::slice::from_raw_parts_mut(values, size);
    let hello_world = std::ffi::CString::new("Hello new,World").expect("invalid");
    let hello_world2 = std::ffi::CString::new((*state).rownum.to_string()).expect("invalid");
    slice[0] = hello_world.as_ptr();
    slice[1] = hello_world2.as_ptr();
    let tuple =
        pg_sys::BuildTupleFromCStrings(attinmeta, values as *mut *mut ::std::os::raw::c_char);

    // Need one of these per row, and why is that???
    pg_sys::ExecStoreHeapTuple(tuple, slot, false);
    (*state).rownum += 1;

    slot
}

unsafe extern "C" fn hello_re_scan_foreign_scan(node: *mut pg_sys::ForeignScanState) {
    debug1!("HelloFdw: hello_re_scan_foreign_scan");

    let state = (*node).fdw_state as *mut FdwState;
    (*state).rownum = 0;
}

extern "C" fn hello_end_foreign_scan(_node: *mut pg_sys::ForeignScanState) {
    debug1!("HelloFdw: hello_end_foreign_scan");
}

extern "C" fn hello_analyze_foreign_table(
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
        Spi::run("CREATE FOREIGN DATA WRAPPER hello_fdw HANDLER hello_fdw_handler VALIDATOR hello_fdw_validator").unwrap();
        Spi::run("CREATE SERVER hello_server FOREIGN DATA WRAPPER hello_fdw").unwrap();
        Spi::run("CREATE FOREIGN TABLE hello_fdw_table (id text, data text) SERVER hello_server").unwrap();

        let row = Spi::get_two::<String, String>("SELECT * FROM hello_fdw_table");
        let exp = Ok((Some("Hello new,World".to_string()), Some("Hello new,World".to_string())));


        assert_eq!(
            row,
            exp
        );
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
