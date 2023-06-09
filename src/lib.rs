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
    val: Vec<Datum>,
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
            , val: Vec::<Datum>::new()
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

pub(crate) unsafe fn unnest_clause(node: *mut pg_sys::Node) -> *mut pg_sys::Node {
    if is_a(node, pg_sys::NodeTag_T_RelabelType) {
        (*(node as *mut pg_sys::RelabelType)).arg as _
    } else if is_a(node, pg_sys::NodeTag_T_ArrayCoerceExpr) {
        (*(node as *mut pg_sys::ArrayCoerceExpr)).arg as _
    } else {
        node
    }
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

    // Here we start to handle the conditions
    let conds = PgList::<pg_sys::RestrictInfo>::from_pg((*baserel).baserestrictinfo);
    let mut filters:HashMap<String, Filter> = HashMap::new();
    for cond in conds.iter_ptr() {
        let expr = (*cond).clause as *mut pg_sys::Node;
        if is_a(expr, pg_sys::NodeTag_T_OpExpr) {
            let filter = extract_from_op_expr(root, foreigntableid, (*baserel).relids, expr as _);
            if let Some(filter) = filter{
                filters.insert(filter.column.clone(),filter);
            }
        } else {
            continue;
        };
    }

    // Here we handled the options
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

    if ret.contains_key("filename"){
        let Some(filename) = ret.get("filename") else {todo!()};
        state.metadata = Some(read_metadata(filename.to_string()));
        log!("Metadata is set");
    }


    let mut col_vars: *mut pg_sys::List = ptr::null_mut();

    // Here we handle which columns where selected
    let mut cols = Vec::new();
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

    let col_vars: PgList<pg_sys::Var> = PgList::from_pg(col_vars);
    for var in col_vars.iter_ptr() {
        let rte = pg_sys::planner_rt_fetch((*var).varno as u32, root);
        let attno = (*var).varattno;
        let attname = pg_sys::get_attname((*rte).relid, attno, true);
        if !attname.is_null() {
            // generated column is not supported
            if pg_sys::get_attgenerated((*rte).relid, attno) > 0 {
                continue;
            }

            let type_oid = pg_sys::get_atttype((*rte).relid, attno);
            cols.push(ColumnMetadada {
                name: CStr::from_ptr(attname).to_str().unwrap().to_owned(),
                num: attno as usize,
                type_oid,
            });
        }
    }

    log!("The columns are: {:?}", cols);
    state.cols = cols;
    state.opts = ret; 
    state.filters = filters;
    // Continue here , maybe as _ is enough ... 
    (*baserel).fdw_private = state.into_pg() as *mut std::ffi::c_void;
    // Print all the options here and if they are expected and so on. 

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
    _best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    mut scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    debug1!("HelloFdw: hello_get_foreign_plan");

    let state = PgBox::<FdwState>::from_pg((*baserel).fdw_private as _);

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
    log!("HelloFdw: hello_begin_foreign_scan");

    if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32 != 0 {
        return;
    }

    // Get the state correctly
    let scan_state = (*node).ss;
    let plan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
    let list = PgList::<pg_sys::Const>::from_pg((*plan).fdw_private );
    let cst = list.head().unwrap();
    let ptr = i64::from_datum((*cst).constvalue, (*cst).constisnull).unwrap();
    let mut state:PgBox::<FdwState> = PgBox::from_pg(ptr as _);
    state.rownum = 0;

    // initialize scan result lists
    let rel = scan_state.ss_currentRelation;
    let tup_desc = (*rel).rd_att;
    let natts = (*tup_desc).natts as usize;
    
    state.natts = natts;
    (*node).fdw_state = state.into_pg() as *mut std::ffi::c_void;
}

#[pg_guard]
unsafe extern "C" fn hello_iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    let slot = (*node).ss.ss_ScanTupleSlot;
    if let Some(clear) = (*(*slot).tts_ops).clear {
        clear(slot);
    }
    // let state = (*node).fdw_state as *mut FdwState;
    let mut state: PgBox<FdwState> = PgBox::<FdwState>::from_pg((*node).fdw_state as _);

    state.tmp_ctx.reset();
    let mut old_ctx = state.tmp_ctx.set_as_current();

    // when this happens we will not load more data, we return one or multiple rows here as I understand
    // we want to limit how often we read the file so his will be relevant. 

    if (*state).rownum == 0 {
        let meta = (*state).metadata.clone().unwrap();
        let filters = &(*state).filters;
        let data = meta.filter(filters.clone(), &state.cols);
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
