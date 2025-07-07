use std::{collections::HashMap, ffi::{c_int}, ptr, slice};
use once_cell::sync::Lazy;
use pgrx::{
    pg_sys::{ Datum, Index, ModifyTable, PlannerInfo}, prelude::*, AllocatedByRust, PgBox, PgRelation, PgTupleDesc
};
use crate::fdw::utils_share::{
    cell::Cell,
    memory::create_wrappers_memctx,
    row::Row,
    utils::{
        build_attr_name_to_index_map, exec_clear_tuple, get_datum, get_foreign_table_options, string_from_cstr, tuple_desc_attr, tuple_table_slot_to_row
    }
};

use crate::fdw::default_fdw::state::{DefaultFdwState, FdwModifyState};

type TableMap = HashMap<String, String>;
static MEMORY_TABLE: Lazy<std::sync::RwLock<Vec<TableMap>>> = Lazy::new(|| std::sync::RwLock::new(Vec::new()));

pub type FdwRoutine<A = AllocatedByRust> = PgBox<pgrx::pg_sys::FdwRoutine, A>;

#[pg_extern(create_or_replace)]
pub extern "C" fn default_fdw_handler() -> FdwRoutine {
    log!("---> default_fdw_handler");
    unsafe {
        let mut fdw_routine = PgBox::<pgrx::pg_sys::FdwRoutine, AllocatedByRust>::alloc_node(pgrx::pg_sys::NodeTag::T_FdwRoutine);
        fdw_routine.ImportForeignSchema = Some(import_foreign_schema);

        fdw_routine.GetForeignRelSize = Some(get_foreign_rel_size);
        fdw_routine.GetForeignPaths = Some(get_foreign_paths);
        fdw_routine.GetForeignPlan = Some(get_foreign_plan);
        fdw_routine.ExplainForeignScan = Some(explain_foreign_scan);

        // scan phase
        fdw_routine.BeginForeignScan = Some(begin_foreign_scan);
        fdw_routine.IterateForeignScan = Some(iterate_foreign_scan);
        fdw_routine.ReScanForeignScan = Some(re_scan_foreign_scan);
        fdw_routine.EndForeignScan = Some(end_foreign_scan); 

        fdw_routine.AddForeignUpdateTargets = Some(add_foreign_update_targets);
        fdw_routine.PlanForeignModify = Some(plan_foreign_modify);
        fdw_routine.BeginForeignModify = Some(begin_foreign_modify);
        fdw_routine.ExecForeignInsert = Some(exec_foreign_insert);
        fdw_routine.ExecForeignDelete = Some(exec_foreign_delete);
        fdw_routine.ExecForeignUpdate = Some(exec_foreign_update);
        fdw_routine.EndForeignModify = Some(end_foreign_modify);

        fdw_routine
    }
}

#[pg_guard]
extern "C-unwind" fn import_foreign_schema(
    _stmt: *mut pgrx::pg_sys::ImportForeignSchemaStmt,
    _server_oid: pgrx::pg_sys::Oid,
) -> *mut pgrx::pg_sys::List {
    log!("---> import_foreign_schema");
    std::ptr::null_mut()
}

#[pg_guard]
extern "C-unwind" fn get_foreign_rel_size(
    _root: *mut pgrx::pg_sys::PlannerInfo,
    baserel: *mut pgrx::pg_sys::RelOptInfo,
    foreigntableid: pgrx::pg_sys::Oid,
) {
    log!("---> get_foreign_rel_size");
    unsafe {
        let ctx_name = format!("Wrappers_scan_{}", foreigntableid.to_u32());
        log!("Creating memory context: {}", ctx_name);
        let ctx = create_wrappers_memctx(&ctx_name);
        let state = DefaultFdwState::new(ctx);

        (*baserel).fdw_private = Box::into_raw(Box::new(state)) as *mut DefaultFdwState as *mut std::os::raw::c_void;
        log!("(*baserel).fdw_private {:?}",(*baserel).fdw_private);
        (*baserel).rows = 1000.0;
    }
}

#[pg_guard]
extern "C-unwind" fn get_foreign_paths(
    _root: *mut pgrx::pg_sys::PlannerInfo,
    baserel: *mut pgrx::pg_sys::RelOptInfo,
    _foreigntableid: pgrx::pg_sys::Oid,
) {
    log!("---> get_foreign_paths");
    unsafe {
        let path = pgrx::pg_sys::create_foreignscan_path(
            _root,
            baserel,
            ptr::null_mut(),           
            (*baserel).rows,            
            10.0,                       
            100.0,                     
            ptr::null_mut(),            
            ptr::null_mut(),            
            ptr::null_mut(),           
            ptr::null_mut(),           
        );
        pgrx::pg_sys::add_path(baserel, path as *mut pgrx::pg_sys::Path);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn get_foreign_plan(
    _root: *mut pgrx::pg_sys::PlannerInfo,
    baserel: *mut pgrx::pg_sys::RelOptInfo,
    _foreigntableid: pgrx::pg_sys::Oid,
    _best_path: *mut pgrx::pg_sys::ForeignPath,
    tlist: *mut pgrx::pg_sys::List,
    scan_clauses: *mut pgrx::pg_sys::List,
    outer_plan: *mut pgrx::pg_sys::Plan,
) -> *mut pgrx::pg_sys::ForeignScan {
    log!("---> get_foreign_plan");
    pgrx::pg_sys::make_foreignscan(
        tlist,
        pg_sys::extract_actual_clauses(scan_clauses, false),
        (*baserel).relid,
        ptr::null_mut(),
        (*baserel).fdw_private as _,
        ptr::null_mut(),
        ptr::null_mut(),
        outer_plan,
    )
}

#[pg_guard]
extern "C-unwind" fn explain_foreign_scan(
    _node: *mut pgrx::pg_sys::ForeignScanState,
    _es: *mut pgrx::pg_sys::ExplainState,
) {
    log!("---> explain_foreign_scan");
    // You can add custom explanation logic here if needed
}

#[pg_guard]
extern "C-unwind" fn begin_foreign_scan(
    node: *mut pgrx::pg_sys::ForeignScanState,
    eflags: ::std::os::raw::c_int,
) {

    if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32 != 0 {
        return;
    }

    log!("---> begin_foreign_scan");
    unsafe {
        let scan_state = (*node).ss;
        let plan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
        let relation = (*node).ss.ss_currentRelation;
        let relid = (*relation).rd_id;
        let mut state =  PgBox::<DefaultFdwState>::from_pg((*plan).fdw_private as _); 

        let options = get_foreign_table_options(relid);
        log!("Foreign table options: {:?}", options);

        state.header_name_to_colno = build_attr_name_to_index_map(relation);

        log!("Header name to column number mapping: {:?}", state.header_name_to_colno);
        
        (*node).fdw_state = state.into_pg() as _;
    }
}

#[pg_guard]
extern "C-unwind" fn iterate_foreign_scan(
    node: *mut pgrx::pg_sys::ForeignScanState,
) -> *mut pgrx::pg_sys::TupleTableSlot {
    log!("---> iterate_foreign_scan");

    unsafe {
        let mut state = PgBox::<DefaultFdwState>::from_pg((*node).fdw_state as _);
        let slot = (*node).ss.ss_ScanTupleSlot;
        let tupdesc = (*slot).tts_tupleDescriptor;
        let header_name_to_colno = &state.header_name_to_colno;
        let data = MEMORY_TABLE.read().unwrap();
        
        exec_clear_tuple(slot);
        if data.len() == 0 || state.row_count >= data.len() {
            return slot;
        }
    
        let tuple_row = &data[state.row_count];
        log!("iterate_foreign_scan tuple_row: {:?}", tuple_row);
        
        for (col_name, value_str) in tuple_row.iter() {
            let colno = header_name_to_colno[col_name];
            let pgtype = (*tuple_desc_attr(tupdesc, colno )).atttypid;
            let datum_value = get_datum(value_str, pgtype);
            (*slot).tts_values.add(colno).write(datum_value);
            (*slot).tts_isnull.add(colno).write(false);
        }

        // state.values.clear();
        // state.nulls.clear();
        // for i in 0..natts {
        //     let attr = tuple_desc_attr(tupdesc, i);
        //     let col_name = string_from_cstr((*attr).attname.data.as_ptr());
        //     match tuple_row.get(&col_name) {
        //         Some(val) => {
        //             log!("iterate_foreign_scan => Column: {}, Value: {}", col_name, val);

        //             let datum = parse_cell(val).into_datum().unwrap();
        //             state.values.push(datum);
        //             state.nulls.push(false);
        //         }
        //         None => {
        //             state.nulls.push(true);
        //         }
        //     }
        // }
        
        // (*slot).tts_values = state.values.as_mut_ptr();
        // (*slot).tts_isnull = state.nulls.as_mut_ptr();
        pgrx::pg_sys::ExecStoreVirtualTuple(slot);
        
        state.row_count += 1;
        slot
    }
}

#[pg_guard]
extern "C-unwind" fn end_foreign_scan(
    node: *mut pgrx::pg_sys::ForeignScanState,
) {
    log!("---> end_foreign_scan");
    unsafe {
        let state = (*node).fdw_state as *mut DefaultFdwState;
        let _ = Box::from_raw(state);
    }
}

#[pg_guard]
extern "C-unwind" fn re_scan_foreign_scan(
    _node: *mut pgrx::pg_sys::ForeignScanState,
) {
    log!("---> re_scan_foreign_scan");
    // Reset or reinitialize scan state here if needed
}

#[cfg(feature = "pg13")]
#[pg_guard]
unsafe extern "C-unwind" fn add_foreign_update_targets(
    parsetree: *mut pgrx::pg_sys::Query,
    _target_rte: *mut pgrx::pg_sys::RangeTblEntry,
    target_relation: pgrx::pg_sys::Relation,
) {
    log!("---> add_foreign_update_targets");
}

#[cfg(not(feature = "pg13"))]
#[pg_guard]
unsafe extern "C-unwind" fn add_foreign_update_targets(
    _root: *mut pgrx::pg_sys::PlannerInfo,
    _rtindex: pgrx::pg_sys::Index,
    _target_rte: *mut pgrx::pg_sys::RangeTblEntry,
    _target_relation: pgrx::pg_sys::Relation,
) {
    log!("---> add_foreign_update_targets");
}

#[pg_guard]
unsafe extern "C-unwind" fn plan_foreign_modify(
    root: *mut PlannerInfo,
    _plan: *mut ModifyTable,
    result_relation: Index,
    _subplan_index: ::core::ffi::c_int,
) -> *mut pgrx::pg_sys::List {
    log!("---> plan_foreign_modify");

    unsafe {
        let rte = pgrx::pg_sys::planner_rt_fetch(result_relation, root);
        let rel = PgRelation::with_lock((*rte).relid, pgrx::pg_sys::NoLock as _);
        // get rowid column name from table options
        // search for rowid attribute in tuple descrition
        let tup_desc = PgTupleDesc::from_relation(&rel);
        let rowid_name = "id";
        for attr in tup_desc.iter().filter(|a| !a.attisdropped) {
            let attname = pgrx::name_data_to_str(&attr.attname);
            if attname == rowid_name {
                let ftable_id = rel.oid();

                // create memory context for modify
                let ctx_name = format!("Wrappers_modify_{}", ftable_id.to_u32());
                let ctx = create_wrappers_memctx(&ctx_name);

                // create modify state
                let mut state = FdwModifyState::new(ftable_id, ctx);

                state.rowid_name = rowid_name.to_string();
                state.rowid_typid = attr.atttypid;
                let raw_state_ptr = Box::into_raw(Box::new(state))  as *mut std::os::raw::c_void;

                return pg_sys::lcons(raw_state_ptr as *mut _, std::ptr::null_mut());
            }
        }

        ptr::null_mut()
    }
}

#[pg_guard]
extern "C-unwind" fn begin_foreign_modify(
    mtstate: *mut pgrx::pg_sys::ModifyTableState,
    rinfo: *mut pgrx::pg_sys::ResultRelInfo,
    fdw_private: *mut pgrx::pg_sys::List,
    subplan_index: ::std::os::raw::c_int,
    eflags: ::std::os::raw::c_int,
) {
    log!("---> begin_foreign_modify");
    unsafe {
        let state = PgBox::<FdwModifyState>::from_pg(fdw_private as _);
        (*rinfo).ri_FdwState = state.into_pg() as *mut std::os::raw::c_void;
    }
}

#[pg_guard]
extern "C-unwind" fn exec_foreign_insert(
    _estate: *mut pgrx::pg_sys::EState,
    _rinfo: *mut pgrx::pg_sys::ResultRelInfo,
    slot: *mut pgrx::pg_sys::TupleTableSlot,
    _plan_slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> *mut pgrx::pg_sys::TupleTableSlot {
    log!("---> exec_foreign_insert");
     unsafe {
        let mut map = TableMap::new();
        let row: Row = tuple_table_slot_to_row(slot);
        for i in 0..row.cells.len() {
            let cell = &row.cells[i];
            let col_name = &row.cols[i];
            let val = match cell {
                Some(c) => c.to_string(),
                None => "NULL".to_string(),
            };
            log!(
                "Inserted column: {}, value: {}",
                col_name.to_string(),
                val
            );
            map.insert(col_name.to_string(), val);
        }

        MEMORY_TABLE.write().unwrap().push(map);
        (*slot).tts_tableOid = pgrx::pg_sys::InvalidOid;
        slot
    }
}

#[pg_guard]
extern "C-unwind" fn exec_foreign_update(
    _estate: *mut pgrx::pg_sys::EState,
    _rinfo: *mut pgrx::pg_sys::ResultRelInfo,
    _slot: *mut pgrx::pg_sys::TupleTableSlot,
    _plan_slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> *mut pgrx::pg_sys::TupleTableSlot {
    log!("---> exec_foreign_update");
    unsafe { (*_slot).tts_tableOid = pgrx::pg_sys::InvalidOid };
    _slot
}

#[pg_guard]
extern "C-unwind" fn exec_foreign_delete(
    _estate: *mut pgrx::pg_sys::EState,
    rinfo: *mut pgrx::pg_sys::ResultRelInfo,
    slot: *mut pgrx::pg_sys::TupleTableSlot,
    plan_slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> *mut pgrx::pg_sys::TupleTableSlot {
    log!("---> exec_foreign_delete");
    unsafe {
        let state = PgBox::<FdwModifyState>::from_pg((*rinfo).ri_FdwState as _);
        // let cell = get_rowid_cell(&state, plan_slot);
        // log!("Delete operation: rowid cell: {:?}", cell);
    }

    let mut data = MEMORY_TABLE.write().unwrap();
    data.clear();

    slot
}

unsafe fn get_rowid_cell(
    state: &FdwModifyState,
    plan_slot: *mut pg_sys::TupleTableSlot,
) -> Option<Cell> {
    let mut is_null: bool = true;
    let datum = slot_getattr(plan_slot, 1, &mut is_null);
    Cell::from_polymorphic_datum(datum, is_null, state.rowid_typid)
}

unsafe fn slot_getattr(
    slot: *mut pg_sys::TupleTableSlot,
    attnum: c_int,
    isnull: *mut bool,
) -> Datum {
    assert!(attnum > 0);

    if attnum > (*slot).tts_nvalid.into() {
        pg_sys::slot_getsomeattrs_int(slot, attnum);
    }

    let attnum = attnum as usize;
    let values = slice::from_raw_parts((*slot).tts_values, attnum);
    let nulls = slice::from_raw_parts((*slot).tts_isnull, attnum);

    *isnull = nulls[attnum - 1];
    values[attnum - 1]
}


#[pg_guard]
extern "C-unwind" fn end_foreign_modify(
    _estate: *mut pgrx::pg_sys::EState,
    _rinfo: *mut pgrx::pg_sys::ResultRelInfo,
) {
    log!("---> end_foreign_modify");
    // unsafe {
    //     let state = PgBox::<FdwModifyState>::from_pg((*_rinfo).ri_FdwState as _);
    //     let _ = Box::from_raw(state.into_pg());  
    // }
}
