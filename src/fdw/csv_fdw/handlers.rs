use std::{ffi::c_void, ptr};
use csv::StringRecord;
use pgrx::{ list::{self}, memcx, pg_sys::{Const, Expr, NodeTag, Var}, prelude::*, AllocatedByRust, PgBox
};
use crate::fdw::{csv_fdw::state::get_csv_reader, utils_share::{cell, utils::{
        build_attr_name_to_index_map, build_header_index_map, deserialize_from_list, exec_clear_tuple, get_datum, get_foreign_table_options, pg_list_to_rust_list, serialize_to_list, tuple_desc_attr
    }}};
use crate::fdw::csv_fdw::state::CsvFdwState;
use crate::fdw::utils_share::cell::Cell;

pub type FdwRoutine<A = AllocatedByRust> = PgBox<pg_sys::FdwRoutine, A>;

#[pg_extern(create_or_replace)]
pub extern "C" fn csv_fdw_handler() -> FdwRoutine {
    log!("---> csv_fdw_handler");
    unsafe {
        let mut fdw_routine = PgBox::<pg_sys::FdwRoutine, AllocatedByRust>::alloc_node(pg_sys::NodeTag::T_FdwRoutine);

        fdw_routine.GetForeignRelSize = Some(get_foreign_rel_size);
        fdw_routine.GetForeignPaths = Some(get_foreign_paths);
        fdw_routine.GetForeignPlan = Some(get_foreign_plan);
        // scan phase
        fdw_routine.BeginForeignScan = Some(begin_foreign_scan);
        fdw_routine.IterateForeignScan = Some(iterate_foreign_scan);
        fdw_routine.ReScanForeignScan = Some(re_scan_foreign_scan);
        fdw_routine.EndForeignScan = Some(end_foreign_scan); 

        fdw_routine
    }
}

#[pg_guard]
extern "C-unwind" fn get_foreign_rel_size(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    log!("---> get_foreign_rel_size");
    unsafe {
        (*baserel).rows = 1000.0;
        let state = CsvFdwState::new();
        (*baserel).fdw_private = Box::into_raw(Box::new(state)) as *mut CsvFdwState as *mut c_void;
    }
}

#[pg_guard]
extern "C-unwind" fn get_foreign_paths(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    log!("---> get_foreign_paths");
    unsafe {
        let startup_cost = 100.0;
        let total_cost = startup_cost + (*baserel).rows;
        // create a ForeignPath node and add it as the only possible path
        let path = pg_sys::create_foreignscan_path(
            root,
            baserel,
            ptr::null_mut(), // default pathtarget
            (*baserel).rows,
            startup_cost,
            total_cost,
            ptr::null_mut(), // no pathkeys
            ptr::null_mut(), // no outer rel either
            ptr::null_mut(), // no extra plan
            ptr::null_mut(), // no fdw_private data
        );
        pg_sys::add_path(baserel, &mut ((*path).path));
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn get_foreign_plan(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
    _best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    log!("---> get_foreign_plan");

    let state = PgBox::<CsvFdwState>::from_pg((*baserel).fdw_private  as _);
    pg_sys::make_foreignscan(
        tlist,
        pg_sys::extract_actual_clauses(scan_clauses, false), 
        (*baserel).relid,
        ptr::null_mut(),
        serialize_to_list(state),
        ptr::null_mut(),
        ptr::null_mut(),
        outer_plan,
    )

}

#[pg_guard]
extern "C-unwind" fn begin_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    eflags: ::std::os::raw::c_int,
) {

    if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32 != 0 {
        return;
    }

    log!("---> begin_foreign_scan");
    unsafe {
        let plan = (*node).ss.ps.plan as *mut pg_sys::ForeignScan;
        let mut state = deserialize_from_list::<CsvFdwState>((*plan).fdw_private as _);
        let relation = (*node).ss.ss_currentRelation;
        let relid = (*relation).rd_id;
        let options  = get_foreign_table_options(relid);
        log!("Foreign table options: {:?}", options);
        let mut csv_reader = get_csv_reader(&options);

        let header = csv_reader.headers().expect("Failed to read CSV headers");

        let header_name_to_colno = build_attr_name_to_index_map(relation);
        state.header_name_to_colno = build_header_index_map( header, &header_name_to_colno );
        state.csv_reader = Some(csv_reader);
        (*node).fdw_state = state.into_pg() as *mut c_void;
    }
}



#[pg_guard]
extern "C-unwind" fn iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    log!("---> iterate_foreign_scan");

    unsafe {
        let mut state = PgBox::<CsvFdwState>::from_pg((*node).fdw_state as _);
        let slot = (*node).ss.ss_ScanTupleSlot;
        let tupdesc = (*slot).tts_tupleDescriptor;
        exec_clear_tuple(slot);
        let mut record = StringRecord::new();
        
        let csv_reader = match state.csv_reader {
            Some(ref mut reader) => reader,
            None => {
                log!("CSV reader is not initialized.");
                return slot;
            }
        };

        match csv_reader.read_record(&mut record) {
            Ok(false) => { }
            Ok(true) => {
                for (i,field) in record.iter().enumerate(){
                    let colno = state.header_name_to_colno[i];
                    let pgtype = (*tuple_desc_attr(tupdesc, colno)).atttypid;
                    let datum_value = get_datum(field, pgtype);
                    (*slot).tts_values.add(colno).write(datum_value);
                    (*slot).tts_isnull.add(colno).write(false);
                }
                pg_sys::ExecStoreVirtualTuple(slot);
            }
            Err(e) => {
                log!("Error reading CSV record: {}", e);
            }
        }
        
        slot
    }
}

#[pg_guard]
extern "C-unwind" fn end_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) {
    log!("---> end_foreign_scan");
    unsafe {
        let state = (*node).fdw_state as *mut CsvFdwState;
        let _ = Box::from_raw(state);
    }
}

#[pg_guard]
extern "C-unwind" fn re_scan_foreign_scan(
    _node: *mut pg_sys::ForeignScanState,
) {
    log!("---> re_scan_foreign_scan");
    // Reset or reinitialize scan state here if needed
}
