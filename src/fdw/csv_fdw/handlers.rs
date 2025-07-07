use std::{collections::HashMap, ptr};
use csv::StringRecord;
use pgrx::{ prelude::*, AllocatedByRust, PgBox
};
use crate::fdw::{csv_fdw::state::get_csv_reader, utils_share::utils::{
        build_attr_name_to_index_map, build_header_index_map, exec_clear_tuple, get_datum, get_foreign_table_options, tuple_desc_attr
    }};
use crate::fdw::csv_fdw::state::CsvFdwState;

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
    }
}

#[pg_guard]
extern "C-unwind" fn get_foreign_paths(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    log!("---> get_foreign_paths");
    unsafe {
        let path = pg_sys::create_foreignscan_path(
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
        pg_sys::add_path(baserel, path as *mut pg_sys::Path);
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
    pg_sys::make_foreignscan(
        tlist,
        scan_clauses,
        (*baserel).relid,
        ptr::null_mut(),
        (*baserel).fdw_private as _,
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
        let relation = (*node).ss.ss_currentRelation;
        let relid = (*relation).rd_id;
        let options  = get_foreign_table_options(relid);
        log!("Foreign table options: {:?}", options);
        let mut csv_reader = get_csv_reader(&options);

        let header = csv_reader.headers().expect("Failed to read CSV headers");
        let header_name_to_colno = build_attr_name_to_index_map(relation);

        let header_name_to_colno = build_header_index_map( header, &header_name_to_colno );

        let state = CsvFdwState::new(header_name_to_colno,options, csv_reader);

        (*node).fdw_state = Box::into_raw(Box::new(state)) as *mut std::os::raw::c_void;
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
        let csv_reader = &mut state.csv_reader;
        let mut record = StringRecord::new();
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
