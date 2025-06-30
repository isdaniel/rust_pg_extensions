use pgrx::pg_sys;
use pgrx::pg_sys::Datum;
use pgrx::prelude::*;
use pgrx::PgMemoryContexts;
use std::ffi::CString;
use pgrx::AllocatedByRust;
use std::ptr;

#[pg_extern(create_or_replace)]
pub extern "C" fn default_fdw_handler() -> PgBox<pg_sys::FdwRoutine> {
    
    log!("---> default_fdw_handler");
    unsafe {
        let mut fdw_routine = PgBox::<pg_sys::FdwRoutine, AllocatedByRust>::alloc_node(pg_sys::NodeTag::T_FdwRoutine);

        fdw_routine.BeginForeignScan = Some(begin_foreign_scan);
        fdw_routine.IterateForeignScan = Some(iterate_foreign_scan);
        fdw_routine.EndForeignScan = Some(end_foreign_scan);

        fdw_routine.GetForeignRelSize = Some(get_foreign_rel_size);
        fdw_routine.GetForeignPaths = Some(get_foreign_paths);
        fdw_routine.GetForeignPlan = Some(get_foreign_plan);

        fdw_routine.into_pg_boxed()


        // let mut fdw_routine =
        //         FdwRoutine::<AllocatedByRust>::allocnode(pg_sys::NodeTag::T_FdwRoutine);

        //     // import foreign schema
        //     fdw_routine.ImportForeignSchema =
        //         Some(import_foreign_schema::import_foreign_schema::<E, Self>);

        //     // plan phase
        //     fdw_routine.GetForeignRelSize = Some(scan::get_foreign_rel_size::<E, Self>);
        //     fdw_routine.GetForeignPaths = Some(scan::get_foreign_paths::<E, Self>);
        //     fdw_routine.GetForeignPlan = Some(scan::get_foreign_plan::<E, Self>);
        //     fdw_routine.ExplainForeignScan = Some(scan::explain_foreign_scan::<E, Self>);

        //     // scan phase
        //     fdw_routine.BeginForeignScan = Some(scan::begin_foreign_scan::<E, Self>);
        //     fdw_routine.IterateForeignScan = Some(scan::iterate_foreign_scan::<E, Self>);
        //     fdw_routine.ReScanForeignScan = Some(scan::re_scan_foreign_scan::<E, Self>);
        //     fdw_routine.EndForeignScan = Some(scan::end_foreign_scan::<E, Self>);

        //     // modify phase
        //     fdw_routine.AddForeignUpdateTargets = Some(modify::add_foreign_update_targets);
        //     fdw_routine.PlanForeignModify = Some(modify::plan_foreign_modify::<E, Self>);
        //     fdw_routine.BeginForeignModify = Some(modify::begin_foreign_modify::<E, Self>);
        //     fdw_routine.ExecForeignInsert = Some(modify::exec_foreign_insert::<E, Self>);
        //     fdw_routine.ExecForeignDelete = Some(modify::exec_foreign_delete::<E, Self>);
        //     fdw_routine.ExecForeignUpdate = Some(modify::exec_foreign_update::<E, Self>);
        //     fdw_routine.EndForeignModify = Some(modify::end_foreign_modify::<E, Self>);

        //     Self::fdw_routine_hook(&mut fdw_routine);
        //     fdw_routine.into_pg_boxed()
    }
}

#[pg_guard]
extern "C" fn get_foreign_rel_size(
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
extern "C" fn get_foreign_paths(
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
extern "C" fn get_foreign_plan(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
    _best_path: *mut pg_sys::ForeignPath,
    _tlist: *mut pg_sys::List,
    _scan_clauses: *mut pg_sys::List,
    _outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    log!("---> get_foreign_plan");
    unsafe {
        pg_sys::make_foreignscan(
            _tlist,
            _scan_clauses,
            (*baserel).relid,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            _outer_plan,
        )
    }
}

#[pg_guard]
extern "C" fn begin_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    _eflags: ::std::os::raw::c_int,
) {
    log!("---> begin_foreign_scan");
    unsafe {
        let state = PgMemoryContexts::CurrentMemoryContext
            .leak_and_drop_on_delete(RedisFdwState { row: 0 });

        (*node).fdw_state = state as *mut std::ffi::c_void;
    }
}

#[pg_guard]
extern "C" fn iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {

    log!("---> iterate_foreign_scan");

    unsafe {
        let state = &mut *((*node).fdw_state as *mut RedisFdwState);
        let slot = (*node).ss.ss_ScanTupleSlot;
        let tupdesc = (*slot).tts_tupleDescriptor;
        let natts = (*tupdesc).natts as usize;
        log!("state.row : {}",state.row);

        if state.row >= 5 {
            exec_clear_tuple(slot);
            return slot;
        }

        exec_clear_tuple(slot);

        let values_ptr = PgMemoryContexts::For((*slot).tts_mcxt)
            .palloc(std::mem::size_of::<pg_sys::Datum>() * natts)
            as *mut pg_sys::Datum;

        let nulls_ptr = PgMemoryContexts::For((*slot).tts_mcxt)
            .palloc(std::mem::size_of::<bool>() * natts)
            as *mut bool;

        // Fill values
        *values_ptr.add(0) = (state.row + 1).into();
        let name = format!("hello_{}", state.row + 1);
        let cstring = CString::new(name).unwrap();
        *values_ptr.add(1) = Datum::from( pg_sys::cstring_to_text(cstring.as_ptr()));
        
        // Mark not null
        *nulls_ptr.add(0) = false;
        *nulls_ptr.add(1) = false;

        // Set slot
        (*slot).tts_values = values_ptr;
        (*slot).tts_isnull = nulls_ptr;

        pg_sys::ExecStoreVirtualTuple(slot);

        state.row += 1;

        slot
    }
}

#[pg_guard]
extern "C" fn end_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) {
    log!("---> end_foreign_scan");
    unsafe {
        if !(*node).fdw_state.is_null() {
            (*node).fdw_state = std::ptr::null_mut();
        }
    }
}

#[repr(C)]
struct RedisFdwState {
    row: i32,
}

unsafe fn exec_clear_tuple(slot: *mut pg_sys::TupleTableSlot) {
    if let Some(clear) = (*(*slot).tts_ops).clear {
        clear(slot);
    }
}

