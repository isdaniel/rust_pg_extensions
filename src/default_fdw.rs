use pgrx::pg_sys::{AsPgCStr, Datum, Index, ModifyTable, Oid, PlannerInfo,MemoryContext};
use pgrx::{fcinfo, prelude::*, AllocatedByRust, PgMemoryContexts, PgTupleDesc,};
use std::fmt::Debug;
use std::{collections::HashMap, ffi::{CStr, CString}, fmt, iter::Zip, num::NonZeroUsize, ptr, slice::Iter, sync::RwLock};
use once_cell::sync::Lazy;
type TableMap = HashMap<String, String>;
static MEMORY_TABLE: Lazy<RwLock<Vec<TableMap>>> = Lazy::new(|| RwLock::new(Vec::new()));

#[cfg(any(feature = "pg13", feature = "pg14"))]
use pg_sys::Value;


#[cfg(any(feature = "pg13", feature = "pg14"))]
unsafe fn pg_string_to_rust(val_value: *mut Value) -> String {
    unsafe {
        CStr::from_ptr((*val_value).val.str_)
            .to_str()
            .unwrap_or_default().to_string()
    }
}

#[cfg(any(feature = "pg15", feature = "pg16"))] 
#[repr(C)]
pub struct Value {
    pub type_: pg_sys::NodeTag,
    pub val: pgrx::pg_sys::ValUnion,
}


#[cfg(any(feature = "pg15", feature = "pg16"))] 
unsafe fn pg_string_to_rust(val_value: *mut Value) -> String {
    (*val_value).val.sval.to_string()
}


pub unsafe fn get_foreign_table_options(relid: pg_sys::Oid) -> HashMap<String, String> {
    let mut options = HashMap::new();

    let ft = pg_sys::GetForeignTable(relid);
    if ft.is_null() {
        return options;
    }

    let opts_list = (*ft).options;
    if opts_list.is_null() {
        return options;
    }

    let len = (*opts_list).length;
    let cells = (*opts_list).elements;

    for i in 0..len {
        let cell_ptr = cells.offset(i as isize);
        let def_elem =  (*cell_ptr).ptr_value as *mut pg_sys::DefElem;
        
        if def_elem.is_null() {
            continue;
        }

        let def_name = CStr::from_ptr((*def_elem).defname)
                .to_str()
                .unwrap_or_default();

        let def_val_node = (*def_elem).arg;
        if !def_val_node.is_null() && (*def_val_node).type_ == pg_sys::NodeTag::T_String {
            let val_value: *mut Value = def_val_node as *mut Value;
            
            let val = pg_string_to_rust(val_value);
            options.insert(def_name.to_string(), val.to_string());
        }
    }

    options
}

#[pg_extern(create_or_replace)]
pub extern "C" fn default_fdw_handler() -> PgBox<pg_sys::FdwRoutine> {
    
    log!("---> default_fdw_handler");
    unsafe {
        let mut fdw_routine = PgBox::<pg_sys::FdwRoutine, AllocatedByRust>::alloc_node(pg_sys::NodeTag::T_FdwRoutine);
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

        //fdw_routine.AddForeignUpdateTargets = Some(add_foreign_update_targets);
        fdw_routine.PlanForeignModify = Some(plan_foreign_modify);
        fdw_routine.BeginForeignModify = Some(begin_foreign_modify);
        fdw_routine.ExecForeignInsert = Some(exec_foreign_insert);
        fdw_routine.ExecForeignDelete = Some(exec_foreign_delete);
        fdw_routine.ExecForeignUpdate = Some(exec_foreign_update);
        fdw_routine.EndForeignModify = Some(end_foreign_modify);

        fdw_routine.into_pg_boxed()
    }
}

#[pg_guard]
extern "C-unwind" fn import_foreign_schema(
    _stmt: *mut pg_sys::ImportForeignSchemaStmt,
    _server_oid: pg_sys::Oid,
) -> *mut pg_sys::List {
    log!("---> import_foreign_schema");
    std::ptr::null_mut()
}

#[pg_guard]
extern "C-unwind" fn get_foreign_rel_size(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
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
extern "C-unwind" fn get_foreign_plan(
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
extern "C-unwind" fn explain_foreign_scan(
    _node: *mut pg_sys::ForeignScanState,
    _es: *mut pg_sys::ExplainState,
) {
    log!("---> explain_foreign_scan");
    // You can add custom explanation logic here if needed
}

#[pg_guard]
extern "C-unwind" fn begin_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    _eflags: ::std::os::raw::c_int,
) {
    log!("---> begin_foreign_scan");
    unsafe {
        let relid = (*(*node).ss.ss_currentRelation).rd_id;
        let options = get_foreign_table_options(relid);
        log!("Foreign table options: {:?}", options);

        let ctx_name = format!("Wrappers_scan_{}", relid.to_u32());
        log!("Creating memory context: {}", ctx_name);
        let ctx = create_wrappers_memctx(&ctx_name);
        let state = RedisFdwState::new(ctx);
        // let state = PgMemoryContexts::CurrentMemoryContext
        //     .leak_and_drop_on_delete(RedisFdwState { 
        //         row: 0 ,
        //         values: Vec::new(),
        //         nulls: Vec::new(),
        //     });

        (*node).fdw_state = Box::leak(Box::new(state)) as *mut RedisFdwState as *mut std::os::raw::c_void;
    }
}

#[pg_guard]
extern "C-unwind" fn iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {

    log!("---> iterate_foreign_scan");

    unsafe {
        //let state = &mut *((*node).fdw_state as *mut RedisFdwState);
        let mut state = PgBox::<RedisFdwState>::from_pg((*node).fdw_state as _);
        let slot = (*node).ss.ss_ScanTupleSlot;
        let tupdesc = (*slot).tts_tupleDescriptor;
        let natts = (*tupdesc).natts as usize;
        let data = MEMORY_TABLE.read().unwrap();
        //log!("iterate_foreign_scan data: {:?}", data);
        exec_clear_tuple(slot);
        if data.len() == 0 || state.row >= data.len() as i32 {
            return slot;
        }

        let tuple_row = &data[state.row as usize];
        log!("iterate_foreign_scan tuple_row: {:?}", tuple_row);

        state.values.clear();
        state.nulls.clear();
        let attrs_ptr = (*tupdesc).attrs.as_ptr();
        for i in 0..natts {
            let attr = &*attrs_ptr.add(i);
            let col_name = CStr::from_ptr(attr.attname.data.as_ptr())
                .to_string_lossy()
                .trim_end_matches('\0')
                .to_string();

            match tuple_row.get(&col_name) {
                Some(val) => {
                    log!("iterate_foreign_scan => Column: {}, Value: {}", col_name, val);

                    let datum = parse_cell(val).into_datum().unwrap();
                    state.values.push(datum);
                    state.nulls.push(false);
                }
                None => {
                    state.nulls.push(true);
                }
            }
        }

        (*slot).tts_values = state.values.as_mut_ptr();
        (*slot).tts_isnull = state.nulls.as_mut_ptr();
        pg_sys::ExecStoreVirtualTuple(slot);
        
        state.row += 1;
        slot
    }
}

fn parse_cell(val: &str) -> Cell {
    match val.parse::<i32>() {
        Ok(i) => Cell::I32(i),
        Err(_) => Cell::String(val.to_string()),
    }
}

#[pg_guard]
extern "C-unwind" fn end_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) {
    log!("---> end_foreign_scan");
    unsafe {
        if !(*node).fdw_state.is_null() {
            (*node).fdw_state = std::ptr::null_mut();
        }
    }
}

#[pg_guard]
extern "C-unwind" fn re_scan_foreign_scan(
    _node: *mut pg_sys::ForeignScanState,
) {
    log!("---> re_scan_foreign_scan");
    // Reset or reinitialize scan state here if needed
}

// #[pg_guard]
// unsafe extern "C-unwind" fn add_foreign_update_targets(
//     root: *mut PlannerInfo,
//     rtindex: Index,
//     target_rte: *mut RangeTblEntry,
//     target_relation: Relation,
// ) {
//     log!("---> add_foreign_update_targets");
//     // Implementation for adding foreign update targets
// }

#[pg_guard]
unsafe extern "C-unwind" fn plan_foreign_modify(
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    resultRelation: Index,
    subplan_index: ::core::ffi::c_int,
) -> *mut pg_sys::List {
    log!("---> plan_foreign_modify");
    std::ptr::null_mut()
}

#[pg_guard]
extern "C-unwind" fn begin_foreign_modify(
    _mtstate: *mut pg_sys::ModifyTableState,
    _rinfo: *mut pg_sys::ResultRelInfo,
    _fdw_private: *mut pg_sys::List,
    _subplan_index: ::std::os::raw::c_int,
    _eflags: ::std::os::raw::c_int,
) {
    log!("---> begin_foreign_modify");
    // Implementation for beginning foreign modify
}

#[pg_guard]
extern "C-unwind" fn exec_foreign_insert(
    estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    slot: *mut pg_sys::TupleTableSlot,
    planSlot: *mut pg_sys::TupleTableSlot,
) -> *mut pg_sys::TupleTableSlot {
    log!("---> exec_foreign_insert");
     unsafe {
        //let tupdesc = (*slot).tts_tupleDescriptor;
        //let natts = (*tupdesc).natts as usize;
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
        (*slot).tts_tableOid = pg_sys::InvalidOid;
        slot
    }
}


#[pg_guard]
extern "C-unwind" fn exec_foreign_update(
    _estate: *mut pg_sys::EState,
    _rinfo: *mut pg_sys::ResultRelInfo,
    _slot: *mut pg_sys::TupleTableSlot,
    _planSlot: *mut pg_sys::TupleTableSlot,
) -> *mut pg_sys::TupleTableSlot {
    log!("---> exec_foreign_update");
    unsafe { (*_slot).tts_tableOid = pg_sys::InvalidOid };
    _slot
}

#[pg_guard]
extern "C-unwind" fn exec_foreign_delete(
    estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    slot: *mut pg_sys::TupleTableSlot,
    planSlot: *mut pg_sys::TupleTableSlot,
) -> *mut pg_sys::TupleTableSlot {
    log!("---> exec_foreign_delete");
    unsafe {
        let tupdesc = (*slot).tts_tupleDescriptor;
        let id_attr =  tuple_desc_attr(tupdesc, 0);
        let id_name = CStr::from_ptr((*id_attr).attname.data.as_ptr())
            .to_str()
            .unwrap_or("id")
            .to_string();
        
        let id_val = (*slot).tts_values.read();
        let str =  FromDatum::from_polymorphic_datum(id_val, false, (*id_attr).atttypid)
                        .unwrap_or_else(|| "<conversion error>".to_string());

        log!("Deleted column: Col {}, value: {}",id_name, str);

        let mut data = MEMORY_TABLE.write().unwrap();
        data.retain(|row| row.get(&id_name).map(|v| v.as_str()) != Some(str.as_ref()));

        (*slot).tts_tableOid = pg_sys::InvalidOid;
        slot
    }
}

unsafe fn tuple_table_slot_to_row(slot: *mut pg_sys::TupleTableSlot) -> Row {
    let tup_desc = PgTupleDesc::from_pg_copy((*slot).tts_tupleDescriptor);

    let mut should_free = false;
    let htup = pg_sys::ExecFetchSlotHeapTuple(slot, false, &mut should_free);
    let htup = PgBox::from_pg(htup);
    let mut row = Row::new();

    for (att_idx, attr) in tup_desc.iter().filter(|a| !a.attisdropped).enumerate() {
        let col = pgrx::name_data_to_str(&attr.attname);
        let attno = NonZeroUsize::new(att_idx + 1).unwrap();
        let cell: Option<Cell> = pgrx::htup::heap_getattr(&htup, attno, &tup_desc);
        row.push(col, cell);
    }

    row
}

#[pg_guard]
extern "C-unwind" fn end_foreign_modify(
    _estate: *mut pg_sys::EState,
    _rinfo: *mut pg_sys::ResultRelInfo,
) {
    log!("---> end_foreign_modify");
    // Implementation for ending foreign modify
}

#[inline]
unsafe fn tuple_desc_attr(tupdesc: pg_sys::TupleDesc, attnum: usize) -> *const pg_sys::FormData_pg_attribute {
     (*tupdesc).attrs.as_mut_ptr().add(attnum)
}

#[repr(C)]
struct RedisFdwState {
    row: i32,
    values: Vec<Datum>,
    nulls: Vec<bool>,
    tmp_ctx: MemoryContext,
}

impl RedisFdwState {
    fn new(tmp_ctx: MemoryContext) -> Self {
        RedisFdwState {
            row: 0,
            values: Vec::new(),
            nulls: Vec::new(),
            tmp_ctx
        }
    }
}


unsafe fn exec_clear_tuple(slot: *mut pg_sys::TupleTableSlot) {
    if let Some(clear) = (*(*slot).tts_ops).clear {
        clear(slot);
    }
}


#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema] 
mod tests {
    use pgrx_macros::pg_test;
    use std::ffi::CString;
    use pgrx::pg_sys::{self};
    use crate::default_fdw::*;
    
    #[cfg(any(feature = "pg13", feature = "pg14"))]
    #[pg_test]
    fn test_get_str_from_pgvalue_pg14() {

        let cstr = CString::new("hello").expect("CString::new failed");
        let val = pg_sys::Value {
            type_: pg_sys::NodeTag::T_String,
            val: pg_sys::ValUnion { str_: cstr.as_ptr() as *mut i8 },
        };
        let result = unsafe { pg_string_to_rust(&val as *const _ as *mut _) };
        assert_eq!(result, "hello");
    }

    #[cfg(any(feature = "pg15", feature = "pg16"))]
    #[pg_test]
    fn test_get_str_from_pgvalue_pg15() {

        let cstring = CString::from(c"hello");
        let pg_string = pg_sys::String {
            type_: pg_sys::NodeTag::T_String,
            sval: cstring.as_ptr() as *mut _,
        };

        let val = Value {
            type_: pg_sys::NodeTag::T_String,
            val: pg_sys::ValUnion { sval: pg_string} ,
        };

        let result = unsafe { pg_string_to_rust(&val as *const _ as *mut _) };

        // log!("act Debug result: {} ", result);
        assert_eq!(result, "\"hello\"");
    }
}


#[derive(Debug, Clone, Default)]
pub struct Row {
    /// column names
    pub cols: Vec<String>,

    /// column cell list, should match with cols
    pub cells: Vec<Option<Cell>>,
}

impl Row {
    /// Create an empty row
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a cell with column name to this row
    pub fn push(&mut self, col: &str, cell: Option<Cell>) {
        self.cols.push(col.to_owned());
        self.cells.push(cell);
    }
    
    pub fn iter(&self) -> Zip<Iter<'_, String>, Iter<'_, Option<Cell>>> {
        self.cols.iter().zip(self.cells.iter())
    }

    /// Remove a cell at the specified index
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut((&String, &Option<Cell>)) -> bool,
    {
        let keep: Vec<bool> = self.iter().map(f).collect();
        let mut iter = keep.iter();
        self.cols.retain(|_| *iter.next().unwrap_or(&true));
        iter = keep.iter();
        self.cells.retain(|_| *iter.next().unwrap_or(&true));
    }

}

#[derive(Debug)]
pub enum Cell {
    Bool(bool),
    I8(i8),
    I16(i16),
    F32(f32),
    I32(i32),
    F64(f64),
    I64(i64),
    Numeric(AnyNumeric),
    String(String),
    Date(Date),
    Time(Time),
    Timestamp(Timestamp),
    Timestamptz(TimestampWithTimeZone),
    Interval(Interval),
    BoolArray(Vec<Option<bool>>),
    I16Array(Vec<Option<i16>>),
    I32Array(Vec<Option<i32>>),
    I64Array(Vec<Option<i64>>),
    F32Array(Vec<Option<f32>>),
    F64Array(Vec<Option<f64>>),
    StringArray(Vec<Option<String>>),
}

impl FromDatum for Cell {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, typoid: Oid) -> Option<Self>
    where
        Self: Sized,
    {
        let oid = PgOid::from(typoid);
        match oid {
            PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
                bool::from_datum(datum, is_null).map(Cell::Bool)
            }
            PgOid::BuiltIn(PgBuiltInOids::CHAROID) => i8::from_datum(datum, is_null).map(Cell::I8),
            PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
                i16::from_datum(datum, is_null).map(Cell::I16)
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
                f32::from_datum(datum, is_null).map(Cell::F32)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
                i32::from_datum(datum, is_null).map(Cell::I32)
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
                f64::from_datum(datum, is_null).map(Cell::F64)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
                i64::from_datum(datum, is_null).map(Cell::I64)
            }
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                AnyNumeric::from_datum(datum, is_null).map(Cell::Numeric)
            }
            PgOid::BuiltIn(PgBuiltInOids::TEXTOID) => {
                String::from_datum(datum, is_null).map(Cell::String)
            }
            PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                Date::from_datum(datum, is_null).map(Cell::Date)
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => {
                Time::from_datum(datum, is_null).map(Cell::Time)
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                Timestamp::from_datum(datum, is_null).map(Cell::Timestamp)
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                TimestampWithTimeZone::from_datum(datum, is_null).map(Cell::Timestamptz)
            }
            PgOid::BuiltIn(PgBuiltInOids::INTERVALOID) => {
                Interval::from_datum(datum, is_null).map(Cell::Interval)
            }
            PgOid::BuiltIn(PgBuiltInOids::BOOLARRAYOID) => {
                Vec::<Option<bool>>::from_datum(datum, false).map(Cell::BoolArray)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT2ARRAYOID) => {
                Vec::<Option<i16>>::from_datum(datum, false).map(Cell::I16Array)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT4ARRAYOID) => {
                Vec::<Option<i32>>::from_datum(datum, false).map(Cell::I32Array)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8ARRAYOID) => {
                Vec::<Option<i64>>::from_datum(datum, false).map(Cell::I64Array)
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4ARRAYOID) => {
                Vec::<Option<f32>>::from_datum(datum, false).map(Cell::F32Array)
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8ARRAYOID) => {
                Vec::<Option<f64>>::from_datum(datum, false).map(Cell::F64Array)
            }
            PgOid::BuiltIn(PgBuiltInOids::TEXTARRAYOID) => {
                Vec::<Option<String>>::from_datum(datum, false).map(Cell::StringArray)
            }
            _ => None,
        }
    }
}



impl IntoDatum for Cell {
    fn into_datum(self) -> Option<Datum> {
        match self {
            Cell::Bool(v) => v.into_datum(),
            Cell::I8(v) => v.into_datum(),
            Cell::I16(v) => v.into_datum(),
            Cell::F32(v) => v.into_datum(),
            Cell::I32(v) => v.into_datum(),
            Cell::F64(v) => v.into_datum(),
            Cell::I64(v) => v.into_datum(),
            Cell::Numeric(v) => v.into_datum(),
            Cell::String(v) => v.into_datum(),
            Cell::Date(v) => v.into_datum(),
            Cell::Time(v) => v.into_datum(),
            Cell::Timestamp(v) => v.into_datum(),
            Cell::Timestamptz(v) => v.into_datum(),
            Cell::Interval(v) => v.into_datum(),
            Cell::BoolArray(v) => v.into_datum(),
            Cell::I16Array(v) => v.into_datum(),
            Cell::I32Array(v) => v.into_datum(),
            Cell::I64Array(v) => v.into_datum(),
            Cell::F32Array(v) => v.into_datum(),
            Cell::F64Array(v) => v.into_datum(),
            Cell::StringArray(v) => v.into_datum(),
        }
    }

    fn type_oid() -> Oid {
        Oid::INVALID
    }

    fn is_compatible_with(other: Oid) -> bool {
        Self::type_oid() == other
            || other == pg_sys::BOOLOID
            || other == pg_sys::CHAROID
            || other == pg_sys::INT2OID
            || other == pg_sys::FLOAT4OID
            || other == pg_sys::INT4OID
            || other == pg_sys::FLOAT8OID
            || other == pg_sys::INT8OID
            || other == pg_sys::NUMERICOID
            || other == pg_sys::TEXTOID
            || other == pg_sys::DATEOID
            || other == pg_sys::TIMEOID
            || other == pg_sys::TIMESTAMPOID
            || other == pg_sys::TIMESTAMPTZOID
            || other == pg_sys::INTERVALOID
            || other == pg_sys::JSONBOID
            || other == pg_sys::BYTEAOID
            || other == pg_sys::UUIDOID
            || other == pg_sys::BOOLARRAYOID
            || other == pg_sys::INT2ARRAYOID
            || other == pg_sys::INT4ARRAYOID
            || other == pg_sys::INT8ARRAYOID
            || other == pg_sys::FLOAT4ARRAYOID
            || other == pg_sys::FLOAT8ARRAYOID
            || other == pg_sys::TEXTARRAYOID
    }
}


impl Clone for Cell {
    fn clone(&self) -> Self {
        match self {
            Cell::Bool(v) => Cell::Bool(*v),
            Cell::I8(v) => Cell::I8(*v),
            Cell::I16(v) => Cell::I16(*v),
            Cell::F32(v) => Cell::F32(*v),
            Cell::I32(v) => Cell::I32(*v),
            Cell::F64(v) => Cell::F64(*v),
            Cell::I64(v) => Cell::I64(*v),
            Cell::Numeric(v) => Cell::Numeric(v.clone()),
            Cell::String(v) => Cell::String(v.clone()),
            Cell::Date(v) => Cell::Date(*v),
            Cell::Time(v) => Cell::Time(*v),
            Cell::Timestamp(v) => Cell::Timestamp(*v),
            Cell::Timestamptz(v) => Cell::Timestamptz(*v),
            Cell::Interval(v) => Cell::Interval(*v),
            Cell::BoolArray(v) => Cell::BoolArray(v.clone()),
            Cell::I16Array(v) => Cell::I16Array(v.clone()),
            Cell::I32Array(v) => Cell::I32Array(v.clone()),
            Cell::I64Array(v) => Cell::I64Array(v.clone()),
            Cell::F32Array(v) => Cell::F32Array(v.clone()),
            Cell::F64Array(v) => Cell::F64Array(v.clone()),
            Cell::StringArray(v) => Cell::StringArray(v.clone()),
        }
    }
}


impl fmt::Display for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cell::Bool(v) => write!(f, "{}", v),
            Cell::I8(v) => write!(f, "{}", v),
            Cell::I16(v) => write!(f, "{}", v),
            Cell::F32(v) => write!(f, "{}", v),
            Cell::I32(v) => write!(f, "{}", v),
            Cell::F64(v) => write!(f, "{}", v),
            Cell::I64(v) => write!(f, "{}", v),
            Cell::Numeric(v) => write!(f, "{}", v),
            Cell::String(v) => write!(f, "'{}'", v),
            Cell::Date(v) => unsafe {
                let dt =
                    fcinfo::direct_function_call_as_datum(pg_sys::date_out, &[(*v).into_datum()])
                        .expect("cell should be a valid date");
                let dt_cstr = CStr::from_ptr(dt.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    dt_cstr.to_str().expect("date should be a valid string")
                )
            },
            Cell::Time(v) => unsafe {
                let ts =
                    fcinfo::direct_function_call_as_datum(pg_sys::time_out, &[(*v).into_datum()])
                        .expect("cell should be a valid time");
                let ts_cstr = CStr::from_ptr(ts.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    ts_cstr.to_str().expect("time hould be a valid string")
                )
            },
            Cell::Timestamp(v) => unsafe {
                let ts = fcinfo::direct_function_call_as_datum(
                    pg_sys::timestamp_out,
                    &[(*v).into_datum()],
                )
                .expect("cell should be a valid timestamp");
                let ts_cstr = CStr::from_ptr(ts.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    ts_cstr
                        .to_str()
                        .expect("timestamp should be a valid string")
                )
            },
            Cell::Timestamptz(v) => unsafe {
                let ts = fcinfo::direct_function_call_as_datum(
                    pg_sys::timestamptz_out,
                    &[(*v).into_datum()],
                )
                .expect("cell should be a valid timestamptz");
                let ts_cstr = CStr::from_ptr(ts.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    ts_cstr
                        .to_str()
                        .expect("timestamptz should be a valid string")
                )
            },
            Cell::Interval(v) => write!(f, "{}", v),
            Cell::BoolArray(v) => write_array(v, f),
            Cell::I16Array(v) => write_array(v, f),
            Cell::I32Array(v) => write_array(v, f),
            Cell::I64Array(v) => write_array(v, f),
            Cell::F32Array(v) => write_array(v, f),
            Cell::F64Array(v) => write_array(v, f),
            Cell::StringArray(v) => write_array(v, f),
        }
    }
}

fn write_array<T: std::fmt::Display>(
    array: &[Option<T>],
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    let res = array
        .iter()
        .map(|e| match e {
            Some(val) => format!("{}", val),
            None => "null".to_owned(),
        })
        .collect::<Vec<String>>()
        .join(",");
    write!(f, "[{}]", res)
}

const ROOT_MEMCTX_NAME: &str = "WrappersRootMemCtx";

unsafe fn create_wrappers_memctx(name: &str) -> MemoryContext {
    let mut root = ensure_root_wrappers_memctx();
    let name = root.switch_to(|_| name.as_pg_cstr());
    pg_sys::AllocSetContextCreateExtended(
        root.value(),
        name,
        pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
        pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
        pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
    )
}

unsafe fn ensure_root_wrappers_memctx() -> PgMemoryContexts {
    find_memctx_under(ROOT_MEMCTX_NAME, PgMemoryContexts::CacheMemoryContext).unwrap_or_else(|| {
        let name = PgMemoryContexts::CacheMemoryContext.pstrdup(ROOT_MEMCTX_NAME);
        let ctx = pg_sys::AllocSetContextCreateExtended(
            PgMemoryContexts::CacheMemoryContext.value(),
            name,
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );
        PgMemoryContexts::For(ctx)
    })
}

// search memory context by name under specified MemoryContext
unsafe fn find_memctx_under(name: &str, under: PgMemoryContexts) -> Option<PgMemoryContexts> {
    let mut ctx = (*under.value()).firstchild;
    while !ctx.is_null() {
        if let Ok(ctx_name) = std::ffi::CStr::from_ptr((*ctx).name).to_str() {
            if ctx_name == name {
                return Some(PgMemoryContexts::For(ctx));
            }
        }
        ctx = (*ctx).nextchild;
    }
    None
}
