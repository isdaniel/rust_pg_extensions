use std::collections::HashMap;

use pgrx::pg_sys::{Oid, Datum, MemoryContext};

#[repr(C)]
pub struct RedisFdwState {
    pub row_count: usize,
    pub values: Vec<Datum>,
    pub nulls: Vec<bool>,
    pub tmp_ctx: MemoryContext,
    pub header_name_to_colno: HashMap<String, usize> ,
}

#[repr(C)]
pub struct FdwModifyState {
    // row id attribute number and type id
    pub rowid_name: String,
    pub rowid_attno: pgrx::pg_sys::AttrNumber,
    pub rowid_typid: Oid,

    // temporary memory context per foreign table, created under Wrappers root
    // memory context
    pub tmp_ctx: MemoryContext,

    #[cfg(feature = "pg13")]
    pub update_cols: Vec<String>,
}

impl RedisFdwState {
    pub fn new(tmp_ctx: MemoryContext) -> Self {
        RedisFdwState {
            row_count: 0,
            values: Vec::new(),
            nulls: Vec::new(),
            tmp_ctx,
            header_name_to_colno : HashMap::default()
        }
    }
}

impl FdwModifyState {
    pub fn new(_ftable_id: pgrx::pg_sys::Oid, tmp_ctx: MemoryContext) -> Self {
       Self {
            rowid_name: String::default(),
            rowid_attno: 0,
            rowid_typid: Oid::INVALID,
            tmp_ctx,
            #[cfg(feature = "pg13")]
            update_cols: Vec::new(),
        }
    }
}
