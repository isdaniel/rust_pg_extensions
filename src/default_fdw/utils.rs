use std::{collections::HashMap, ffi::CStr, num::NonZeroUsize};
use pgrx::{list, pg_sys::{self, defGetString, list_concat}, PgBox, PgTupleDesc};

#[cfg(any(feature = "pg13", feature = "pg14"))]
use pgrx::pg_sys::Value;

#[cfg(any(feature = "pg15", feature = "pg16"))] 
#[repr(C)]
pub struct Value {
    pub type_: pgrx::pg_sys::NodeTag,
    pub val: pgrx::pg_sys::ValUnion,
}

#[cfg(any(feature = "pg13", feature = "pg14"))]
pub unsafe fn pg_string_to_rust(val_value: *mut Value) -> String {
    CStr::from_ptr((*val_value).val.str_)
        .to_str()
        .unwrap_or_default().to_string()
}

#[cfg(any(feature = "pg15", feature = "pg16"))] 
pub unsafe fn pg_string_to_rust(val_value: *mut Value) -> String {
    (*val_value).val.sval.to_string()
}

pub unsafe fn get_foreign_table_options(relid: pgrx::pg_sys::Oid) -> HashMap<String, String> {
    let mut options = HashMap::new();
    let table = pg_sys::GetForeignTable(relid);
    let server = pg_sys::GetForeignServer((*table).serverid);
    let wrapper = pg_sys::GetForeignDataWrapper((*server).fdwid);

    let mut opts_list = std::ptr::null_mut();

    opts_list = list_concat(opts_list, (*wrapper).options);
    opts_list = list_concat(opts_list, (*server).options);
    opts_list = list_concat(opts_list, (*table).options);

    if opts_list.is_null() {
        return options;
    }

    let len = (*opts_list).length;
    let cells = (*opts_list).elements;

    for i in 0..len {
        let cell_ptr = cells.offset(i as isize);
        let def_elem =  (*cell_ptr).ptr_value as *mut pgrx::pg_sys::DefElem;
        
        if def_elem.is_null() {
            continue;
        }

        let def_name = CStr::from_ptr((*def_elem).defname)
                .to_str()
                .unwrap_or_default();

        let def_val_node = (*def_elem).arg;
        if !def_val_node.is_null() && (*def_val_node).type_ == pgrx::pg_sys::NodeTag::T_String {
            let val_value: *mut Value = def_val_node as *mut Value;
            
            let val = pg_string_to_rust(val_value);
            options.insert(def_name.to_string(), val.to_string());
        }
    }

    options
}

/// Get the attribute descriptor for a given attribute number in a tuple descriptor
/// This function is unsafe because it dereferences raw pointers and assumes that the tuple descriptor is valid and properly initialized.
/// # Arguments
/// * `tupdesc`: A pointer to a `TupleDesc` structure.
/// * `attnum`: The attribute number (1-based index) for which to retrieve the attribute descriptor.
/// # Returns
/// A pointer to the `FormData_pg_attribute` structure for the specified attribute number.
/// # Note
/// The attribute number is 1-based, meaning that `attnum = 1` corresponds to the first attribute in the tuple descriptor.

pub unsafe fn tuple_desc_attr(tupdesc: pgrx::pg_sys::TupleDesc, attnum: usize) -> *const pgrx::pg_sys::FormData_pg_attribute {
     (*tupdesc).attrs.as_mut_ptr().add(attnum)
}

pub unsafe fn exec_clear_tuple(slot: *mut pgrx::pg_sys::TupleTableSlot) {
    if let Some(clear) = (*(*slot).tts_ops).clear {
        clear(slot);
    }
}

/// Convert a `TupleTableSlot` to a `Row`
/// This function is unsafe because it dereferences raw pointers and assumes that the `TupleTableSlot` is valid and properly initialized.
/// # Arguments
/// * `slot`: A pointer to a `TupleTableSlot` structure.
/// # Returns
/// A `Row` containing the data from the `TupleTableSlot`. The row will contain cells for each attribute in the tuple descriptor, excluding dropped attributes.
pub unsafe fn tuple_table_slot_to_row(slot: *mut pgrx::pg_sys::TupleTableSlot) -> crate::default_fdw::Row {
    use crate::default_fdw::{Row, Cell};
    
    let tup_desc = PgTupleDesc::from_pg_copy((*slot).tts_tupleDescriptor);

    let mut should_free = false;
    let htup = pgrx::pg_sys::ExecFetchSlotHeapTuple(slot, false, &mut should_free);
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

/// Convert a C string pointer to a Rust String
/// # Safety
/// This function is unsafe because it dereferences a raw pointer. Ensure that the pointer is valid and points to a null-terminated C string.
/// Convert a C string pointer to a Rust String
/// # Safety
/// This function is unsafe because it dereferences a raw pointer. Ensure that the pointer is valid and points to a null-terminated C string.
/// # Arguments
/// * `c_str`: A pointer to a null-terminated C string.
/// 
/// # Returns
/// A Rust `String` containing the contents of the C string. If the pointer is null, an empty string is returned.
pub fn string_from_cstr(c_str: *const i8) -> String {
    if c_str.is_null() {
        return String::new();
    }
    unsafe { CStr::from_ptr(c_str).to_string_lossy().into_owned() }
}