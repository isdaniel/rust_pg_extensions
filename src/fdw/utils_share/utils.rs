use std::{collections::HashMap, ffi::{c_void, CStr, CString}, num::NonZeroUsize};
use pgrx::{list, memcx::{self, MemCx}, pg_sys::{self, defGetString, fmgr_info, getTypeInputInfo, list_concat, Datum, FmgrInfo, InputFunctionCall, Oid}, PgBox, PgTupleDesc};
use crate::fdw::utils_share::row::Row;
use crate::fdw::utils_share::cell::Cell;

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
    let opts_list = get_options_from_fdw(relid);
    if opts_list.is_null() {
        return options;
    }

    memcx::current_context(|mcx| {
        let opts_list = pg_list_to_rust_list::<*mut c_void>(opts_list, mcx);
        for option in opts_list.iter() {
            let def_elem = option.cast::<pg_sys::DefElem>();
            if def_elem.is_null() {
                continue;
            }
            options.insert(
                string_from_cstr((*def_elem).defname),
                string_from_cstr(defGetString(def_elem)),
            );
        }
    });
    options
}

unsafe fn get_options_from_fdw(relid: Oid) -> *mut pg_sys::List {
    let table = pg_sys::GetForeignTable(relid);
    let server = pg_sys::GetForeignServer((*table).serverid);
    let wrapper = pg_sys::GetForeignDataWrapper((*server).fdwid);
    //pg_sys::GetUserMapping(pg_sys::GetUserId(), (*server).fdwid);
    let mut opts_list = std::ptr::null_mut();
    
    opts_list = list_concat(opts_list, (*wrapper).options);
    opts_list = list_concat(opts_list, (*server).options);
    opts_list = list_concat(opts_list, (*table).options);
    opts_list
}


// fn another_way_extract_options() {
    // let len = (*opts_list).length;
    // let cells = (*opts_list).elements;
    
    // for i in 0..len {
    //     let cell_ptr = cells.offset(i as isize);
    //     let def_elem =  (*cell_ptr).ptr_value as *mut pgrx::pg_sys::DefElem;
        
    //     if def_elem.is_null() {
    //         continue;
    //     }

    //     let def_val_node = (*def_elem).arg;
    //     if !def_val_node.is_null() && (*def_val_node).type_ == pgrx::pg_sys::NodeTag::T_String {
    //         let val_value: *mut Value = def_val_node as *mut Value;            

    //         let val = pg_string_to_rust(val_value);
    //         let name = string_from_cstr((*def_elem).defname);
    //         options.insert(name, val);
    //     }
    // }
//}

/// Get the attribute descriptor for a given attribute number in a tuple descriptor
/// This function is unsafe because it dereferences raw pointers and assumes that the tuple descriptor is valid and properly initialized.
/// # Arguments
/// * `tupdesc`: A pointer to a `TupleDesc` structure.
/// * `attnum`: The attribute number (1-based index) for which to retrieve the attribute descriptor.
/// # Returns
/// A pointer to the `FormData_pg_attribute` structure for the specified attribute number.
/// # Note
/// The attribute number is 1-based, meaning that `attnum = 1` corresponds to the first attribute in the tuple descriptor.

pub unsafe fn tuple_desc_attr(tupdesc: pg_sys::TupleDesc, attnum: usize) -> *const pg_sys::FormData_pg_attribute {
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
pub unsafe fn tuple_table_slot_to_row(slot: *mut pgrx::pg_sys::TupleTableSlot) -> crate::fdw::utils_share::row::Row {

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
    unsafe { CStr::from_ptr(c_str).to_string_lossy().trim_end_matches('\0').to_string() }
}



/// Convert a string to a Datum using the type input function for the specified Oid
/// This function is unsafe because it dereferences raw pointers and assumes that the type input function is
/// valid for the given Oid. Ensure that the Oid corresponds to a valid data type in PostgreSQL.
/// # Arguments
/// * `value_str`: A string slice containing the value to convert.
/// * `typid`: The Oid of the data type to which the string should be
/// converted.
/// # Returns
/// A `Datum` representing the converted value. If the input string is empty, a null
/// `Datum` is returned.
/// # Note
/// This function uses the PostgreSQL type input function to convert the string to a `Datum`.
/// It is intended for use with PostgreSQL data types that have a defined input function.
/// The function retrieves the type input information for the specified Oid and calls the input function to
/// perform the conversion. If the input string is empty, it returns a null `Datum`.
/// This function is unsafe because it relies on the PostgreSQL C API and assumes that the input
/// function for the specified Oid is correctly defined and available in the PostgreSQL environment.
/// It is the caller's responsibility to ensure that the Oid corresponds to a valid data type
/// and that the input function is properly registered in the PostgreSQL system.
pub unsafe fn get_datum(value_str: &str, typid: Oid) -> Datum {    
    if value_str.is_empty() {
        return Datum::null();
    }

    let c_value = CString::new(value_str).unwrap();
    let mut typeinput = Oid::default();
    let mut typeioparam = Oid::default();
    let mut finfo = FmgrInfo::default();
    getTypeInputInfo(typid, &mut typeinput, &mut typeioparam);
    fmgr_info(typeinput, &mut finfo);
    let res = InputFunctionCall(&mut finfo, c_value.as_ptr().cast_mut(), typeioparam, -1);
    res
}

unsafe fn pg_list_to_rust_list<'a, T: list::Enlist>(
    list: *mut pg_sys::List,
    mcx: &'a MemCx<'_>,
) -> list::List<'a, T> {
    list::List::<T>::downcast_ptr_in_memcx(list, mcx).expect("Failed to downcast list pointer")
}