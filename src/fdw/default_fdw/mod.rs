mod handlers;

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema] 
mod tests {
    use std::ffi::CString;
    use pgrx_macros::pg_test;
    use pgrx::pg_sys;
    use crate::fdw::utils_share::utils::*;

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
