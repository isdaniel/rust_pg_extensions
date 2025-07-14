#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema] 
mod tests {
    use std::{ffi::CString, fmt::format};
    use pgrx_macros::pg_test;
    use pgrx::{pg_sys, Spi};
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
    
    #[cfg(not(feature = "pg13"))]
    #[pg_test]
    fn default_fdw_insert_and_select() {
        Spi::connect_mut(|c| {
            init_fdw_table(c);

            let rows = c
                .select("SELECT * FROM hello ORDER BY id", None, &[])
                .unwrap();

            let results: Vec<(i64, Option<String>)> = rows
                .map(|row| {
                    let id = row.get::<i64>(1).unwrap().unwrap();
                    let col = row.get::<String>(2).unwrap();
                    (id, col)
                })
                .collect();

            assert_eq!(
                results,
                vec![
                    (1, Some("'test1'".to_string())),
                    (2, Some("'test2'".to_string())),
                    (21, Some("'test21'".to_string())),
                    (123, Some("NULL".to_string())),
                ]
            );
        });
    }

    #[cfg(not(feature = "pg13"))]
    #[pg_test]
    fn default_fdw_insert_and_update() {
        Spi::connect_mut(|c| {
            init_fdw_table(c);

            c.update(
                r#"update hello set col = 'update_val' where id = 1"#,
                None,
                &[],
            )
            .unwrap();

            assert_eq!(
                get_hello_result(c),
                vec![
                    (1, Some("'update_val'".to_string())),
                    (2, Some("'test2'".to_string())),
                    (21, Some("'test21'".to_string())),
                    (123, Some("NULL".to_string())),
                ]
            );
            let update_val = "aaaaa";
            c.update(
                format!(r#"update hello set col = '{}' "#, update_val).as_str(),
                None,
                &[],
            )
            .unwrap();

            let expect = "'aaaaa'";
            assert_eq!(
                get_hello_result(c),
                vec![
                    (1, Some(expect.to_string())),
                    (2, Some(expect.to_string())),
                    (21, Some(expect.to_string())),
                    (123, Some(expect.to_string())),
                ]
            );
        });
    }

    fn get_hello_result(c: &mut pgrx::spi::SpiClient<'_>) -> Vec<(i64, Option<String>)> {
        let rows = c
            .select("SELECT * FROM hello ORDER BY id", None, &[])
            .unwrap();
    
        let results: Vec<(i64, Option<String>)> = rows
            .map(|row| {
                let id = row.get::<i64>(1).unwrap().unwrap();
                let col = row.get::<String>(2).unwrap();
                (id, col)
            })
            .collect();
        
        results
    }
    
    fn init_fdw_table(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"create foreign data wrapper default_wrapper handler default_fdw_handler;"#,
            None,
            &[],
        )
        .unwrap();
        c.update(
            r#"create server my_default_server
                    foreign data wrapper default_wrapper
                    options (
                    foo 'bar'
                    );"#,
            None,
            &[],
        )
        .unwrap();
    
        c.update(
            r#"
                create foreign table hello (
                id bigint,
                col text
                )
                server my_default_server options (
                    foo 'bar'
                );
             "#,
            None,
            &[],
        )
        .unwrap();
    
        c.update(
            r#"
                insert into hello values (1,'test1');
                insert into hello values (2,'test2');
                insert into hello values (21,'test21');
                insert into hello values (123,NULL);
             "#,
            None,
            &[],
        )
        .unwrap();
    }
}




