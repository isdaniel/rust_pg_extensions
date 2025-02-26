use pgx::prelude::*;

mod encryption_lib;
pgx::pg_module_magic!();


#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::prelude::*;
    use crate::encryption_lib::{compute_hash,data_decrypt,data_encrypt};

    #[pg_test]
    fn test_md5_hashing() {
        assert_eq!("fc3ff98e8c6a0d3087d515c0473f8677", compute_hash("hello world!","md5"));
    }

    #[pg_test]
    fn test_sha1_hashing() {
        assert_eq!("430ce34d020724ed75a196dfc2ad67c77772d169", compute_hash("hello world!","sha1"));
    }

    #[pg_test]
    fn test_sha256_hashing() {
        assert_eq!("7509e5bda0c762d2bac7f90d758b5b2263fa01ccbc542ab5e3df163be08e6ca9", compute_hash("hello world!","sha256"));
    }

    #[pg_test]
    fn test_data_encryption() {
        let key = "01234567890123456789012345678901".as_bytes();
        let plain_text = "Hello World!!!~~!@#";
        let byte_buffer = data_encrypt(key,plain_text);
        let expect = data_decrypt(key,byte_buffer.as_slice());
        assert_eq!(plain_text, expect);
    }
}

/// This module is required by `cargo pgx test` invocations. 
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
