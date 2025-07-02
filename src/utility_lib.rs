use aes_gcm::KeyInit;
use md5::digest::DynDigest;
use pgrx::*;
use md5::{Digest, Md5};
use sha1::{Sha1};
use sha2::Sha256;
use hex::{encode};
use aes_gcm::{
    aead::{Aead, generic_array::GenericArray},
    Aes256Gcm, 
    Nonce      
};

#[pg_extern]
pub fn compute_hash(input: &str,hash_type:&str) -> String{
    let mut hasher:Box<dyn DynDigest>= match hash_type.to_lowercase().as_str() {
        "md5" => Box::new(Md5::new()),
        "sha1" => Box::new(Sha1::new()),
        "sha256" => Box::new(Sha256::new()),
        _ => return format!("Unsupported hash type: {}", hash_type),
    };

    hasher.update(input.as_bytes());
    encode(hasher.finalize())
}

const FIXED_NONCE:[u8;12] =  [
    0x6a, 0x3d, 0x1f, 0xb8, 0x4e, 0x7a, 
    0x93, 0x2d, 0x5f, 0x7c, 0x82, 0x1b
];

#[pg_extern]
pub fn data_encrypt(key: &[u8], plaintext: &str) -> Vec<u8> {
    let cipher = Aes256Gcm::new(GenericArray::from_slice(key));
    let nonce = Nonce::from_slice(&FIXED_NONCE);
    cipher.encrypt(nonce, plaintext.as_bytes()).unwrap()
}

#[pg_extern]
pub fn data_decrypt(key: &[u8], cipher_buffer: &[u8]) -> String{
    let cipher = Aes256Gcm::new(GenericArray::from_slice(key));
    let nonce = Nonce::from_slice(&FIXED_NONCE);
    String::from_utf8(cipher.decrypt(nonce, cipher_buffer).expect("Decryption failed")).unwrap()
}


#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema] 
mod tests {
    use pgrx::prelude::*;
    use crate::utility_lib::{compute_hash,data_decrypt,data_encrypt};
    
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
