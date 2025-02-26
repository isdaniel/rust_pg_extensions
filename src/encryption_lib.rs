use aes_gcm::KeyInit;
use md5::digest::DynDigest;
use pgx::*;
use md5::{Digest, Md5};
use sha1::Sha1;
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

//TableIterator<'static, (name!(status, Option<i32>), name!(stdout, String))>
// #[pg_extern]
// fn test_tuples() -> TableIterator<'static,(name!(val1,i32),name!(val2,String))>{
//     TableIterator::once((
//         200,
//         "Hello World!".to_string()
//     ))
// }
