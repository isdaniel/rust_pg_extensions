use aes_gcm::KeyInit;
use md5::digest::DynDigest;
use pgx::*;
use serde::{Deserialize, Serialize};
use std::ffi::CStr;
use std::fmt::{Display, Error, Formatter};
use std::process::Command;
use std::str;
use md5::{Digest, Md5};
use sha1::Sha1;
use sha2::Sha256;
use hex::{encode};
use aes_gcm::{
    aead::{Aead, generic_array::GenericArray},
    Aes256Gcm, 
    Nonce      
};
use base64::{Engine as _, engine::general_purpose};


#[pg_extern]
fn compute_hash(input: &str,hash_type:&str) -> String{
    let mut hasher:Box<dyn DynDigest>= match hash_type.to_lowercase().as_str() {
        "md5" => Box::new(Md5::new()),
        "sha1" => Box::new(Sha1::new()),
        "sha256" => Box::new(Sha256::new()),
        _ => return format!("Unsupported hash type: {}", hash_type),
    };

    hasher.update(input.as_bytes());
    encode(hasher.finalize())
}

#[pg_extern]
fn data_encrypt(key: &[u8], plaintext: &str) -> String{
    let cipher = Aes256Gcm::new(GenericArray::from_slice(key));
    let nonce_bytes: [u8; 12] = [
        0x6a, 0x3d, 0x1f, 0xb8, 0x4e, 0x7a, 
        0x93, 0x2d, 0x5f, 0x7c, 0x82, 0x1b
    ];
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher.encrypt(nonce, plaintext.as_bytes()).unwrap();
    general_purpose::STANDARD.encode(ciphertext)
}

#[pg_extern]
fn data_decrypt(key: &[u8], ciphertext: &str) -> String{
    let nonce: [u8; 12]= [
        0x6a, 0x3d, 0x1f, 0xb8, 0x4e, 0x7a, 
        0x93, 0x2d, 0x5f, 0x7c, 0x82, 0x1b
    ];
    let cipher_buffer = general_purpose::STANDARD.decode(ciphertext).expect("Base64 decode failed...");
    let cipher = Aes256Gcm::new(GenericArray::from_slice(key));
    let nonce = Nonce::from_slice(&nonce);
    String::from_utf8(cipher.decrypt(nonce, cipher_buffer.as_slice()).expect("Decryption failed")).unwrap()
}
