use std::{collections::HashMap, fs::File};

use crate::fdw::utils_share::cell::Cell;


#[repr(C)]
#[derive(Debug)]
pub struct CsvFdwState {
    pub options : HashMap<String, String>,
    pub csv_reader : Option<csv::Reader<File>>,
    pub header_name_to_colno: Vec<usize>,
    pub filters: Vec<(usize, Cell)>,
    pub file_path: String,
}


impl CsvFdwState {
    pub fn new() -> Self {
        CsvFdwState {
            header_name_to_colno : Vec::default(),
            options : HashMap::default(),
            csv_reader : Option::None,
            filters : Vec::default(),
            file_path: String::new(),
        }
    }
}

#[derive(Debug)]
struct User {
    id: u32,
    name: String,
    email: String,
    age: u8,
}


pub fn get_csv_reader(file_path:&str) -> csv::Reader<File> {
    let file = File::open(file_path).expect(&format!("Failed to open CSV file: {}", file_path));
    csv::Reader::from_reader(file)
}