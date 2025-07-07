use std::{collections::HashMap, fs::File};

use crate::fdw::utils_share::cell::Cell;


#[repr(C)]
#[derive(Debug)]
pub struct CsvFdwState {
    pub options : HashMap<String, String>,
    pub csv_reader : Option<csv::Reader<File>>,
    pub header_name_to_colno: Vec<usize>,
    pub filters: Vec<(usize, Cell)>,
}


impl CsvFdwState {
    pub fn new() -> Self {
        CsvFdwState {
            header_name_to_colno : Vec::default(),
            options : HashMap::default(),
            csv_reader : Option::None,
            filters : Vec::default(),
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


pub fn get_csv_reader(options: &HashMap<String, String>) -> csv::Reader<File> {
    let file_path = options.get("filepath").expect("CSV file path must be provided in options");
    let file = File::open(file_path).expect(&format!("Failed to open CSV file: {}", file_path));
    csv::Reader::from_reader(file)
}