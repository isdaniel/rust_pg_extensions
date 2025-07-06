use std::{collections::HashMap, fs::File};


#[repr(C)]
pub struct CsvFdwState {
    pub options : HashMap<String, String>,
    pub csv_reader : csv::Reader<File>,
}


impl CsvFdwState {
    pub fn new(options: HashMap<String, String>,
               csv_reader : csv::Reader<File> ) -> Self {
        CsvFdwState {
            options,
            csv_reader
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