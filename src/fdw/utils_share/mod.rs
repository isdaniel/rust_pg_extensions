pub mod cell;
pub mod row;
pub mod memory;
pub mod utils;


#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema] 
mod tests {
    use std::collections::HashMap;
    use csv::StringRecord;
    use crate::fdw::utils_share::utils::build_header_index_map;
    
    #[test]
    fn test_build_header_index_map_valid() {
        let headers = StringRecord::from(vec!["id", "name", "email"]);
        let mut attr_map = HashMap::new();
        attr_map.insert("id".to_string(), 0);
        attr_map.insert("name".to_string(), 1);
        attr_map.insert("email".to_string(), 2);

        let result = build_header_index_map(&headers, &attr_map);
        assert_eq!(result, vec![0, 1, 2]);
    }

    #[test]
    #[should_panic(expected = "CSV header column 'age' not found in relation attributes")]
    fn test_build_header_index_map_missing_column() {
        let headers = StringRecord::from(vec!["id", "name", "age"]);
        let mut attr_map = HashMap::new();
        attr_map.insert("id".to_string(), 0);
        attr_map.insert("name".to_string(), 1);

        build_header_index_map(&headers, &attr_map);
    }

    #[test]
    fn test_build_header_index_map_column_order_change() {
        let headers = StringRecord::from(vec!["email", "id", "name"]);
        let mut attr_map = HashMap::new();
        attr_map.insert("id".to_string(), 0);
        attr_map.insert("name".to_string(), 1);
        attr_map.insert("email".to_string(), 2);

        let result = build_header_index_map(&headers, &attr_map);
        assert_eq!(result, vec![2, 0, 1]);
    }
}

