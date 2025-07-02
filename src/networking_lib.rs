use pgrx::{pg_extern};

#[pg_extern]
pub fn get_server_hostname() -> String {
    hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "Unknown".to_string())
}

#[pg_extern]
pub fn get_server_ip() -> Vec<String> {
    let mut ips = Vec::new();
    if let Ok(interfaces) = get_if_addrs::get_if_addrs() {
        for iface in interfaces {
            if !iface.is_loopback() {
                ips.push(iface.ip().to_string());
            }
        }
    }
    ips
}


#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema] 
mod tests {
    use pgrx_macros::pg_test;
    use crate::networking_lib::*;
    
     #[pg_test]
    fn test_get_server_hostname() {
        let hostname = get_server_hostname();
        
        assert!(!hostname.is_empty(), "Hostname should not be empty");
        assert_ne!(hostname, "Unknown", "Hostname should not be 'Unknown'");
    }

    #[pg_test]
    fn test_get_server_ip() {
        let ips = get_server_ip();
        // It could be empty if there are no non-loopback interfaces, but we can check type
        for ip in &ips {
            assert!(
                ip.parse::<std::net::IpAddr>().is_ok(),
                "IP '{}' should be a valid IP address",
                ip
            );
        }
    }
}