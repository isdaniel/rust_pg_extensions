use pgrx::{pg_extern};

#[pg_extern]
fn get_server_hostname() -> String {
    hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "Unknown".to_string())
}

#[pg_extern]
fn get_server_ip() -> Vec<String> {
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
