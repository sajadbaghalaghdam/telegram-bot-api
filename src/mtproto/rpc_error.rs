//! Parsing of RPC error texts (`FLOOD_WAIT_x`, `*_MIGRATE_x`).

/// Extracts the decimal number following `prefix` in `err_msg`.
fn number_after<T: std::str::FromStr>(err_msg: &str, prefix: &str) -> Option<Option<T>> {
    let idx = err_msg.find(prefix)?;
    let rest = &err_msg[idx + prefix.len()..];
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    Some(rest[..end].parse::<T>().ok())
}

/// DC id from `USER_MIGRATE_x` / `FILE_MIGRATE_x` / `PHONE_MIGRATE_x` / `NETWORK_MIGRATE_x`.
pub fn parse_migrate_dc(err_msg: &str) -> Option<i32> {
    for prefix in ["USER_MIGRATE_", "FILE_MIGRATE_", "PHONE_MIGRATE_", "NETWORK_MIGRATE_"] {
        if let Some(Some(dc)) = number_after::<i32>(err_msg, prefix) {
            return Some(dc);
        }
    }
    None
}

/// Seconds from `FLOOD_WAIT_x` (1 when the number is missing or malformed).
pub fn parse_flood_wait(err_msg: &str) -> Option<u64> {
    number_after::<u64>(err_msg, "FLOOD_WAIT_").map(|secs| secs.unwrap_or(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrate_dc_variants() {
        assert_eq!(parse_migrate_dc("MTProto RPC Error 303: FILE_MIGRATE_4"), Some(4));
        assert_eq!(parse_migrate_dc("USER_MIGRATE_2 x"), Some(2));
        assert_eq!(parse_migrate_dc("PHONE_MIGRATE_"), None);
        assert_eq!(parse_migrate_dc("BAD_REQUEST"), None);
    }

    #[test]
    fn flood_wait_variants() {
        assert_eq!(parse_flood_wait("MTProto RPC Error 420: FLOOD_WAIT_17"), Some(17));
        assert_eq!(parse_flood_wait("FLOOD_WAIT_"), Some(1));
        assert_eq!(parse_flood_wait("FLOOD_WAIT_99999999999999999999999"), Some(1));
        assert_eq!(parse_flood_wait("OTHER"), None);
    }
}
