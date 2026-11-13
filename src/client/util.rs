//! Small allocation-free helpers shared by the relay modules.

/// ASCII-lowercases `s` into `buf` without allocating. Strings longer than the buffer are returned
/// unchanged: no method name we match on is that long, so they can never match anyway.
pub fn ascii_lower<'a>(s: &'a str, buf: &'a mut [u8; 32]) -> &'a str {
    if s.len() > buf.len() {
        return s;
    }
    let out = &mut buf[..s.len()];
    out.copy_from_slice(s.as_bytes());
    out.make_ascii_lowercase();
    // ASCII lowercasing keeps valid UTF-8 valid; fall back to the original on the impossible case.
    std::str::from_utf8(out).unwrap_or(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowercases_without_alloc() {
        let mut b = [0u8; 32];
        assert_eq!(ascii_lower("SendMessage", &mut b), "sendmessage");
        let mut b = [0u8; 32];
        assert_eq!(ascii_lower("ÄBc", &mut b), "Äbc");
        let long = "A".repeat(40);
        let mut b = [0u8; 32];
        assert_eq!(ascii_lower(&long, &mut b), long);
    }
}
