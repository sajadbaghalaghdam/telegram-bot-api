//! Positioned reads and writes of a whole buffer (`pread` / `pwrite`) on every platform, so
//! several threads can use one open file without sharing a cursor.
//!
//! On Windows `seek_read` / `seek_write` do move the file's cursor, but every use here passes an
//! explicit offset, so it never matters.
use std::fs::File;
use std::io;

/// Fills `buf` from `file` starting at byte `offset`; `UnexpectedEof` if the file ends first.
#[cfg(unix)]
pub fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

/// Writes all of `buf` to `file` at byte `offset`.
#[cfg(unix)]
pub fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, buf, offset)
}

#[cfg(windows)]
pub fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer")),
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(windows)]
pub fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_write(buf, offset) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write whole buffer")),
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positioned_writes_and_reads_do_not_depend_on_a_cursor() {
        let path = std::env::temp_dir().join(format!("file_at_{}.bin", std::process::id()));
        let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
        // Out of order, as parallel parts arrive.
        write_all_at(&file, b"world", 6).unwrap();
        write_all_at(&file, b"hello ", 0).unwrap();
        let mut all = [0u8; 11];
        read_exact_at(&file, &mut all, 0).unwrap();
        assert_eq!(&all, b"hello world");
        let mut tail = [0u8; 5];
        read_exact_at(&file, &mut tail, 6).unwrap();
        assert_eq!(&tail, b"world");
        // Past the end: an error, not a short read.
        let mut past = [0u8; 4];
        assert_eq!(read_exact_at(&file, &mut past, 9).unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        drop(file);
        std::fs::remove_file(path).unwrap();
    }
}
