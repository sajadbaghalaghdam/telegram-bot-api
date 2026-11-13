//! Pure-Rust gunzip for `gzip_packed#3072cfa1` MTProto frames.
use std::io::{Error, ErrorKind};

/// Pure-Rust RFC 1952 / RFC 1951 gunzip decompressor for `gzip_packed#3072cfa1` MTProto frames.
pub fn gunzip_pure_rust(data: &[u8]) -> Result<Vec<u8>, Error> {
    if data.len() < 18 || data[0] != 0x1f || data[1] != 0x8b || data[2] != 0x08 {
        return Err(Error::new(ErrorKind::InvalidData, "Invalid gzip header"));
    }
    let flg = data[3];
    let mut pos = 10;
    if (flg & 4) != 0 {
        if pos + 2 > data.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "Truncated FEXTRA"));
        }
        let xlen = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2 + xlen;
    }
    if (flg & 8) != 0 {
        while pos < data.len() && data[pos] != 0 {
            pos += 1;
        }
        pos += 1;
    }
    if (flg & 16) != 0 {
        while pos < data.len() && data[pos] != 0 {
            pos += 1;
        }
        pos += 1;
    }
    if (flg & 2) != 0 {
        pos += 2;
    }
    if pos >= data.len().saturating_sub(8) {
        return Err(Error::new(ErrorKind::UnexpectedEof, "Empty deflate body"));
    }
    inflate_raw(&data[pos..data.len() - 8])
}

fn inflate_raw(input: &[u8]) -> Result<Vec<u8>, Error> {
    struct BitReader<'a> {
        data: &'a [u8],
        byte_pos: usize,
        bit_pos: u8,
    }
    impl<'a> BitReader<'a> {
        fn read_bits(&mut self, n: u8) -> Result<u32, Error> {
            let mut val = 0u32;
            for i in 0..n {
                if self.byte_pos >= self.data.len() {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "Deflate EOF"));
                }
                let bit = ((self.data[self.byte_pos] >> self.bit_pos) & 1) as u32;
                val |= bit << i;
                self.bit_pos += 1;
                if self.bit_pos == 8 {
                    self.bit_pos = 0;
                    self.byte_pos += 1;
                }
            }
            Ok(val)
        }
        fn align_byte(&mut self) {
            if self.bit_pos > 0 {
                self.bit_pos = 0;
                self.byte_pos += 1;
            }
        }
    }

    struct HuffTable {
        counts: [u16; 16],
        symbols: Vec<u16>,
    }
    impl HuffTable {
        fn from_lengths(lengths: &[u8]) -> Self {
            let mut counts = [0u16; 16];
            for &l in lengths {
                if l > 0 && (l as usize) < 16 {
                    counts[l as usize] += 1;
                }
            }
            let mut offsets = [0u16; 16];
            let mut total = 0u16;
            for bits in 1..16 {
                offsets[bits] = total;
                total += counts[bits];
            }
            let mut symbols = vec![0u16; total as usize];
            for (sym, &l) in lengths.iter().enumerate() {
                if l > 0 && (l as usize) < 16 {
                    let idx = offsets[l as usize] as usize;
                    symbols[idx] = sym as u16;
                    offsets[l as usize] += 1;
                }
            }
            Self { counts, symbols }
        }
        fn decode(&self, br: &mut BitReader<'_>) -> Result<u16, Error> {
            let mut code = 0i32;
            let mut first = 0i32;
            let mut index = 0i32;
            for len in 1..16 {
                code |= br.read_bits(1)? as i32;
                let count = self.counts[len] as i32;
                if code - count < first {
                    return Ok(self.symbols[(index + (code - first)) as usize]);
                }
                index += count;
                first += count;
                first <<= 1;
                code <<= 1;
            }
            Err(Error::new(ErrorKind::InvalidData, "Invalid Huffman code"))
        }
    }

    const LEN_BASE: [u16; 29] = [
        3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115,
        131, 163, 195, 227, 258,
    ];
    const LEN_EXTRA: [u8; 29] = [
        0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
    ];
    const DIST_BASE: [u16; 30] = [
        1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537,
        2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577,
    ];
    const DIST_EXTRA: [u8; 30] = [
        0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12,
        13, 13,
    ];
    const CL_ORDER: [usize; 19] = [
        16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
    ];

    let mut br = BitReader {
        data: input,
        byte_pos: 0,
        bit_pos: 0,
    };
    let mut out = Vec::with_capacity(input.len() * 2);

    loop {
        let bfinal = br.read_bits(1)?;
        let btype = br.read_bits(2)?;
        match btype {
            0 => {
                br.align_byte();
                if br.byte_pos + 4 > br.data.len() {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "Stored block header EOF"));
                }
                let len = u16::from_le_bytes([br.data[br.byte_pos], br.data[br.byte_pos + 1]]) as usize;
                br.byte_pos += 4;
                if br.byte_pos + len > br.data.len() {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "Stored block data EOF"));
                }
                out.extend_from_slice(&br.data[br.byte_pos..br.byte_pos + len]);
                br.byte_pos += len;
            }
            1 | 2 => {
                let (lit_table, dist_table) = if btype == 1 {
                    let mut ll = vec![0u8; 288];
                    ll[0..144].fill(8);
                    ll[144..256].fill(9);
                    ll[256..280].fill(7);
                    ll[280..288].fill(8);
                    let dl = vec![5u8; 32];
                    (HuffTable::from_lengths(&ll), HuffTable::from_lengths(&dl))
                } else {
                    let hlit = br.read_bits(5)? as usize + 257;
                    let hdist = br.read_bits(5)? as usize + 1;
                    let hclen = br.read_bits(4)? as usize + 4;
                    let mut cl_lens = [0u8; 19];
                    for i in 0..hclen {
                        cl_lens[CL_ORDER[i]] = br.read_bits(3)? as u8;
                    }
                    let cl_table = HuffTable::from_lengths(&cl_lens);
                    let mut lens = Vec::with_capacity(hlit + hdist);
                    while lens.len() < hlit + hdist {
                        let sym = cl_table.decode(&mut br)?;
                        match sym {
                            0..=15 => lens.push(sym as u8),
                            16 => {
                                let prev = *lens.last().unwrap_or(&0);
                                let rep = br.read_bits(2)? as usize + 3;
                                lens.extend(std::iter::repeat_n(prev, rep));
                            }
                            17 => {
                                let rep = br.read_bits(3)? as usize + 3;
                                lens.extend(std::iter::repeat_n(0, rep));
                            }
                            18 => {
                                let rep = br.read_bits(7)? as usize + 11;
                                lens.extend(std::iter::repeat_n(0, rep));
                            }
                            _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid CL sym")),
                        }
                    }
                    (
                        HuffTable::from_lengths(&lens[..hlit]),
                        HuffTable::from_lengths(&lens[hlit..]),
                    )
                };

                loop {
                    let sym = lit_table.decode(&mut br)? as usize;
                    if sym < 256 {
                        out.push(sym as u8);
                    } else if sym == 256 {
                        break;
                    } else {
                        let len_idx = sym - 257;
                        let length = LEN_BASE[len_idx] as usize
                            + br.read_bits(LEN_EXTRA[len_idx])? as usize;
                        let dist_sym = dist_table.decode(&mut br)? as usize;
                        let dist = DIST_BASE[dist_sym] as usize
                            + br.read_bits(DIST_EXTRA[dist_sym])? as usize;
                        if dist > out.len() {
                            return Err(Error::new(ErrorKind::InvalidData, "Invalid LZ77 distance"));
                        }
                        for _ in 0..length {
                            let b = out[out.len() - dist];
                            out.push(b);
                        }
                    }
                }
            }
            _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid deflate block type")),
        }
        if bfinal != 0 {
            break;
        }
    }
    Ok(out)
}
