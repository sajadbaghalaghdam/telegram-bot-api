/// Pollard's rho algorithm for 64-bit integer factorization used in MTProto handshake.
/// Returns `None` when `pq` cannot be split into two non-trivial factors
/// (0, 1, primes), which a legitimate server never sends.
pub fn factorize(pq: u64) -> Option<(u64, u64)> {
    if pq < 4 {
        return None;
    }
    if pq % 2 == 0 {
        return Some((2, pq / 2));
    }

    let mut x: u64 = 2;
    let mut y: u64 = 2;
    let mut d: u64 = 1;
    let mut c: u64 = 1;
    let mut attempts = 0u32;

    let f = |val: u64, c_val: u64| -> u64 {
        let val_128 = val as u128;
        ((val_128 * val_128 + c_val as u128) % (pq as u128)) as u64
    };

    while d == 1 {
        x = f(x, c);
        y = f(f(y, c), c);

        let diff = if x > y { x - y } else { y - x };
        d = gcd(diff, pq);

        if d == pq {
            // Retry with different polynomial constant
            x = 2;
            y = 2;
            c += 1;
            d = 1;
            attempts += 1;
            if attempts > 64 {
                // pq is (almost certainly) prime or a prime square; give up.
                return None;
            }
        }
    }

    let p = d.min(pq / d);
    let q = d.max(pq / d);
    Some((p, q))
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factorize() {
        let p = 1000000007u64;
        let q = 1000000009u64;
        let pq = p * q;
        let (f1, f2) = factorize(pq).unwrap();
        assert_eq!(f1 * f2, pq);
        assert_eq!(f1, p);
        assert_eq!(f2, q);
    }

    #[test]
    fn test_factorize_edge_cases() {
        assert_eq!(factorize(0), None);
        assert_eq!(factorize(1), None);
        assert_eq!(factorize(2), None);
        assert_eq!(factorize(3), None);
        assert_eq!(factorize(4), Some((2, 2)));
        assert_eq!(factorize(1000000007), None); // prime
        assert_eq!(factorize(9), Some((3, 3)));
    }
}
