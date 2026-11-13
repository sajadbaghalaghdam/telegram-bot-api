fn mul_mod(a: u64, b: u64, m: u64) -> u64 {
    ((a as u128 * b as u128) % m as u128) as u64
}

fn pow_mod(mut base: u64, mut exp: u64, m: u64) -> u64 {
    let mut r = 1 % m;
    base %= m;
    while exp > 0 {
        if exp & 1 == 1 {
            r = mul_mod(r, base, m);
        }
        base = mul_mod(base, base, m);
        exp >>= 1;
    }
    r
}

/// Deterministic Miller-Rabin for all u64 (the first 12 primes as bases suffice below 3.3e24).
pub fn is_prime_u64(n: u64) -> bool {
    const BASES: [u64; 12] = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37];
    if n < 2 {
        return false;
    }
    for &p in &BASES {
        if n == p {
            return true;
        }
        if n % p == 0 {
            return false;
        }
    }
    let mut d = n - 1;
    let mut s = 0;
    while d % 2 == 0 {
        d /= 2;
        s += 1;
    }
    'outer: for &a in &BASES {
        let mut x = pow_mod(a, d, n);
        if x == 1 || x == n - 1 {
            continue;
        }
        for _ in 0..s - 1 {
            x = mul_mod(x, x, n);
            if x == n - 1 {
                continue 'outer;
            }
        }
        return false;
    }
    true
}

/// Maximum Pollard-rho steps before giving up (expected work for a 2^32 factor is ~2^16).
const MAX_ITERATIONS: u64 = 1 << 22;

/// Pollard's rho factorization of the MTProto `pq` value into `(p, q)` with `p <= q`.
///
/// Returns `None` if `pq` is < 4, does not fit in 63 bits, is prime, has a factor
/// >= 2^32, or no factor is found within the iteration cap.
pub fn factorize(pq: u64) -> Option<(u64, u64)> {
    if pq < 4 || pq >= (1u64 << 63) || is_prime_u64(pq) {
        return None;
    }
    let d = find_factor(pq)?;
    let (p, q) = (d.min(pq / d), d.max(pq / d));
    if p < 2 || q >= (1u64 << 32) {
        return None;
    }
    Some((p, q))
}

fn find_factor(pq: u64) -> Option<u64> {
    // Trial division handles tiny factors and prime squares, which rho handles poorly.
    for t in 2u64..1000 {
        if t * t > pq {
            break;
        }
        if pq % t == 0 {
            return Some(t);
        }
    }
    let mut iterations = 0u64;
    let mut c: u64 = 1;
    loop {
        let f = |v: u64| ((v as u128 * v as u128 + c as u128) % pq as u128) as u64;
        let (mut x, mut y, mut d) = (2u64, 2u64, 1u64);
        while d == 1 {
            iterations += 1;
            if iterations > MAX_ITERATIONS {
                return None;
            }
            x = f(x);
            y = f(f(y));
            d = gcd(x.abs_diff(y), pq);
        }
        if d != pq {
            return Some(d);
        }
        c += 1; // cycle collapsed: retry with a different polynomial constant
    }
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
        assert_eq!(factorize(u64::MAX), None); // exceeds 63 bits
        assert_eq!(factorize(1 << 63), None);
        assert!(is_prime_u64(1000000007));
        assert!(!is_prime_u64(1000000007u64 * 1000000009));
        // factor >= 2^32 rejected
        assert_eq!(factorize(3 * 4294967311), None);
    }

    #[test]
    fn test_factorize_large_halves() {
        // 4294967291 and 4294967279 are the two largest primes below 2^32
        let (p, q) = (4294967279u64, 4294967291u64);
        assert_eq!(factorize(p * q), None); // p*q >= 2^63? 2^64-ish, rejected
        let (p, q) = (2147483629u64, 2147483647u64);
        assert_eq!(factorize(p * q), Some((p, q)));
    }
}
