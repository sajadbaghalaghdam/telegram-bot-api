//! Minimal inspection of an upstream Bot API reply: the relay only needs `ok`, `error_code` and
//! `parameters.retry_after`, so the reply is neither parsed into a `serde_json::Value` nor copied.

use std::fmt;

use bytes::Bytes;
use serde::de::{self, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;

/// A reply body together with what the relay needs to know about it.
pub struct Relayed {
    pub body: Bytes,
    pub head: ReplyHead,
}

impl Relayed {
    pub fn new(body: Bytes) -> Self {
        let head = ReplyHead::parse(&body);
        Self { body, head }
    }
}

/// The fields of a Bot API envelope the relay acts on. Missing or ill-typed fields are `None`, and a
/// body that is not a JSON object yields the all-`None` default.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReplyHead {
    pub ok: Option<bool>,
    pub error_code: Option<i64>,
    pub retry_after: Option<i64>,
}

/// A JSON scalar of interest; everything else (strings, arrays, objects, floats, null) is `Other`.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
enum Scalar {
    #[default]
    Other,
    Bool(bool),
    Int(i64),
    Uint(u64),
}

impl Scalar {
    fn as_bool(self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(b),
            _ => None,
        }
    }

    fn as_i64(self) -> Option<i64> {
        match self {
            Self::Int(i) => Some(i),
            Self::Uint(u) => i64::try_from(u).ok(),
            _ => None,
        }
    }
}

impl<'de> Deserialize<'de> for Scalar {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Scalar;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any JSON value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Scalar, E> {
                Ok(Scalar::Bool(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Scalar, E> {
                Ok(Scalar::Int(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Scalar, E> {
                Ok(Scalar::Uint(v))
            }

            fn visit_f64<E>(self, _: f64) -> Result<Scalar, E> {
                Ok(Scalar::Other)
            }

            fn visit_str<E>(self, _: &str) -> Result<Scalar, E> {
                Ok(Scalar::Other)
            }

            fn visit_unit<E>(self) -> Result<Scalar, E> {
                Ok(Scalar::Other)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Scalar, A::Error> {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(Scalar::Other)
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Scalar, A::Error> {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(Scalar::Other)
            }
        }
        d.deserialize_any(V)
    }
}

/// `parameters`: only `retry_after` matters; any other shape is ignored.
#[derive(Default)]
struct Parameters {
    retry_after: Scalar,
}

impl<'de> Deserialize<'de> for Parameters {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Parameters;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any JSON value")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Parameters, A::Error> {
                let mut out = Parameters::default();
                while let Some(key) = map.next_key::<Key>()? {
                    match key {
                        Key::RetryAfter => out.retry_after = map.next_value()?,
                        Key::Other => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok(out)
            }

            // Not an object: nothing to read.
            fn visit_bool<E>(self, _: bool) -> Result<Parameters, E> {
                Ok(Parameters::default())
            }

            fn visit_i64<E>(self, _: i64) -> Result<Parameters, E> {
                Ok(Parameters::default())
            }

            fn visit_u64<E>(self, _: u64) -> Result<Parameters, E> {
                Ok(Parameters::default())
            }

            fn visit_f64<E>(self, _: f64) -> Result<Parameters, E> {
                Ok(Parameters::default())
            }

            fn visit_str<E>(self, _: &str) -> Result<Parameters, E> {
                Ok(Parameters::default())
            }

            fn visit_unit<E>(self) -> Result<Parameters, E> {
                Ok(Parameters::default())
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Parameters, A::Error> {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(Parameters::default())
            }
        }
        d.deserialize_any(V)
    }
}

enum Key {
    RetryAfter,
    Other,
}

impl<'de> Deserialize<'de> for Key {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl de::Visitor<'_> for V {
            type Value = Key;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string key")
            }

            fn visit_str<E>(self, v: &str) -> Result<Key, E> {
                Ok(if v == "retry_after" { Key::RetryAfter } else { Key::Other })
            }
        }
        d.deserialize_str(V)
    }
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(default)]
    ok: Scalar,
    #[serde(default)]
    error_code: Scalar,
    #[serde(default)]
    parameters: Parameters,
}

/// Success replies start with this; nothing after `"ok":true` can change how the relay treats them.
const OK_PREFIX: &[u8] = br#"{"ok":true"#;

impl ReplyHead {
    /// Reads the envelope fields. Success replies (`{"ok":true,...`, the overwhelmingly common case
    /// and the only large ones) are recognised from their first bytes without scanning the result.
    pub fn parse(body: &[u8]) -> Self {
        if body.starts_with(OK_PREFIX) && matches!(body.get(OK_PREFIX.len()), Some(b',' | b'}')) {
            return Self { ok: Some(true), ..Self::default() };
        }
        // serde would also read a JSON array as the struct's fields; only objects are envelopes.
        if body.trim_ascii_start().first() != Some(&b'{') {
            return Self::default();
        }
        match serde_json::from_slice::<Envelope>(body) {
            Ok(e) => Self {
                ok: e.ok.as_bool(),
                error_code: e.error_code.as_i64(),
                retry_after: e.parameters.retry_after.as_i64(),
            },
            Err(_) => Self::default(),
        }
    }

    /// `(HTTP status, retry_after)` of a failure reply (`ok: false`). A missing / invalid
    /// `error_code` is a 400.
    pub fn failure(&self) -> Option<(u16, Option<i64>)> {
        (self.ok == Some(false)).then(|| {
            let code = self.error_code.and_then(|c| u64::try_from(c).ok()).unwrap_or(400);
            (code as u16, self.retry_after)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(s: &str) -> ReplyHead {
        ReplyHead::parse(s.as_bytes())
    }

    #[test]
    fn success_replies() {
        let ok = ReplyHead { ok: Some(true), ..Default::default() };
        assert_eq!(head(r#"{"ok":true,"result":{"message_id":1}}"#), ok);
        assert_eq!(head(r#"{"ok":true}"#), ok);
        // not the fast path (whitespace, key order) but the same answer
        assert_eq!(head(r#"{ "result":[1,2,{"a":"b"}], "ok": true }"#), ok);
        assert_eq!(head(r#"{"ok":true,"result":true}"#).failure(), None);
    }

    #[test]
    fn failure_replies() {
        let h = head(r#"{"ok":false,"error_code":429,"description":"x","parameters":{"retry_after":9,"migrate_to_chat_id":-5}}"#);
        assert_eq!(h, ReplyHead { ok: Some(false), error_code: Some(429), retry_after: Some(9) });
        assert_eq!(h.failure(), Some((429, Some(9))));
        assert_eq!(head(r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#).failure(), Some((401, None)));
        // no / ill-typed error_code: 400
        assert_eq!(head(r#"{"ok":false}"#).failure(), Some((400, None)));
        assert_eq!(head(r#"{"ok":false,"error_code":"x"}"#).failure(), Some((400, None)));
        assert_eq!(head(r#"{"ok":false,"error_code":-1}"#).failure(), Some((400, None)));
        // parameters of another shape are ignored
        assert_eq!(head(r#"{"ok":false,"error_code":400,"parameters":"x"}"#).failure(), Some((400, None)));
        assert_eq!(head(r#"{"ok":false,"error_code":400,"parameters":[1,{"a":2}]}"#).failure(), Some((400, None)));
    }

    #[test]
    fn not_an_envelope() {
        for s in ["", "<html>x</html>", "[1]", "[true]", "[true,401]", "null", r#"{"result":1}"#, r#"{"ok":"yes"}"#, r#"{"ok":tru"#] {
            assert_eq!(head(s).failure(), None, "{s}");
            assert_ne!(head(s).ok, Some(true), "{s}");
        }
        // "ok":true-looking prefix that is not the success marker
        assert_eq!(head(r#"{"ok":trueish}"#).ok, None);
    }
}
