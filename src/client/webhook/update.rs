//! Extracts the few fields the scheduler needs from a raw update (`update_id` and the ordering
//! key) without building a `serde_json::Value`: updates are delivered as the exact byte slices
//! upstream sent, so nothing else is parsed, allocated or re-serialised.
use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;

/// Identity of a delivery queue. Updates of one chat are delivered strictly in order; updates
/// without any chat/sender get a queue of their own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueId {
    Chat(i64),
    Solo(i64),
}

/// `update_id` of a raw update. Upstream (the Bot API server) always emits it first and without
/// whitespace, which the fast path relies on; anything else takes the generic parser.
pub fn update_id(raw: &[u8]) -> Option<i64> {
    const PREFIX: &[u8] = br#"{"update_id":"#;
    if let Some(rest) = raw.strip_prefix(PREFIX) {
        let digits = rest.iter().take_while(|b| b.is_ascii_digit()).count();
        if digits > 0 && digits <= 18 && matches!(rest.get(digits), Some(b',' | b'}')) {
            return Some(rest[..digits].iter().fold(0i64, |n, b| n * 10 + i64::from(b - b'0')));
        }
    }
    #[derive(Deserialize)]
    struct Id {
        update_id: i64,
    }
    serde_json::from_slice::<Id>(raw).ok().map(|i| i.update_id)
}

/// Queue of an update: the chat (or sender, when there is no chat) of its payload object, else a
/// queue of its own. Looks at the first payload object (a top-level field with an object value)
/// that carries `chat.id`, `message.chat.id` or `from.id`, in that order of preference.
pub fn queue_id(raw: &[u8], update_id: i64) -> QueueId {
    serde_json::from_slice::<Keys>(raw).ok().and_then(|k| k.0).map_or(QueueId::Solo(update_id), QueueId::Chat)
}

struct Keys(Option<i64>);

impl<'de> Deserialize<'de> for Keys {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Keys;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an update object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Keys, A::Error> {
                let mut found = None;
                while let Some(key) = map.next_key::<&str>()? {
                    if key == "update_id" || found.is_some() {
                        map.next_value::<IgnoredAny>()?;
                    } else if let Some(p) = map.next_value::<Lenient<Payload>>()?.0 {
                        found = p.key();
                    }
                }
                Ok(Keys(found))
            }
        }
        d.deserialize_map(V)
    }
}

/// The payload object of an update (`message`, `callback_query`, `inline_query`, ...).
#[derive(Deserialize, Default)]
struct Payload {
    #[serde(default)]
    chat: Lenient<IdOf>,
    #[serde(default)]
    message: Lenient<MessageChat>,
    #[serde(default)]
    from: Lenient<IdOf>,
}

impl Payload {
    fn key(&self) -> Option<i64> {
        let id = |o: &Lenient<IdOf>| o.0.as_ref().and_then(|i| i.id.0);
        id(&self.chat).or_else(|| self.message.0.as_ref().and_then(|m| id(&m.chat))).or_else(|| id(&self.from))
    }
}

#[derive(Deserialize, Default)]
struct MessageChat {
    #[serde(default)]
    chat: Lenient<IdOf>,
}

#[derive(Deserialize, Default)]
struct IdOf {
    #[serde(default)]
    id: LenientInt,
}

/// `T` when the value is a JSON object, `None` for any other value (which is skipped).
struct Lenient<T>(Option<T>);

impl<T> Default for Lenient<T> {
    fn default() -> Self {
        Self(None)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Lenient<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for V<T> {
            type Value = Lenient<T>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any JSON value")
            }
            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Lenient<T>, A::Error> {
                T::deserialize(de::value::MapAccessDeserializer::new(map)).map(|v| Lenient(Some(v)))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Lenient<T>, A::Error> {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(Lenient(None))
            }
            fn visit_bool<E>(self, _: bool) -> Result<Lenient<T>, E> {
                Ok(Lenient(None))
            }
            fn visit_i64<E>(self, _: i64) -> Result<Lenient<T>, E> {
                Ok(Lenient(None))
            }
            fn visit_u64<E>(self, _: u64) -> Result<Lenient<T>, E> {
                Ok(Lenient(None))
            }
            fn visit_f64<E>(self, _: f64) -> Result<Lenient<T>, E> {
                Ok(Lenient(None))
            }
            fn visit_str<E>(self, _: &str) -> Result<Lenient<T>, E> {
                Ok(Lenient(None))
            }
            fn visit_unit<E>(self) -> Result<Lenient<T>, E> {
                Ok(Lenient(None))
            }
        }
        d.deserialize_any(V(PhantomData))
    }
}

/// An integer that fits `i64`; any other value (string, float, ...) is skipped.
#[derive(Default)]
struct LenientInt(Option<i64>);

impl<'de> Deserialize<'de> for LenientInt {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = LenientInt;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any JSON value")
            }
            fn visit_i64<E>(self, n: i64) -> Result<LenientInt, E> {
                Ok(LenientInt(Some(n)))
            }
            fn visit_u64<E>(self, n: u64) -> Result<LenientInt, E> {
                Ok(LenientInt(i64::try_from(n).ok()))
            }
            fn visit_bool<E>(self, _: bool) -> Result<LenientInt, E> {
                Ok(LenientInt(None))
            }
            fn visit_f64<E>(self, _: f64) -> Result<LenientInt, E> {
                Ok(LenientInt(None))
            }
            fn visit_str<E>(self, _: &str) -> Result<LenientInt, E> {
                Ok(LenientInt(None))
            }
            fn visit_unit<E>(self) -> Result<LenientInt, E> {
                Ok(LenientInt(None))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<LenientInt, A::Error> {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(LenientInt(None))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<LenientInt, A::Error> {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(LenientInt(None))
            }
        }
        d.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(json: &str) -> QueueId {
        let id = update_id(json.as_bytes()).unwrap();
        queue_id(json.as_bytes(), id)
    }

    #[test]
    fn queue_ids() {
        assert_eq!(q(r#"{"update_id":1,"message":{"chat":{"id":10}}}"#), QueueId::Chat(10));
        assert_eq!(q(r#"{"update_id":3,"edited_message":{"chat":{"id":10}}}"#), QueueId::Chat(10));
        assert_eq!(q(r#"{"update_id":4,"callback_query":{"from":{"id":5},"message":{"chat":{"id":20}}}}"#), QueueId::Chat(20));
        assert_eq!(q(r#"{"update_id":5,"inline_query":{"from":{"id":7}}}"#), QueueId::Chat(7));
        assert_eq!(q(r#"{"update_id":6,"poll":{"id":"x"}}"#), QueueId::Solo(6));
        // negative chat ids (groups/channels) are keys like any other
        assert_eq!(q(r#"{"update_id":8,"message":{"chat":{"id":-1001234567890}}}"#), QueueId::Chat(-1001234567890));
    }

    #[test]
    fn queue_key_is_robust_against_odd_payloads() {
        // chat beats message.chat beats from, regardless of field order
        assert_eq!(q(r#"{"update_id":1,"x":{"from":{"id":3},"message":{"chat":{"id":2}},"chat":{"id":1}}}"#), QueueId::Chat(1));
        assert_eq!(q(r#"{"update_id":1,"x":{"from":{"id":3},"message":{"chat":{"id":2}}}}"#), QueueId::Chat(2));
        // non-object payloads / ids that are not integers are skipped, the next payload object wins
        assert_eq!(q(r#"{"update_id":1,"a":5,"b":[1,{"chat":{"id":9}}],"c":"s","d":null,"e":{"chat":{"id":"7"},"from":{"id":4}}}"#), QueueId::Chat(4));
        assert_eq!(q(r#"{"update_id":1,"m":{"chat":5,"message":"x","from":[1,2]}}"#), QueueId::Solo(1));
        assert_eq!(q(r#"{"update_id":1,"m":{"chat":{"id":1.5}}}"#), QueueId::Solo(1));
        assert_eq!(q(r#"{"update_id":1,"m":{"chat":{"id":18446744073709551615}}}"#), QueueId::Solo(1));
        // text that looks like keys does not confuse the parser
        assert_eq!(q(r#"{"update_id":2,"message":{"text":"\"chat\":{\"id\":99}","chat":{"id":5}}}"#), QueueId::Chat(5));
        // garbage after the update id: the update is still delivered, in a queue of its own
        assert_eq!(queue_id(b"{\"update_id\":3,", 3), QueueId::Solo(3));
    }

    #[test]
    fn update_id_fast_and_slow_paths() {
        assert_eq!(update_id(br#"{"update_id":123,"message":{}}"#), Some(123));
        assert_eq!(update_id(br#"{"update_id":7}"#), Some(7));
        assert_eq!(update_id(br#"{"update_id":999999999999999999,"x":1}"#), Some(999_999_999_999_999_999));
        // not first / whitespace / big numbers: generic parser
        assert_eq!(update_id(br#"{"message":{},"update_id":5}"#), Some(5));
        assert_eq!(update_id(b"{ \"update_id\" : 5 }"), Some(5));
        assert_eq!(update_id(br#"{"update_id":9999999999999999999}"#), None);
        assert_eq!(update_id(br#"{"update_id":"5"}"#), None);
        assert_eq!(update_id(br#"{"update_id":-5}"#), Some(-5));
        assert_eq!(update_id(br#"{"update_id":5x}"#), None);
        assert_eq!(update_id(br#"{"x":1}"#), None);
        assert_eq!(update_id(b"[]"), None);
        assert_eq!(update_id(b""), None);
    }
}
