//! Helpers over `rmpv` that read loosely typed msgpack the way the Go decoder does: a string field
//! may arrive as str or bin, an integer in any width.

use rmpv::Value;

use crate::ProtoError;

/// Reads one value from the front of `buf`, returning it and the bytes after it.
pub(crate) fn read_value(buf: &[u8]) -> Result<(Value, &[u8]), ProtoError> {
    let mut rest = buf;
    let value =
        rmpv::decode::read_value(&mut rest).map_err(|e| ProtoError::Msgpack(e.to_string()))?;
    Ok((value, rest))
}

pub(crate) fn as_string(v: &Value, field: &'static str) -> Result<String, ProtoError> {
    match v {
        Value::String(s) => s.as_str().map(str::to_owned).ok_or(ProtoError::Type {
            field,
            expected: "utf-8 string",
        }),
        Value::Binary(b) => String::from_utf8(b.clone()).map_err(|_| ProtoError::Type {
            field,
            expected: "utf-8 string",
        }),
        _ => Err(ProtoError::Type {
            field,
            expected: "string",
        }),
    }
}

pub(crate) fn as_bytes(v: &Value, field: &'static str) -> Result<Vec<u8>, ProtoError> {
    match v {
        Value::Binary(b) => Ok(b.clone()),
        Value::String(s) => Ok(s.as_bytes().to_vec()),
        Value::Nil => Ok(Vec::new()),
        _ => Err(ProtoError::Type {
            field,
            expected: "bytes",
        }),
    }
}

pub(crate) fn as_map<'a>(
    v: &'a Value,
    field: &'static str,
) -> Result<&'a [(Value, Value)], ProtoError> {
    match v {
        Value::Map(m) => Ok(m),
        _ => Err(ProtoError::Type {
            field,
            expected: "map",
        }),
    }
}

/// The value stored under `key` in a msgpack map with string keys.
pub(crate) fn lookup<'a>(map: &'a [(Value, Value)], key: &str) -> Option<&'a Value> {
    map.iter()
        .find(|(k, _)| k.as_str() == Some(key))
        .map(|(_, v)| v)
}

/// A string under `key`, or the empty string when the key is absent or not a string (what Go's
/// `m[key].(string)` with the comma-ok form gives).
pub(crate) fn lookup_string(map: &[(Value, Value)], key: &str) -> String {
    lookup(map, key)
        .and_then(|v| as_string(v, "").ok())
        .unwrap_or_default()
}

pub(crate) fn lookup_bool(map: &[(Value, Value)], key: &str) -> bool {
    lookup(map, key).and_then(Value::as_bool).unwrap_or(false)
}
