use chrono::prelude::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use rmpv::Value;

pub fn decode_msgpack_timestamp(v: Value) -> Result<DateTime<Utc>, &'static str> {
  match v {
    Value::String(raw) => raw
      .as_str()
      .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
      .map(|dt| dt.with_timezone(&Utc))
      .ok_or("invalid timestamp"),
    Value::Ext(typ, raw) => match typ {
      -1 => {
        // https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type
        let (sec, ns) = match raw.len() {
          4 => (u32::from_be_bytes(raw.try_into().unwrap()).into(), 0u64),
          8 => {
            let ns: u64 = (u32::from_be_bytes(raw[..4].try_into().unwrap()) >> 2).into();
            let sec = u64::from_be_bytes(raw.try_into().unwrap())
              & 0b11_11111111_11111111_11111111_11111111;
            let sec: i64 = sec.try_into().unwrap();
            (sec, ns)
          }
          12 => {
            let ns: u64 = u32::from_be_bytes(raw[..4].try_into().unwrap()).into();
            let sec = i64::from_be_bytes(raw[4..].try_into().unwrap());
            (sec, ns)
          }
          _ => unreachable!(),
        };
        Utc
          .timestamp_opt(sec, ns.try_into().unwrap())
          .single()
          .ok_or_else(|| "invalid timestamp")
      }
      _ => Err("not a timestamp"),
    },
    _ => Err("invalid type"),
  }
}
