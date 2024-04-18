use chrono::prelude::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use rmpv::Value;

pub fn decode_msgpack_timestamp(v: Value) -> DateTime<Utc> {
  let Value::Ext(typ, raw) = v else {
    panic!("not an ext type");
  };
  match typ {
    -1 => {
      // https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type
      let (sec, ns) = match raw.len() {
        4 => (u32::from_be_bytes(raw.try_into().unwrap()).into(), 0u64),
        8 => {
          let ns: u64 = (u32::from_be_bytes(raw[..4].try_into().unwrap()) >> 2).into();
          let sec =
            u64::from_be_bytes(raw.try_into().unwrap()) & 0b11_11111111_11111111_11111111_11111111;
          (sec, ns)
        }
        12 => {
          let ns: u64 = u32::from_be_bytes(raw[..4].try_into().unwrap()).into();
          let sec = u64::from_be_bytes(raw[4..].try_into().unwrap());
          (sec, ns)
        }
        _ => unreachable!(),
      };
      Utc
        .timestamp_opt(sec.try_into().unwrap(), ns.try_into().unwrap())
        .unwrap()
    }
    _ => panic!("not a timestamp ({typ})"),
  }
}
