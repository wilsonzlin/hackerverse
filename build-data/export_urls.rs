use crate::common::ts_field;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::StringArray;
use arrow::array::TimestampSecondArray;
use arrow::array::UInt32Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use chrono::DateTime;
use chrono::Utc;
use common::arrow::ArrowIpcOutput;
use db_rpc_client_rs::DbRpcDbClient;
use serde::Deserialize;
use serde_with::formats::Strict;
use serde_with::serde_as;
use serde_with::BoolFromInt;
use serde_with::DefaultOnNull;
use std::sync::Arc;

#[serde_as]
#[derive(Deserialize)]
struct UrlRow {
  id: u32,
  url: String,
  proto: String,
  #[serde_as(deserialize_as = "DefaultOnNull")]
  fetched: DateTime<Utc>,
  #[serde_as(deserialize_as = "DefaultOnNull")]
  fetch_err: String,
  #[serde_as(deserialize_as = "DefaultOnNull")]
  fetched_via: String,
  #[serde_as(as = "DefaultOnNull<BoolFromInt<Strict>>")]
  found_in_archive: bool,
}

impl UrlRow {
  #[rustfmt::skip]
  pub fn to_columnar(rows: Vec<UrlRow>) -> Vec<ArrayRef> {
    let mut ids = Vec::new();
    let mut urls = Vec::new();
    let mut protos = Vec::new();
    let mut fetcheds = Vec::new();
    let mut fetch_errs = Vec::new();
    let mut fetched_vias = Vec::new();
    let mut found_in_archives = Vec::new();
    for url in rows {
      ids.push(url.id);
      urls.push(url.url);
      protos.push(url.proto);
      fetcheds.push(url.fetched.timestamp());
      fetch_errs.push(url.fetch_err);
      fetched_vias.push(url.fetched_via);
      found_in_archives.push(url.found_in_archive);
    };
    vec![
      Arc::new(UInt32Array::from(ids)),
      Arc::new(StringArray::from(urls)),
      Arc::new(StringArray::from(protos)),
      Arc::new(TimestampSecondArray::from(fetcheds).with_timezone_utc()),
      Arc::new(StringArray::from(fetch_errs)),
      Arc::new(StringArray::from(fetched_vias)),
      Arc::new(BooleanArray::from(found_in_archives)),
    ]
  }
}

pub async fn export_urls(db: DbRpcDbClient) {
  let url_schema = Schema::new(vec![
    Field::new("id", DataType::UInt32, false),
    Field::new("url", DataType::Utf8, false),
    Field::new("proto", DataType::Utf8, false),
    ts_field("fetched"),                                      // 1970 if null.
    Field::new("fetch_err", DataType::Utf8, false),           // Empty string if null.
    Field::new("fetched_via", DataType::Utf8, false),         // Empty string if null.
    Field::new("found_in_archive", DataType::Boolean, false), // False if null.
  ]);

  let Some(mut out_urls) = ArrowIpcOutput::new("urls", url_schema, UrlRow::to_columnar) else {
    return;
  };

  let mut next_url_id = 0;
  loop {
    let rows = db
      .query::<UrlRow>(
        r#"
          select *
          from url
          where id >= ?
          order by id
          limit 10000000
        "#,
        vec![next_url_id.into()],
      )
      .await
      .unwrap();
    let n = rows.len();
    if n == 0 {
      println!("end of URLs");
      break;
    };
    println!("fetch {} URLs from ID {}", n, next_url_id);
    next_url_id = rows.last().unwrap().id + 1;

    for r in rows {
      out_urls.push(r);
    }
  }

  out_urls.finish();
}
