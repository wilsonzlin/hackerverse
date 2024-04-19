use crate::common::ts_field;
use crate::common::KvRowsFetcher;
use arrow::array::ArrayRef;
use arrow::array::StringArray;
use arrow::array::TimestampSecondArray;
use arrow::array::UInt32Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use chrono::DateTime;
use chrono::Utc;
use common::arrow::ArrowIpcOutput;
use common::msgpack::decode_msgpack_timestamp;
use db_rpc_client_rs::DbRpcDbClient;
use serde::Deserialize;
use std::sync::Arc;

struct UrlMetaRow {
  id: u32,
  description: String,
  image_url: String,
  lang: String,
  snippet: String,
  timestamp: DateTime<Utc>,
  timestamp_modified: DateTime<Utc>,
  title: String,
}

impl UrlMetaRow {
  #[rustfmt::skip]
  pub fn to_columnar(rows: Vec<Self>) -> Vec<ArrayRef> {
    let mut ids = Vec::new();
    let mut descriptions = Vec::new();
    let mut image_urls = Vec::new();
    let mut langs = Vec::new();
    let mut snippets = Vec::new();
    let mut timestamps = Vec::new();
    let mut timestamp_modifieds = Vec::new();
    let mut titles = Vec::new();
    for r in rows {
      ids.push(r.id);
      descriptions.push(r.description);
      image_urls.push(r.image_url);
      langs.push(r.lang);
      snippets.push(r.snippet);
      timestamps.push(r.timestamp.timestamp());
      timestamp_modifieds.push(r.timestamp_modified.timestamp());
      titles.push(r.title);
    };
    vec![
      Arc::new(UInt32Array::from(ids)),
      Arc::new(StringArray::from(descriptions)),
      Arc::new(StringArray::from(image_urls)),
      Arc::new(StringArray::from(langs)),
      Arc::new(StringArray::from(snippets)),
      Arc::new(TimestampSecondArray::from(timestamps).with_timezone_utc()),
      Arc::new(TimestampSecondArray::from(timestamp_modifieds).with_timezone_utc()),
      Arc::new(StringArray::from(titles)),
    ]
  }
}

pub async fn export_url_metas(db: DbRpcDbClient) {
  let url_meta_schema = Schema::new(vec![
    Field::new("id", DataType::UInt32, false),
    Field::new("description", DataType::Utf8, false), // Empty string if null.
    Field::new("image_url", DataType::Utf8, false),   // Empty string if null.
    Field::new("lang", DataType::Utf8, false),        // Empty string if null.
    Field::new("snippet", DataType::Utf8, false),     // Empty string if null.
    ts_field("timestamp"),                            // 1970 if null.
    ts_field("timestamp_modified"),                   // 1970 if null.
    Field::new("title", DataType::Utf8, false),       // Empty string if null.
  ]);

  let Some(mut out_url_metas) =
    ArrowIpcOutput::new("url_metas", url_meta_schema, UrlMetaRow::to_columnar)
  else {
    return;
  };

  let mut fetcher = KvRowsFetcher::new("url/%/meta");
  loop {
    let rows = fetcher.fetch_next(&db).await;
    if rows.is_empty() {
      break;
    };

    for r in rows {
      #[derive(Deserialize)]
      #[serde(rename_all = "camelCase")]
      struct Meta {
        description: Option<String>,
        image_url: Option<String>,
        lang: Option<String>,
        snippet: Option<String>,
        // Due to inconsistencies (original crawler was written in JavaScript but later changed to Rust), timestamps could be either ext(-1) by rmp_serde or an ISO 8601 string.
        timestamp: Option<rmpv::Value>,
        timestamp_modified: Option<rmpv::Value>,
        title: Option<String>,
      }
      let meta: Meta = rmp_serde::from_slice(&r.v).unwrap();
      out_url_metas.push(UrlMetaRow {
        id: r.extract_id(),
        description: meta.description.unwrap_or_default(),
        image_url: meta.image_url.unwrap_or_default(),
        lang: meta.lang.unwrap_or_default(),
        snippet: meta.snippet.unwrap_or_default(),
        timestamp: meta
          .timestamp
          .map(|ts| decode_msgpack_timestamp(ts).unwrap())
          .unwrap_or_default(),
        timestamp_modified: meta
          .timestamp_modified
          .map(|ts| decode_msgpack_timestamp(ts).unwrap())
          .unwrap_or_default(),
        title: meta.title.unwrap_or_default(),
      });
    }
  }

  out_url_metas.finish();
}
