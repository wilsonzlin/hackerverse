use ::serde::Deserialize;
use arrow::array::ArrayRef;
use arrow::array::StringArray;
use arrow::array::UInt32Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use db_rpc_client_rs::DbRpcDbClient;
use once_cell::sync::Lazy;
use std::sync::Arc;

pub(crate) fn ts_field(name: &'static str) -> Field {
  Field::new(
    name,
    DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
    false,
  )
}

#[derive(Deserialize)]
pub(crate) struct KvRow {
  pub k: String,
  #[serde(with = "serde_bytes")]
  pub v: Vec<u8>,
}

impl KvRow {
  pub fn extract_id(&self) -> u32 {
    let (_, rem) = self.k.split_once('/').unwrap();
    let (id, _) = rem.split_once('/').unwrap();
    id.parse().unwrap()
  }
}

pub(crate) struct KvRowsFetcher {
  next_k: String,
  q: String,
}

impl KvRowsFetcher {
  pub fn new(like: &str) -> Self {
    Self {
      next_k: String::new(),
      q: format!(
        r#"
          select *
          from kv
          where k > ?
            and k like '{like}'
          order by k
          limit 123456
        "#
      ),
    }
  }

  pub async fn fetch_next(&mut self, db: &DbRpcDbClient) -> Vec<KvRow> {
    let rows = db
      .query::<KvRow>(&self.q, vec![self.next_k.as_str().into()])
      .await
      .unwrap();
    if let Some(row) = rows.last() {
      self.next_k = row.k.clone();
    };
    rows
  }
}

// Used for comment texts, post titles, post texts.
pub(crate) struct TextRow {
  pub id: u32,
  pub text: String,
}

impl TextRow {
  #[rustfmt::skip]
  pub fn to_columnar(rows: Vec<Self>) -> Vec<ArrayRef> {
    let mut ids = Vec::new();
    let mut texts = Vec::new();
    for r in rows {
      ids.push(r.id);
      texts.push(r.text);
    };
    vec![
      Arc::new(UInt32Array::from(ids)),
      Arc::new(StringArray::from(texts)),
    ]
  }
}

pub(crate) static TEXT_SCHEMA: Lazy<Schema> = Lazy::new(|| {
  Schema::new(vec![
    Field::new("id", DataType::UInt32, false),
    Field::new("text", DataType::Utf8, false),
  ])
});
