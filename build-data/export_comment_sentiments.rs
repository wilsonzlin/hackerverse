use crate::common::KvRowsFetcher;
use arrow::array::ArrayRef;
use arrow::array::Float32Array;
use arrow::array::UInt32Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use common::arrow::ArrowIpcOutput;
use db_rpc_client_rs::DbRpcDbClient;
use serde::Deserialize;
use std::sync::Arc;

struct Row {
  id: u32,
  positive: f32,
  neutral: f32,
  negative: f32,
}

impl Row {
  #[rustfmt::skip]
  pub fn to_columnar(rows: Vec<Self>) -> Vec<ArrayRef> {
    let mut ids = Vec::new();
    let mut positives = Vec::new();
    let mut neutrals = Vec::new();
    let mut negatives = Vec::new();
    for r in rows {
      ids.push(r.id);
      positives.push(r.positive);
      neutrals.push(r.neutral);
      negatives.push(r.negative);
    };
    vec![
      Arc::new(UInt32Array::from(ids)),
      Arc::new(Float32Array::from(positives)),
      Arc::new(Float32Array::from(neutrals)),
      Arc::new(Float32Array::from(negatives)),
    ]
  }
}

#[derive(Deserialize)]
struct Sentiment {
  positive: f32,
  neutral: f32,
  negative: f32,
}

pub async fn export_comment_sentiments(db: DbRpcDbClient) {
  let schema = Schema::new(vec![
    Field::new("id", DataType::UInt32, false),
    Field::new("positive", DataType::Float32, false),
    Field::new("neutral", DataType::Float32, false),
    Field::new("negative", DataType::Float32, false),
  ]);

  let Some(mut out) = ArrowIpcOutput::new("comment_sentiments", schema, Row::to_columnar) else {
    return;
  };

  let mut fetcher = KvRowsFetcher::new("comment/%/sentiment");
  loop {
    let rows = fetcher.fetch_next(&db).await;
    if rows.is_empty() {
      break;
    };

    for r in rows {
      let s: Sentiment = rmp_serde::from_slice(&r.v).unwrap();
      out.push(Row {
        id: r.extract_id(),
        positive: s.positive,
        neutral: s.neutral,
        negative: s.negative,
      });
    }
  }

  out.finish();
}
