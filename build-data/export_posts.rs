use crate::common::ts_field;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::Int16Array;
use arrow::array::TimestampSecondArray;
use arrow::array::UInt32Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use chrono::DateTime;
use chrono::Utc;
use common::arrow::ArrowIpcOutput;
use dashmap::DashMap;
use db_rpc_client_rs::DbRpcDbClient;
use serde::Deserialize;
use serde_with::formats::Strict;
use serde_with::serde_as;
use serde_with::BoolFromInt;
use serde_with::DefaultOnNull;
use std::collections::BTreeSet;
use std::sync::Arc;

#[serde_as]
#[derive(Deserialize)]
struct PostRow {
  id: u32,
  #[serde_as(as = "BoolFromInt<Strict>")]
  deleted: bool,
  #[serde_as(as = "BoolFromInt<Strict>")]
  dead: bool,
  score: i16,
  #[serde_as(deserialize_as = "DefaultOnNull")]
  author: u32,
  #[serde_as(deserialize_as = "DefaultOnNull")]
  ts: DateTime<Utc>,
  #[serde_as(deserialize_as = "DefaultOnNull")]
  url: u32,
  #[serde_as(as = "BoolFromInt<Strict>")]
  emb_missing_page: bool,
}

impl PostRow {
  #[rustfmt::skip]
  pub fn to_columnar(posts: Vec<PostRow>) -> Vec<ArrayRef> {
    let mut ids = Vec::new();
    let mut deleteds = Vec::new();
    let mut deads = Vec::new();
    let mut scores = Vec::new();
    let mut authors = Vec::new();
    let mut tss = Vec::new();
    let mut urls = Vec::new();
    let mut emb_missing_pages = Vec::new();
    for p in posts {
      ids.push(p.id);
      deleteds.push(p.deleted);
      deads.push(p.dead);
      scores.push(p.score);
      authors.push(p.author);
      tss.push(p.ts.timestamp());
      urls.push(p.url);
      emb_missing_pages.push(p.emb_missing_page);
    };
    vec![
      Arc::new(UInt32Array::from(ids)),
      Arc::new(BooleanArray::from(deleteds)),
      Arc::new(BooleanArray::from(deads)),
      Arc::new(Int16Array::from(scores)),
      Arc::new(UInt32Array::from(authors)),
      Arc::new(TimestampSecondArray::from(tss).with_timezone_utc()),
      Arc::new(UInt32Array::from(urls)),
      Arc::new(BooleanArray::from(emb_missing_pages)),
    ]
  }
}

pub async fn export_posts(db: DbRpcDbClient, interactions: Arc<DashMap<u32, BTreeSet<u32>>>) {
  let post_schema = Schema::new(vec![
    Field::new("id", DataType::UInt32, false),
    Field::new("deleted", DataType::Boolean, false),
    Field::new("dead", DataType::Boolean, false),
    Field::new("score", DataType::Int16, false),
    Field::new("author", DataType::UInt32, false), // 0 if null.
    ts_field("ts"),                                // 1970 if null.
    Field::new("url", DataType::UInt32, false),    // 0 if null.
    Field::new("emb_missing_page", DataType::Boolean, false),
  ]);

  let Some(mut out_posts) = ArrowIpcOutput::new("posts", post_schema, PostRow::to_columnar) else {
    return;
  };

  let mut next_post_id = 0;
  loop {
    let rows = db
      .query::<PostRow>(
        r#"
          select *
          from post
          where id >= ?
          order by id
          limit 10000000
        "#,
        vec![next_post_id.into()],
      )
      .await
      .unwrap();
    let n = rows.len();
    if n == 0 {
      println!("end of posts");
      break;
    };
    println!("fetch {} posts from ID {}", n, next_post_id);
    next_post_id = rows.last().unwrap().id + 1;

    for r in rows {
      if r.author != 0 {
        interactions.entry(r.author).or_default().insert(r.id);
      };
      out_posts.push(r);
    }
  }

  out_posts.finish();
}
