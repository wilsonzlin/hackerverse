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
struct CommentRow {
  id: u32,
  #[serde_as(as = "BoolFromInt<Strict>")]
  deleted: bool,
  #[serde_as(as = "BoolFromInt<Strict>")]
  dead: bool,
  score: i16,
  parent: u32,
  #[serde_as(deserialize_as = "DefaultOnNull")]
  author: u32,
  #[serde_as(deserialize_as = "DefaultOnNull")]
  ts: DateTime<Utc>,
  #[serde_as(deserialize_as = "DefaultOnNull")]
  post: u32,
}

impl CommentRow {
  #[rustfmt::skip]
  pub fn to_columnar(comments: Vec<CommentRow>) -> Vec<ArrayRef> {
    let mut ids = Vec::new();
    let mut deleteds = Vec::new();
    let mut deads = Vec::new();
    let mut scores = Vec::new();
    let mut parents = Vec::new();
    let mut authors = Vec::new();
    let mut tss = Vec::new();
    let mut posts = Vec::new();
    for c in comments {
      ids.push(c.id);
      deleteds.push(c.deleted);
      deads.push(c.dead);
      scores.push(c.score);
      parents.push(c.parent);
      authors.push(c.author);
      tss.push(c.ts.timestamp());
      posts.push(c.post);
    };
    vec![
      Arc::new(UInt32Array::from(ids)),
      Arc::new(BooleanArray::from(deleteds)),
      Arc::new(BooleanArray::from(deads)),
      Arc::new(Int16Array::from(scores)),
      Arc::new(UInt32Array::from(parents)),
      Arc::new(UInt32Array::from(authors)),
      Arc::new(TimestampSecondArray::from(tss).with_timezone_utc()),
      Arc::new(UInt32Array::from(posts)),
    ]
  }
}

pub async fn export_comments(db: DbRpcDbClient, interactions: Arc<DashMap<u32, BTreeSet<u32>>>) {
  let comment_schema = Schema::new(vec![
    Field::new("id", DataType::UInt32, false),
    Field::new("deleted", DataType::Boolean, false),
    Field::new("dead", DataType::Boolean, false),
    Field::new("score", DataType::Int16, false),
    Field::new("parent", DataType::UInt32, false),
    Field::new("author", DataType::UInt32, false), // 0 if null.
    ts_field("ts"),                                // 1970 if null.
    Field::new("post", DataType::UInt32, false),   // 0 if null.
  ]);

  let Some(mut out_comments) =
    ArrowIpcOutput::new("comments", comment_schema, CommentRow::to_columnar)
  else {
    return;
  };

  let mut next_comment_id = 0;
  loop {
    let rows = db
      .query::<CommentRow>(
        r#"
          select *
          from comment
          where id >= ?
          order by id
          limit 10000000
        "#,
        vec![next_comment_id.into()],
      )
      .await
      .unwrap();
    let n = rows.len();
    if n == 0 {
      println!("end of comments");
      break;
    };
    println!("fetch {} comments from ID {}", n, next_comment_id);
    next_comment_id = rows.last().unwrap().id + 1;

    for r in rows {
      if r.author != 0 {
        interactions.entry(r.author).or_default().insert(r.post);
      };
      out_comments.push(r);
    }
  }

  out_comments.finish();
}
