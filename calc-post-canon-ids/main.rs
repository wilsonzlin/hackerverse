use ahash::AHashMap;
use ahash::AHashSet;
use arrow::array::ArrayRef;
use arrow::array::BinaryArray;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::FixedSizeListArray;
use arrow::array::Int16Array;
use arrow::array::ListArray;
use arrow::array::ListBuilder;
use arrow::array::MapArray;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::array::TimestampSecondArray;
use arrow::array::UInt16Array;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Fields;
use arrow::datatypes::Schema;
use arrow::datatypes::UInt64Type;
use arrow::ipc::writer::FileWriter;
use chrono::Utc;
use common::DbRpcClient;
use itertools::Itertools;
use serde::Deserialize;
use std::collections::hash_map::Entry;
use std::collections::BTreeSet;
use std::fs::File;
use std::sync::Arc;

struct DisjointSet {
  parent: AHashMap<u64, u64>,
}

impl DisjointSet {
  fn new() -> Self {
    DisjointSet {
      parent: AHashMap::new(),
    }
  }

  fn find(&mut self, id: u64) -> u64 {
    if !self.parent.contains_key(&id) {
      self.parent.insert(id, id);
      return id;
    }

    let mut leader = *self.parent.get(&id).unwrap();
    if leader != id {
      leader = self.find(leader);
      self.parent.insert(id, leader);
    }
    leader
  }

  fn union(&mut self, id1: u64, id2: u64) {
    let leader1 = self.find(id1);
    let leader2 = self.find(id2);
    if leader1 != leader2 {
      self.parent.insert(leader2, leader1);
    }
  }
}

#[tokio::main]
async fn main() {
  let client = DbRpcClient::new();

  #[derive(Deserialize)]
  struct PostRow {
    id: u64,
    title: String,
    url: Option<String>,
  }

  let mut ds = DisjointSet::new();
  let mut by_url = AHashMap::<String, u64>::new();
  let mut by_title = AHashMap::<String, u64>::new();
  let mut post_ids = Vec::new();

  // We can't fit all the data in memory.
  let mut next_post_id = 0;
  loop {
    let posts = client
      .query::<PostRow>(
        r#"
          select id, title, url
          from hn_post
          where id >= ?
            and not deleted
            and not dead
            and title != ''
          order by id
          limit 100000
        "#,
        vec![next_post_id.into()],
      )
      .await;
    let n = posts.len();
    if n == 0 {
      println!("end of posts");
      break;
    };
    println!("fetch {} posts from ID {}", n, next_post_id);
    next_post_id = posts.last().unwrap().id + 1;

    for p in posts {
      post_ids.push(p.id);
      if let Some(url) = p.url {
        if let Some(other_id) = by_url.insert(url, p.id) {
          ds.union(other_id, p.id);
        };
      };
      if let Some(other_id) = by_title.insert(p.title, p.id) {
        ds.union(other_id, p.id);
      };
    }
  }

  let canon_ids = post_ids.iter().map(|id| ds.find(*id)).collect_vec();

  let schema = Schema::new(vec![
    Field::new("id", DataType::UInt64, false),
    Field::new("canon_id", DataType::UInt64, false),
  ]);
  let out = File::create("/hndr-data/posts_canon.arrow").unwrap();
  let mut w = FileWriter::try_new(out, &schema).unwrap();
  let batch = RecordBatch::try_new(Arc::new(schema), vec![
    Arc::new(UInt64Array::from(post_ids)),
    Arc::new(UInt64Array::from(canon_ids)),
  ])
  .unwrap();
  w.write(&batch).unwrap();
  w.finish().unwrap();

  println!("all done!");
}
