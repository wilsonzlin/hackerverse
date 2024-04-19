use arrow::array::ArrayRef;
use arrow::array::UInt32Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use common::arrow::ArrowIpcOutput;
use dashmap::DashMap;
use std::collections::BTreeSet;
use std::sync::Arc;

struct InteractionRow {
  user: u32,
  post: u32,
}

impl InteractionRow {
  #[rustfmt::skip]
  pub fn to_columnar(interactions: Vec<InteractionRow>) -> Vec<ArrayRef> {
    let mut users = Vec::new();
    let mut posts = Vec::new();
    for int in interactions {
      users.push(int.user);
      posts.push(int.post);
    };
    vec![
      Arc::new(UInt32Array::from(users)),
      Arc::new(UInt32Array::from(posts)),
    ]
  }
}

pub fn export_interactions(interactions: &DashMap<u32, BTreeSet<u32>>) {
  let interaction_schema = Schema::new(vec![
    Field::new("user", DataType::UInt32, false),
    Field::new("post", DataType::UInt32, false),
  ]);

  let Some(mut out_interactions) = ArrowIpcOutput::new(
    "interactions",
    interaction_schema,
    InteractionRow::to_columnar,
  ) else {
    return;
  };

  for e in interactions.iter() {
    let (&user, user_posts) = e.pair();
    for &post in user_posts {
      out_interactions.push(InteractionRow { user, post });
    }
  }

  out_interactions.finish();
}
