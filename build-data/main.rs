use ahash::AHashMap;
use arrow::array::ArrayRef;
use arrow::array::BinaryArray;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::Int16Array;
use arrow::array::StringArray;
use arrow::array::TimestampSecondArray;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use chrono::Utc;
use common::arrow::ArrowIpcOutput;
use common::DbRpcClient;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Deserialize)]
struct PostRow {
  id: u64,
  score: i16,
  title: String,
  text: String,
  author: Option<String>,
  ts: Option<chrono::DateTime<Utc>>,
  url: Option<String>,
  #[serde(with = "serde_bytes")]
  emb_dense_title: Option<Vec<u8>>,
  #[serde(with = "serde_bytes")]
  emb_sparse_title: Option<Vec<u8>>,
  #[serde(with = "serde_bytes")]
  emb_dense_text: Option<Vec<u8>>,
  #[serde(with = "serde_bytes")]
  emb_sparse_text: Option<Vec<u8>>,
}

#[derive(Deserialize)]
struct CommentRow {
  id: u64,
  score: i16,
  text: String,
  parent: u64,
  author: String,
  ts: Option<chrono::DateTime<Utc>>,
  post: u64,
  #[serde(with = "serde_bytes")]
  emb_dense_text: Option<Vec<u8>>,
  #[serde(with = "serde_bytes")]
  emb_sparse_text: Option<Vec<u8>>,
}

struct UserRow {
  id: u64,
  name: String,
}

struct InteractionRow {
  user: u64,
  post: u64,
}

#[rustfmt::skip]
fn post_rows_to_columnar(posts: Vec<PostRow>) -> Vec<ArrayRef> {
  let mut ids = Vec::new();
  let mut scores = Vec::new();
  let mut titles = Vec::new();
  let mut texts = Vec::new();
  let mut authors = Vec::new();
  let mut tss = Vec::new();
  let mut urls = Vec::new();
  let mut emb_dense_titles = Vec::new();
  let mut emb_sparse_titles = Vec::new();
  let mut emb_dense_texts = Vec::new();
  let mut emb_sparse_texts = Vec::new();
  for p in posts {
    ids.push(p.id);
    scores.push(p.score);
    titles.push(p.title);
    texts.push(p.text);
    authors.push(p.author);
    tss.push(p.ts.map(|ts| ts.timestamp()).unwrap_or(0));
    urls.push(p.url.unwrap_or_default());
    emb_dense_titles.push(p.emb_dense_title);
    emb_sparse_titles.push(p.emb_sparse_title);
    emb_dense_texts.push(p.emb_dense_text);
    emb_sparse_texts.push(p.emb_sparse_text);
  };
  vec![
    Arc::new(UInt64Array::from(ids)),
    Arc::new(Int16Array::from(scores)),
    Arc::new(StringArray::from(titles)),
    Arc::new(StringArray::from(texts)),
    Arc::new(StringArray::from(authors)),
    Arc::new(TimestampSecondArray::from(tss).with_timezone_utc()),
    Arc::new(StringArray::from(urls)),
    Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(emb_dense_titles.into_iter(), 4096).unwrap()),
    Arc::new(BinaryArray::from_iter(emb_sparse_titles)),
    Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(emb_dense_texts.into_iter(), 4096).unwrap()),
    Arc::new(BinaryArray::from_iter(emb_sparse_texts)),
  ]
}

#[rustfmt::skip]
fn comment_rows_to_columnar(comments: Vec<CommentRow>) -> Vec<ArrayRef> {
  let mut ids = Vec::new();
  let mut scores = Vec::new();
  let mut texts = Vec::new();
  let mut parents = Vec::new();
  let mut authors = Vec::new();
  let mut tss = Vec::new();
  let mut posts = Vec::new();
  let mut emb_dense_texts = Vec::new();
  let mut emb_sparse_texts = Vec::new();
  for c in comments {
    ids.push(c.id);
    scores.push(c.score);
    texts.push(c.text);
    parents.push(c.parent);
    authors.push(c.author);
    tss.push(c.ts.map(|ts| ts.timestamp()).unwrap_or(0));
    posts.push(c.post);
    emb_dense_texts.push(c.emb_dense_text);
    emb_sparse_texts.push(c.emb_sparse_text);
  };
  vec![
    Arc::new(UInt64Array::from(ids)),
    Arc::new(Int16Array::from(scores)),
    Arc::new(StringArray::from(texts)),
    Arc::new(UInt64Array::from(parents)),
    Arc::new(StringArray::from(authors)),
    Arc::new(TimestampSecondArray::from(tss).with_timezone_utc()),
    Arc::new(UInt64Array::from(posts)),
    Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(emb_dense_texts.into_iter(), 4096).unwrap()),
    Arc::new(BinaryArray::from_iter(emb_sparse_texts)),
  ]
}

#[rustfmt::skip]
fn user_rows_to_columnar(users: Vec<UserRow>) -> Vec<ArrayRef> {
  let mut ids = Vec::new();
  let mut names = Vec::new();
  for user in users {
    ids.push(user.id);
    names.push(user.name);
  };
  vec![
    Arc::new(UInt64Array::from(ids)),
    Arc::new(StringArray::from(names)),
  ]
}

#[rustfmt::skip]
fn interaction_rows_to_columnar(interactions: Vec<InteractionRow>) -> Vec<ArrayRef> {
  let mut users = Vec::new();
  let mut posts = Vec::new();
  for int in interactions {
    users.push(int.user);
    posts.push(int.post);
  };
  vec![
    Arc::new(UInt64Array::from(users)),
    Arc::new(UInt64Array::from(posts)),
  ]
}

fn create_dense_embedding_field(name: &'static str) -> Field {
  // Embeddings are null if the source input text is empty.
  Field::new(name, DataType::FixedSizeBinary(1024 * 4), true)
}

fn create_sparse_embedding_field(name: &'static str) -> Field {
  // Embeddings are null if the source input text is empty.
  Field::new(name, DataType::Binary, true)
}

#[tokio::main]
async fn main() {
  let client = DbRpcClient::new();

  let post_schema = Schema::new(vec![
    Field::new("id", DataType::UInt64, false),
    Field::new("score", DataType::Int16, false),
    Field::new("title", DataType::Utf8, false),
    Field::new("text", DataType::Utf8, false),
    Field::new("author", DataType::Utf8, false), // Empty string if null.
    Field::new(
      "ts",
      DataType::Timestamp(arrow::datatypes::TimeUnit::Second, Some("+00:00".into())),
      false,
    ), // 1970 if null.
    Field::new("url", DataType::Utf8, false),    // Empty string if null.
    create_dense_embedding_field("emb_dense_title"),
    create_sparse_embedding_field("emb_sparse_title"),
    create_dense_embedding_field("emb_dense_text"),
    create_sparse_embedding_field("emb_sparse_text"),
  ]);

  let comment_schema = Schema::new(vec![
    Field::new("id", DataType::UInt64, false),
    Field::new("score", DataType::Int16, false),
    Field::new("text", DataType::Utf8, false),
    Field::new("parent", DataType::UInt64, false),
    Field::new("author", DataType::Utf8, false), // Empty string if null.
    Field::new(
      "ts",
      DataType::Timestamp(arrow::datatypes::TimeUnit::Second, Some("+00:00".into())),
      false,
    ), // 1970 if null.
    Field::new("post", DataType::UInt64, false),
    create_dense_embedding_field("emb_dense_text"),
    create_sparse_embedding_field("emb_sparse_text"),
  ]);

  let user_schema = Schema::new(vec![
    Field::new("id", DataType::UInt64, false),
    Field::new("name", DataType::Utf8, false),
  ]);

  let interaction_schema = Schema::new(vec![
    Field::new("user", DataType::UInt64, false),
    Field::new("post", DataType::UInt64, false),
  ]);

  let mut out_posts = ArrowIpcOutput::new("posts", post_schema, post_rows_to_columnar);
  let mut out_comments = ArrowIpcOutput::new("comments", comment_schema, comment_rows_to_columnar);
  let mut out_users = ArrowIpcOutput::new("users", user_schema, user_rows_to_columnar);
  let mut out_interactions = ArrowIpcOutput::new(
    "interactions",
    interaction_schema,
    interaction_rows_to_columnar,
  );

  let mut user_ids = AHashMap::<String, u64>::new();
  // Map from user ID => post IDs. We want post IDs to be sorted chronologically.
  let mut interactions = AHashMap::<u64, BTreeSet<u64>>::new();

  // We can't fit all the data in memory.
  let mut next_post_id = 0;
  loop {
    let posts = client.query::<PostRow>(
      r#"
        select id, score, title, text, author, ts, url, emb_dense_title, emb_sparse_title, emb_dense_text, emb_sparse_text
        from hn_post
        where id >= ?
          and not deleted
          and not dead
          and title != ''
        order by id
        limit 100000
      "#,
      vec![next_post_id.into()],
    ).await;
    let n = posts.len();
    if n == 0 {
      println!("end of posts");
      break;
    };
    println!("fetch {} posts from ID {}", n, next_post_id);
    next_post_id = posts.last().unwrap().id + 1;

    for p in posts {
      if let Some(author) = p.author.clone() {
        let new_user_id = user_ids.len() as u64;
        let user_id = *user_ids.entry(author).or_insert(new_user_id);
        interactions.entry(user_id).or_default().insert(p.id);
      };
      out_posts.push(p);
    }
  }

  let mut next_comment_id = 0;
  loop {
    let comments = client
      .query::<CommentRow>(
        r#"
          select id, score, text, parent, author, ts, post, emb_dense_text, emb_sparse_text
          from hn_comment
          where id >= ?
            and not deleted
            and not dead
            and text != ''
            and author is not null
          order by id
          limit 100000
        "#,
        vec![next_comment_id.into()],
      )
      .await;
    let n = comments.len();
    if n == 0 {
      println!("end of comments");
      break;
    };
    println!("fetch {} comments from ID {}", n, next_comment_id);
    next_comment_id = comments.last().unwrap().id + 1;

    for c in comments {
      let new_user_id = user_ids.len() as u64;
      let user_id = *user_ids.entry(c.author.clone()).or_insert(new_user_id);
      interactions.entry(user_id).or_default().insert(c.post);
      out_comments.push(c);
    }
  }

  println!("calculated {} users", user_ids.len());

  for (name, id) in user_ids {
    out_users.push(UserRow { id, name });
  }

  for (user, user_posts) in interactions {
    for post in user_posts {
      out_interactions.push(InteractionRow { user, post });
    }
  }

  out_posts.finish();
  out_comments.finish();
  out_users.finish();
  out_interactions.finish();
  println!("all done!");
}
