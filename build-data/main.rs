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
use std::collections::BTreeSet;
use std::fs::File;
use std::sync::Arc;

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
    author: Option<String>,
    ts: Option<chrono::DateTime<Utc>>,
    post: u64,
    #[serde(with = "serde_bytes")]
    emb_dense_text: Option<Vec<u8>>,
    #[serde(with = "serde_bytes")]
    emb_sparse_text: Option<Vec<u8>>,
  }

  fn new_output_file(name: &str, schema: &Schema) -> FileWriter<File> {
    let out = File::create(format!("/hndr-data/{name}.arrow")).unwrap();
    FileWriter::try_new(out, schema).unwrap()
  }

  // Build Arrow data.
  fn flush(out: &mut FileWriter<File>, schema: &Schema, data: Vec<ArrayRef>) {
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), data).unwrap();
    out.write(&batch).unwrap();
  }

  let mut out_posts = new_output_file("posts", &post_schema);
  let mut out_comments = new_output_file("comments", &comment_schema);
  let mut out_users = new_output_file("users", &user_schema);
  let mut out_interactions = new_output_file("interactions", &interaction_schema);

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

    for p in posts.iter() {
      if let Some(author) = p.author.clone() {
        let new_user_id = user_ids.len() as u64;
        let user_id = *user_ids.entry(author).or_insert(new_user_id);
        interactions.entry(user_id).or_default().insert(p.id);
      };
    }

    #[rustfmt::skip]
    flush(&mut out_posts, &post_schema, {
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
    });
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

    for c in comments.iter() {
      if let Some(author) = c.author.clone() {
        let new_user_id = user_ids.len() as u64;
        let user_id = *user_ids.entry(author).or_insert(new_user_id);
        interactions.entry(user_id).or_default().insert(c.post);
      };
    }

    #[rustfmt::skip]
    flush(&mut out_comments, &comment_schema, {
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
    });
  }

  println!("calculated {} users", user_ids.len());

  #[rustfmt::skip]
  flush(&mut out_users, &user_schema, {
    let mut ids = Vec::new();
    let mut names = Vec::new();
    for (name, id) in user_ids {
      ids.push(id);
      names.push(name);
    };
    vec![
      Arc::new(UInt64Array::from(ids)),
      Arc::new(StringArray::from(names)),
    ]
  });

  #[rustfmt::skip]
  flush(&mut out_interactions, &interaction_schema, {
    let mut users = Vec::new();
    let mut posts = Vec::new();
    for (user, user_posts) in interactions {
      for post in user_posts {
        users.push(user);
        posts.push(post);
      };
    };
    vec![
      Arc::new(UInt64Array::from(users)),
      Arc::new(UInt64Array::from(posts)),
    ]
  });

  out_posts.finish().unwrap();
  out_comments.finish().unwrap();
  out_users.finish().unwrap();
  out_interactions.finish().unwrap();
  println!("all done!");
}
