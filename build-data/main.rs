use ahash::AHashMap;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::Int16Array;
use arrow::array::StringArray;
use arrow::array::TimestampSecondArray;
use arrow::array::UInt32Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use chrono::DateTime;
use chrono::Utc;
use common::arrow::ArrowIpcOutput;
use common::create_db_client;
use common::msgpack::decode_msgpack_timestamp;
use regex::Regex;
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

#[derive(Deserialize)]
struct UserRow {
  id: u32,
  username: String,
}

impl UserRow {
  #[rustfmt::skip]
  pub fn to_columnar(users: Vec<UserRow>) -> Vec<ArrayRef> {
    let mut ids = Vec::new();
    let mut usernames = Vec::new();
    for user in users {
      ids.push(user.id);
      usernames.push(user.username);
    };
    vec![
      Arc::new(UInt32Array::from(ids)),
      Arc::new(StringArray::from(usernames)),
    ]
  }
}

// These are not stored in the database, but are derived.

// Used for comment embeddings, post embeddings.
struct EmbeddingRow {
  id: u32,
  emb: [u8; 512 * 4],
}

impl EmbeddingRow {
  #[rustfmt::skip]
  pub fn to_columnar(rows: Vec<Self>) -> Vec<ArrayRef> {
    let mut ids = Vec::new();
    let mut embs = Vec::new();
    for r in rows {
      ids.push(r.id);
      embs.push(r.emb);
    };
    vec![
      Arc::new(UInt32Array::from(ids)),
      Arc::new(FixedSizeBinaryArray::try_from_iter(embs.into_iter()).unwrap()),
    ]
  }
}

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

// Used for comment texts, post titles, post texts.
struct TextRow {
  id: u32,
  text: String,
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

fn ts_field(name: &'static str) -> Field {
  Field::new(
    name,
    DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
    false,
  )
}

#[tokio::main]
async fn main() {
  let client = create_db_client();

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

  let url_schema = Schema::new(vec![
    Field::new("id", DataType::UInt32, false),
    Field::new("url", DataType::Utf8, false),
    Field::new("proto", DataType::Utf8, false),
    ts_field("fetched"),                                      // 1970 if null.
    Field::new("fetch_err", DataType::Utf8, false),           // Empty string if null.
    Field::new("fetched_via", DataType::Utf8, false),         // Empty string if null.
    Field::new("found_in_archive", DataType::Boolean, false), // False if null.
  ]);

  let user_schema = Schema::new(vec![
    Field::new("id", DataType::UInt32, false),
    Field::new("username", DataType::Utf8, false),
  ]);

  let emb_schema = Schema::new(vec![
    Field::new("id", DataType::UInt32, false),
    Field::new("emb", DataType::FixedSizeBinary(512 * 4), false),
  ]);

  let interaction_schema = Schema::new(vec![
    Field::new("user", DataType::UInt32, false),
    Field::new("post", DataType::UInt32, false),
  ]);

  let text_schema = Schema::new(vec![
    Field::new("id", DataType::UInt32, false),
    Field::new("text", DataType::Utf8, false),
  ]);

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

  let mut out_comment_embs = ArrowIpcOutput::new(
    "comment_embs",
    emb_schema.clone(),
    EmbeddingRow::to_columnar,
  );
  let mut out_comment_texts =
    ArrowIpcOutput::new("comment_texts", text_schema.clone(), TextRow::to_columnar);
  let mut out_comments = ArrowIpcOutput::new("comments", comment_schema, CommentRow::to_columnar);
  let mut out_interactions = ArrowIpcOutput::new(
    "interactions",
    interaction_schema,
    InteractionRow::to_columnar,
  );
  let mut out_post_embs =
    ArrowIpcOutput::new("post_embs", emb_schema.clone(), EmbeddingRow::to_columnar);
  let mut out_post_texts =
    ArrowIpcOutput::new("post_texts", text_schema.clone(), TextRow::to_columnar);
  let mut out_post_titles =
    ArrowIpcOutput::new("post_titles", text_schema.clone(), TextRow::to_columnar);
  let mut out_posts = ArrowIpcOutput::new("posts", post_schema, PostRow::to_columnar);
  let mut out_urls = ArrowIpcOutput::new("urls", url_schema, UrlRow::to_columnar);
  let mut out_url_metas =
    ArrowIpcOutput::new("url_metas", url_meta_schema, UrlMetaRow::to_columnar);
  let mut out_url_texts =
    ArrowIpcOutput::new("url_texts", text_schema.clone(), TextRow::to_columnar);
  let mut out_users = ArrowIpcOutput::new("users", user_schema, UserRow::to_columnar);

  // Map from user ID => post IDs. We want post IDs to be sorted chronologically.
  let mut interactions = AHashMap::<u32, BTreeSet<u32>>::new();

  // We can't fit all the data in memory.

  let mut next_comment_id = 0;
  loop {
    let rows = client
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

  let mut next_post_id = 0;
  loop {
    let rows = client
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

  let mut next_url_id = 0;
  loop {
    let rows = client
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

  let mut next_usr_id = 0;
  loop {
    let rows = client
      .query::<UserRow>(
        r#"
          select *
          from usr
          where id >= ?
          order by id
          limit 10000000
        "#,
        vec![next_usr_id.into()],
      )
      .await
      .unwrap();
    let n = rows.len();
    if n == 0 {
      println!("end of users");
      break;
    };
    println!("fetch {} users from ID {}", n, next_usr_id);
    next_usr_id = rows.last().unwrap().id + 1;

    for r in rows {
      out_users.push(r);
    }
  }

  #[derive(Deserialize)]
  struct KvRow {
    k: String,
    #[serde(with = "serde_bytes")]
    v: Vec<u8>,
  }
  let mut next_k = String::new();
  let re_comment_emb = Regex::new(r"^comment/([0-9]+)/emb$").unwrap();
  let re_comment_text = Regex::new(r"^comment/([0-9]+)/text$").unwrap();
  let re_post_emb = Regex::new(r"^post/([0-9]+)/emb$").unwrap();
  let re_post_text = Regex::new(r"^post/([0-9]+)/text$").unwrap();
  let re_post_title = Regex::new(r"^post/([0-9]+)/title$").unwrap();
  let re_url_meta = Regex::new(r"^url/([0-9]+)/meta$").unwrap();
  let re_url_text = Regex::new(r"^url/([0-9]+)/text$").unwrap();
  loop {
    let rows = client
      .query::<KvRow>(
        r#"
          select *
          from kv
          where k > ?
          order by k
          limit 123456
        "#,
        vec![next_k.as_str().into()],
      )
      .await
      .unwrap();
    let n = rows.len();
    if n == 0 {
      println!("end of KV rows");
      break;
    };
    println!("fetch {} KV rows from key {}", n, next_k);
    next_k = rows.last().unwrap().k.clone();

    for r in rows {
      if r.k.ends_with("/emb_input") {
        continue;
      };

      if let Some(m) = re_comment_emb.captures(&r.k) {
        let comment_id: u32 = m.get(1).unwrap().as_str().parse().unwrap();
        out_comment_embs.push(EmbeddingRow {
          id: comment_id,
          emb: r.v.try_into().unwrap(),
        });
      } else if let Some(m) = re_comment_text.captures(&r.k) {
        let comment_id: u32 = m.get(1).unwrap().as_str().parse().unwrap();
        out_comment_texts.push(TextRow {
          id: comment_id,
          text: String::from_utf8(r.v).unwrap(),
        });
      } else if let Some(m) = re_post_emb.captures(&r.k) {
        let post_id: u32 = m.get(1).unwrap().as_str().parse().unwrap();
        out_post_embs.push(EmbeddingRow {
          id: post_id,
          emb: r.v.try_into().unwrap(),
        });
      } else if let Some(m) = re_post_title.captures(&r.k) {
        let post_id: u32 = m.get(1).unwrap().as_str().parse().unwrap();
        out_post_titles.push(TextRow {
          id: post_id,
          text: String::from_utf8(r.v).unwrap(),
        });
      } else if let Some(m) = re_post_text.captures(&r.k) {
        let post_id: u32 = m.get(1).unwrap().as_str().parse().unwrap();
        out_post_texts.push(TextRow {
          id: post_id,
          text: String::from_utf8(r.v).unwrap(),
        });
      } else if let Some(m) = re_url_meta.captures(&r.k) {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Meta {
          description: Option<String>,
          image_url: Option<String>,
          lang: Option<String>,
          snippet: Option<String>,
          // Timestamps are encoded as ext(-1) by rmp_serde, but can't be decoded back to a timestamp, so we must manually do so.
          timestamp: Option<rmpv::Value>,
          timestamp_modified: Option<rmpv::Value>,
          title: Option<String>,
        }
        let meta: Meta = rmp_serde::from_slice(&r.v).unwrap();
        out_url_metas.push(UrlMetaRow {
          id: m.get(1).unwrap().as_str().parse().unwrap(),
          description: meta.description.unwrap_or_default(),
          image_url: meta.image_url.unwrap_or_default(),
          lang: meta.lang.unwrap_or_default(),
          snippet: meta.snippet.unwrap_or_default(),
          timestamp: meta
            .timestamp
            .map(decode_msgpack_timestamp)
            .unwrap_or_default(),
          timestamp_modified: meta
            .timestamp_modified
            .map(decode_msgpack_timestamp)
            .unwrap_or_default(),
          title: meta.title.unwrap_or_default(),
        });
      } else if let Some(m) = re_url_text.captures(&r.k) {
        let url_id: u32 = m.get(1).unwrap().as_str().parse().unwrap();
        out_url_texts.push(TextRow {
          id: url_id,
          text: String::from_utf8(r.v).unwrap(),
        });
      } else {
        panic!("unknown key: {}", r.k);
      };
    }
  }

  for (user, user_posts) in interactions {
    for post in user_posts {
      out_interactions.push(InteractionRow { user, post });
    }
  }

  out_comment_embs.finish();
  out_comment_texts.finish();
  out_comments.finish();
  out_interactions.finish();
  out_post_embs.finish();
  out_post_texts.finish();
  out_post_titles.finish();
  out_posts.finish();
  out_url_metas.finish();
  out_url_texts.finish();
  out_urls.finish();
  out_users.finish();
  println!("all done!");
}
