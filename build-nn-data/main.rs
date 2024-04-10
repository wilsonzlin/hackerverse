use ahash::AHashMap;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::AsArray;
use arrow::array::BooleanArray;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::Float32Array;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::UInt64Type;
use arrow::ipc::reader::FileReader;
use chrono::Datelike;
use chrono::TimeDelta;
use chrono::TimeZone;
use chrono::Utc;
use common::arrow::ArrowIpcOutput;
use common::DbRpcClient;
use futures::Future;
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::VecDeque;
use std::fs::File;
use std::sync::Arc;

pub trait Row: DeserializeOwned {
  fn id(&self) -> u64;
}

pub struct DbRowStream<R: Row> {
  client: DbRpcClient,
  query: &'static str,
  next_id: u64,
  buf: VecDeque<R>,
  ended: bool,
}

impl<R: Row> DbRowStream<R> {
  pub fn new(client: DbRpcClient, query: &'static str) -> Self {
    Self {
      client,
      query,
      next_id: 0,
      buf: VecDeque::new(),
      ended: false,
    }
  }

  async fn maybe_fill(&mut self) {
    if self.ended || !self.buf.is_empty() {
      return;
    };

    let rows = self
      .client
      .query::<R>(self.query, vec![self.next_id.into()])
      .await;
    if rows.is_empty() {
      self.ended = true;
    } else {
      self.next_id = rows.last().unwrap().id() + 1;
      for r in rows {
        self.buf.push_back(r);
      }
    };
  }

  pub async fn peek(&mut self) -> Option<&R> {
    self.maybe_fill().await;
    self.buf.front()
  }

  pub async fn poll(&mut self) -> Option<R> {
    self.maybe_fill().await;
    self.buf.pop_front()
  }
}

#[derive(Deserialize)]
struct PostRow {
  id: u64,
  score: i16,
  ts: chrono::DateTime<Utc>,
  #[serde(with = "serde_bytes")]
  emb_dense_title: Vec<u8>,
}

impl Row for PostRow {
  fn id(&self) -> u64 {
    self.id
  }
}

#[derive(Deserialize)]
struct CommentRow {
  id: u64,
  author: String,
  #[serde(with = "serde_bytes")]
  emb_dense_text: Vec<u8>,
}

impl Row for CommentRow {
  fn id(&self) -> u64 {
    self.id
  }
}

struct PostDatapoint {
  id: u64,
  emb: [f32; 1024],
  score_ln: f32,
  ts: f32,              // Position within range [2005-01-01, 2035-01-01).
  day_of_year_sin: f32, // This also captures month.
  day_of_year_cos: f32,
  day_of_month_sin: f32,
  day_of_month_cos: f32,
  day_of_week_sin: f32,
  day_of_week_cos: f32,
}

fn posts_to_columnar(posts: Vec<PostDatapoint>) -> Vec<ArrayRef> {
  let mut ids = Vec::new();
  let mut embs = Vec::new();
  let mut score_lns = Vec::new();
  let mut tss = Vec::new();
  let mut day_of_year_sins = Vec::new();
  let mut day_of_year_coss = Vec::new();
  let mut day_of_month_sins = Vec::new();
  let mut day_of_month_coss = Vec::new();
  let mut day_of_week_sins = Vec::new();
  let mut day_of_week_coss = Vec::new();
  for p in posts {
    ids.push(p.id);
    embs.push(serialise_embedding(&p.emb));
    score_lns.push(p.score_ln);
    tss.push(p.ts);
    day_of_year_sins.push(p.day_of_year_sin);
    day_of_year_coss.push(p.day_of_year_cos);
    day_of_month_sins.push(p.day_of_month_sin);
    day_of_month_coss.push(p.day_of_month_cos);
    day_of_week_sins.push(p.day_of_week_sin);
    day_of_week_coss.push(p.day_of_week_cos);
  }
  vec![
    Arc::new(UInt64Array::from(ids)),
    Arc::new(FixedSizeBinaryArray::try_from_iter(embs.into_iter()).unwrap()),
    Arc::new(Float32Array::from(score_lns)),
    Arc::new(Float32Array::from(tss)),
    Arc::new(Float32Array::from(day_of_year_sins)),
    Arc::new(Float32Array::from(day_of_year_coss)),
    Arc::new(Float32Array::from(day_of_month_sins)),
    Arc::new(Float32Array::from(day_of_month_coss)),
    Arc::new(Float32Array::from(day_of_week_sins)),
    Arc::new(Float32Array::from(day_of_week_coss)),
  ]
}

// Comments are almost always within 48 hours of the post, so it's not really useful as an input (either relative to post or absolute). What would be interesting is if some post/topic is often engaged with far away vs. very recently relative to its creation, but that requires view data, not comment data, which HN doesn't expose.
struct InteractionDatapoint {
  user_id: u64,
  user_history: [f32; 1024],  // Up until this point.
  user_comments: [f32; 1024], // Up until this point.
  candidate_post_id: u64,
  did_interact: bool,
}

fn interactions_to_columnar(interactions: Vec<InteractionDatapoint>) -> Vec<ArrayRef> {
  let mut user_ids = Vec::new();
  let mut user_historys = Vec::new();
  let mut user_commentss = Vec::new();
  let mut candidate_post_ids = Vec::new();
  let mut did_interacts = Vec::new();
  for i in interactions {
    user_ids.push(i.user_id);
    user_historys.push(serialise_embedding(&i.user_history));
    user_commentss.push(serialise_embedding(&i.user_comments));
    candidate_post_ids.push(i.candidate_post_id);
    did_interacts.push(i.did_interact);
  }
  vec![
    Arc::new(UInt64Array::from(user_ids)),
    Arc::new(FixedSizeBinaryArray::try_from_iter(user_historys.into_iter()).unwrap()),
    Arc::new(FixedSizeBinaryArray::try_from_iter(user_commentss.into_iter()).unwrap()),
    Arc::new(UInt64Array::from(candidate_post_ids)),
    Arc::new(BooleanArray::from(did_interacts)),
  ]
}

pub struct AvgEmb {
  emb_sum: [f32; 1024],
  count: usize,
}

impl Default for AvgEmb {
  fn default() -> Self {
    Self {
      emb_sum: [0.0; 1024],
      count: 0,
    }
  }
}

impl AvgEmb {
  pub fn add(&mut self, emb: &[f32; 1024]) -> &mut Self {
    for i in 0..1024 {
      self.emb_sum[i] += emb[i];
    }
    self.count += 1;
    self
  }

  pub fn avg(&self) -> [f32; 1024] {
    let mut out = self.emb_sum.clone();
    if self.count > 0 {
      let d = self.count as f32;
      for i in 0..1024 {
        out[i] /= d;
      }
    };
    out
  }
}

async fn ensure_some_with<T, Fut: Future<Output = Option<T>>>(
  opt: &mut Option<T>,
  f: impl FnOnce() -> Fut,
) {
  if opt.is_none() {
    *opt = f().await;
  };
}

fn deserialise_embedding(raw: &[u8]) -> [f32; 1024] {
  assert_eq!(raw.len(), 1024 * 4);
  let mut out = [0.0; 1024];
  for (i, c) in raw.chunks(4).enumerate() {
    out[i] = f32::from_le_bytes(c.try_into().unwrap());
  }
  out
}

fn serialise_embedding(emb: &[f32; 1024]) -> Vec<u8> {
  emb.iter().flat_map(|e| e.to_le_bytes()).collect_vec()
}

#[tokio::main]
async fn main() {
  let client = DbRpcClient::new();

  // We load and use our prebuilt interactions data, as otherwise we'd have to store all posts (inc. embeddings) in memory for future comments.
  // Map from post ID to user IDs.
  let interactions = {
    let f = File::open("/hndr-data/interactions.arrow").unwrap();
    let rd = FileReader::try_new(f, None).unwrap();
    let mut map = AHashMap::<u64, Vec<u64>>::new();
    for batch in rd {
      let batch = batch.unwrap();
      let users = batch
        .column_by_name("user")
        .unwrap()
        .as_primitive::<UInt64Type>()
        .values();
      let posts = batch
        .column_by_name("post")
        .unwrap()
        .as_primitive::<UInt64Type>()
        .values();
      assert_eq!(users.len(), posts.len());
      for i in 0..users.len() {
        map.entry(posts[i]).or_default().push(users[i]);
      }
    }
    map
  };
  println!("Loaded interaction data");

  // Map from user ID to name.
  let user_id_to_name = {
    let f = File::open("/hndr-data/users.arrow").unwrap();
    let rd = FileReader::try_new(f, None).unwrap();
    let mut to_name = AHashMap::<u64, String>::new();
    for batch in rd {
      let batch = batch.unwrap();
      let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_primitive::<UInt64Type>()
        .values();
      // See StringArray type when building, which is an alias for GenericStringArray<i32>.
      let names = batch.column_by_name("name").unwrap().as_string::<i32>();
      assert_eq!(ids.len(), names.len());
      for i in 0..ids.len() {
        let name = names.value(i).to_string();
        assert!(to_name.insert(ids[i], name).is_none());
      }
    }
    to_name
  };
  println!("Loaded user IDs");

  let post_ts_min = Utc
    .with_ymd_and_hms(2005, 1, 1, 0, 0, 0)
    .unwrap()
    .timestamp();
  let post_ts_max = Utc
    .with_ymd_and_hms(2035, 1, 1, 0, 0, 0)
    .unwrap()
    .timestamp();
  let post_ts_range = (post_ts_max - post_ts_min) as f32;
  fn tsc(x: f32, max: f32) -> f32 {
    (2.0 * std::f32::consts::PI * x) / (max + 1.0)
  }
  let mut out_interactions = ArrowIpcOutput::new(
    "nndata_interactions",
    Schema::new(vec![
      Field::new("user_id", DataType::UInt64, false),
      Field::new("user_history", DataType::FixedSizeBinary(4096), false),
      Field::new("user_comments", DataType::FixedSizeBinary(4096), false),
      Field::new("candidate_post_id", DataType::UInt64, false),
      Field::new("did_interact", DataType::Boolean, false),
    ]),
    interactions_to_columnar,
  );
  let mut out_posts = ArrowIpcOutput::new(
    "nndata_posts",
    Schema::new(vec![
      Field::new("id", DataType::UInt64, false),
      Field::new("emb", DataType::FixedSizeBinary(4096), false),
      Field::new("score_ln", DataType::Float32, false),
      Field::new("ts", DataType::Float32, false),
      Field::new("day_of_year_sin", DataType::Float32, false),
      Field::new("day_of_year_cos", DataType::Float32, false),
      Field::new("day_of_month_sin", DataType::Float32, false),
      Field::new("day_of_month_cos", DataType::Float32, false),
      Field::new("day_of_week_sin", DataType::Float32, false),
      Field::new("day_of_week_cos", DataType::Float32, false),
    ]),
    posts_to_columnar,
  );
  let mut user_histories = AHashMap::<String, AvgEmb>::new();
  let mut user_comments = AHashMap::<String, AvgEmb>::new();

  let mut post_stream = DbRowStream::<PostRow>::new(
    client.clone(),
    r#"
      select id, score, ts, emb_dense_title
      from hn_post
      where id >= ?
        and not deleted
        and not dead
        and title != ''
        and ts is not null
        and emb_dense_title is not null
      order by id
      limit 100000
    "#,
  );
  let mut comment_stream = DbRowStream::<CommentRow>::new(
    client.clone(),
    r#"
      select id, author, emb_dense_text
      from hn_comment
      where id >= ?
        and not deleted
        and not dead
        and text != ''
        and author is not null
        and emb_dense_text is not null
      order by id
      limit 100000
    "#,
  );

  let mut buf_comment: Option<CommentRow> = None;
  let mut buf_post: Option<PostRow> = None;
  enum Item {
    Comment(CommentRow),
    Post(PostRow),
  }
  loop {
    let item: Item = {
      ensure_some_with(&mut buf_comment, || async { comment_stream.poll().await }).await;
      ensure_some_with(&mut buf_post, || async { post_stream.poll().await }).await;
      let post_id = buf_post.as_ref().map(|p| p.id).unwrap_or(u64::MAX);
      let comment_id = buf_comment.as_ref().map(|c| c.id).unwrap_or(u64::MAX);
      match (post_id, comment_id) {
        (u64::MAX, u64::MAX) => break,
        (p, c) if p < c => Item::Post(buf_post.take().unwrap()),
        (p, c) if p > c => Item::Comment(buf_comment.take().unwrap()),
        _ => unreachable!(),
      }
    };
    match item {
      Item::Comment(c) => {
        let emb = deserialise_embedding(&c.emb_dense_text);
        user_comments.entry(c.author).or_default().add(&emb);
      }
      Item::Post(p) => {
        let Some(user_ids) = interactions.get(&p.id) else {
          continue;
        };
        let emb = deserialise_embedding(&p.emb_dense_title);
        for &user_id in user_ids {
          let user_name = user_id_to_name.get(&user_id).unwrap();
          let user_history = user_histories
            .entry(user_name.clone())
            .or_default()
            .add(&emb)
            .avg();
          out_interactions.push(InteractionDatapoint {
            user_id,
            user_history,
            user_comments: user_comments.entry(user_name.clone()).or_default().avg(),
            candidate_post_id: p.id,
            did_interact: true,
          });
          // TODO Sample negatives.
        }
        let ts_day_of_year = {
          let diff = p.ts.timestamp()
            - Utc
              .with_ymd_and_hms(p.ts.year(), 1, 1, 0, 0, 0)
              .unwrap()
              .timestamp();
          (diff as f32) / 60.0 * 60.0 * 24.0
        };
        let ts_day_of_month = {
          let diff = p.ts.timestamp()
            - Utc
              .with_ymd_and_hms(p.ts.year(), p.ts.month(), 1, 0, 0, 0)
              .unwrap()
              .timestamp();
          (diff as f32) / 60.0 * 60.0 * 24.0
        };
        let ts_days_in_month = {
          let mth = Utc
            .with_ymd_and_hms(p.ts.year(), p.ts.month(), 1, 0, 0, 0)
            .unwrap();
          let nxt = match p.ts.month() {
            12 => Utc.with_ymd_and_hms(p.ts.year() + 1, 1, 1, 0, 0, 0),
            m => Utc.with_ymd_and_hms(p.ts.year(), m + 1, 1, 0, 0, 0),
          }
          .unwrap();
          let diff = nxt.timestamp() - mth.timestamp();
          (diff as f32) / 60.0 * 60.0 * 24.0
        };
        let ts_day_of_week = {
          let diff = p.ts.timestamp()
            - (Utc
              .with_ymd_and_hms(p.ts.year(), p.ts.month(), p.ts.day(), 0, 0, 0)
              .unwrap()
              - TimeDelta::days(p.ts.weekday().num_days_from_sunday().into()))
            .timestamp();
          (diff as f32) / 60.0 * 60.0 * 24.0
        };
        #[rustfmt::skip]
        let pdp = PostDatapoint {
          id: p.id,
          emb,
          score_ln: (p.score.max(1) as f32).ln(),
          ts: (p.ts.timestamp() - post_ts_min) as f32 / post_ts_range,
          day_of_year_sin: tsc(ts_day_of_year, 365.2422).sin(),
          day_of_year_cos: tsc(ts_day_of_year, 365.2422).cos(),
          day_of_month_sin: tsc(ts_day_of_month, ts_days_in_month).sin(),
          day_of_month_cos: tsc(ts_day_of_month, ts_days_in_month).cos(),
          day_of_week_sin: tsc(ts_day_of_week, 7.0).sin(),
          day_of_week_cos: tsc(ts_day_of_week, 7.0).cos(),
        };
        out_posts.push(pdp);
      }
    };
  }

  out_interactions.finish();
  out_posts.finish();
}
