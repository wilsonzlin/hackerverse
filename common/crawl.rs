use crate::parse::parse_html;
use chrono::DateTime;
use chrono::Utc;
use db_rpc_client_rs::DbRpcDbClient;
use reqwest::header::CONTENT_TYPE;
use reqwest::Response;
use serde::Deserialize;
use tokio::task::spawn_blocking;

#[derive(Deserialize)]
pub struct CrawlTask {
  pub id: u64,
  pub proto: String,
  pub url: String,
}

pub fn datetime_to_rmpv(dt: DateTime<Utc>) -> rmpv::Value {
  rmpv::Value::Ext(
    -1,
    u32::try_from(dt.timestamp())
      .unwrap()
      .to_be_bytes()
      .to_vec(),
  )
}

pub fn reqwest_error_to_code(err: &reqwest::Error) -> String {
  if err.is_body() {
    "body".to_string()
  } else if err.is_builder() {
    "builder".to_string()
  } else if err.is_connect() {
    "connect".to_string()
  } else if err.is_decode() {
    "decode".to_string()
  } else if err.is_redirect() {
    "redirect".to_string()
  } else if err.is_request() {
    "request".to_string()
  } else if err.is_status() {
    format!("status:{}", err.status().unwrap().as_u16())
  } else if err.is_timeout() {
    "timeout".to_string()
  } else {
    "unknown".to_string()
  }
}

pub fn get_content_type(res: &Response) -> Option<String> {
  res
    .headers()
    .get(CONTENT_TYPE)
    .and_then(|v| v.to_str().map(|v| v.to_string()).ok())
}

// If Content-Type is omitted, that's fine, but if it exists, it must be application/xhtml* or text/html* or text/xhtml*.
pub fn is_valid_content_type(ct: Option<&String>) -> bool {
  ct.is_none()
    || ct.is_some_and(|ct| {
      let ct = ct.to_lowercase();
      ct.starts_with("application/xhtml")
        || ct.starts_with("text/html")
        || ct.starts_with("text/xhtml")
    })
}

pub async fn check_if_already_crawled(db: &DbRpcDbClient, id: u64) -> bool {
  #[allow(unused)]
  #[derive(Deserialize)]
  struct Row {
    exists: bool,
  }
  let existing: Option<Row> = db
    .query(
      "select true as exists from url where id = ? and fetched is not null and fetch_err is null",
      vec![id.into()],
    )
    .await
    .unwrap()
    .pop();
  existing.is_some()
}

pub struct ProcessCrawlArgs {
  pub db: DbRpcDbClient,
  pub fetch_started: DateTime<Utc>,
  pub fetched_via: Option<&'static str>,
  pub html: String,
  pub url_id: u64,
  pub url: String,
}

pub async fn process_crawl(
  ProcessCrawlArgs {
    db,
    fetch_started,
    fetched_via,
    html,
    url_id,
    url,
  }: ProcessCrawlArgs,
) {
  let fetched_via = match fetched_via {
    Some(v) => rmpv::Value::String(v.into()),
    None => rmpv::Value::Nil,
  };

  let (meta, text) = spawn_blocking(move || parse_html(&html)).await.unwrap();

  db.batch(
    "insert into kv (k, v) values (?, ?) on duplicate key update v = values(v)",
    vec![
      vec![
        format!("url/{url_id}/text").into(),
        text.into_bytes().into(),
      ],
      vec![
        format!("url/{url_id}/meta").into(),
        rmp_serde::to_vec_named(&meta).unwrap().into(),
      ],
    ],
  )
  .await
  .unwrap();

  db.exec(
    "update url set fetched = ?, fetch_err = NULL, fetched_via = ? where url = ?",
    vec![datetime_to_rmpv(fetch_started), fetched_via, url.into()],
  )
  .await
  .unwrap();
}
