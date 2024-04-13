mod parse;

use cadence::Counted;
use cadence::CountedExt;
use cadence::StatsdClient;
use cadence::Timed;
use chrono::DateTime;
use chrono::Utc;
use common::create_db_client;
use common::create_queue_client;
use common::create_statsd;
use db_rpc_client_rs::DbRpcDbClient;
use itertools::Itertools;
use parse::parse_html;
use queued_client_rs::QueuedQueueClient;
use rand::thread_rng;
use rand::Rng;
use reqwest::header::ACCEPT;
use reqwest::header::ACCEPT_LANGUAGE;
use reqwest::header::CONTENT_TYPE;
use reqwest::Client;
use serde::Deserialize;
use service_toolkit::panic::set_up_panic_hook;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::task::spawn_blocking;
use tokio::time::Instant;

// Designed to run on 1 CPU core and 4 GB RAM.
const CONTENT_CRAWL_CONCURRENCY: usize = 256;

#[derive(Deserialize)]
struct CrawlTask {
  id: u64,
  proto: String,
  url: String,
}

fn datetime_to_rmpv(dt: DateTime<Utc>) -> rmpv::Value {
  rmpv::Value::Ext(
    -1,
    u32::try_from(dt.timestamp())
      .unwrap()
      .to_be_bytes()
      .to_vec(),
  )
}

fn reqwest_error_to_code(err: reqwest::Error) -> String {
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

async fn make_request(client: &Client, url: impl AsRef<str>) -> Result<String, String> {
  let res = client
    .get(url.as_ref())
    .header(ACCEPT, "text/html,application/xhtml+xml")
    .header(ACCEPT_LANGUAGE, "en-US,en;q=0.5")
    .send()
    .await
    .and_then(|res| res.error_for_status())
    .map_err(reqwest_error_to_code)?;
  let ct = res
    .headers()
    .get(CONTENT_TYPE)
    .and_then(|v| v.to_str().map(|v| v.to_string()).ok());
  // If Content-Type is omitted, that's fine, but if it exists, it must be text/html*.
  if ct.as_ref().is_some_and(|ct| !ct.starts_with("text/html")) {
    return Err(format!("content_type:{}", ct.unwrap()));
  };
  let raw = res.bytes().await.map_err(reqwest_error_to_code)?;
  String::from_utf8(raw.to_vec()).map_err(|_| "utf8".to_string())
}

async fn worker_loop(
  client: Client,
  db: DbRpcDbClient,
  queue: QueuedQueueClient,
  statsd: Arc<StatsdClient>,
) {
  loop {
    let Some(t) = queue
      .poll_messages(1, std::time::Duration::from_secs(60 * 20))
      .await
      .unwrap()
      .messages
      .pop()
    else {
      break;
    };
    let CrawlTask { id, proto, url } = rmp_serde::from_slice(&t.contents).unwrap();

    let fetch_started = Utc::now();
    let fetch_started_i = Instant::now();
    let res = make_request(&client, format!("{}//{}", proto, url)).await;
    statsd
      .time_with_tags("fetch_ms", fetch_started_i.elapsed())
      .with_tag("result", match res.as_ref().map_err(|v| v.as_str()) {
        Ok(_) => "ok",
        Err("timeout") => "timeout",
        Err(_) => "error",
      })
      .send();
    match res {
      Ok(html) => {
        statsd.count("fetch_bytes", html.len() as u64).unwrap();

        let parse_started = Instant::now();
        let (meta, text) = spawn_blocking(move || parse_html(&html)).await.unwrap();
        statsd.time("parse", parse_started.elapsed()).unwrap();

        db.batch(
          "insert into kv (k, v) values (?, ?) on duplicate key update v = values(v)",
          vec![
            vec![format!("url/{id}/text").into(), text.into_bytes().into()],
            vec![
              format!("url/{id}/meta").into(),
              rmp_serde::to_vec_named(&meta).unwrap().into(),
            ],
          ],
        )
        .await
        .unwrap();
        db.exec(
          "update url set fetched = ?, fetch_err = NULL where url = ?",
          vec![datetime_to_rmpv(fetch_started), url.into()],
        )
        .await
        .unwrap();
      }
      Err(err) => {
        statsd
          .incr_with_tags("fetch_err")
          .with_tag("error", &err)
          .send();
        if err == "status:429" {
          // Don't update the DB row, we're not finished.
          // Don't instead create a new message, as that could cause exponential explosion if two workers polled the same message somehow.
          let delay = thread_rng().gen_range(0..60 * 15);
          queue
            .update_message(t.message(), std::time::Duration::from_secs(delay))
            .await
            .unwrap();
          continue;
        };
        // Do not overwrite or delete existing text/meta in the KV table if this crawl has failed.
        db.exec(
          "update url set fetched = ?, fetch_err = ? where url = ?",
          vec![datetime_to_rmpv(fetch_started), err.into(), url.into()],
        )
        .await
        .unwrap();
      }
    };
    queue.delete_messages([t.message()]).await.unwrap();
  }
}

#[tokio::main]
async fn main() {
  set_up_panic_hook();

  let db = create_db_client();
  let queue = create_queue_client("hndr:crawl");
  let statsd = create_statsd("crawler");
  let client = reqwest::Client::builder()
    .connect_timeout(Duration::from_secs(20))
    .timeout(Duration::from_secs(60))
    .tcp_keepalive(None)
    .build()
    .unwrap();

  let workers = (0..CONTENT_CRAWL_CONCURRENCY)
    .map(|_| {
      spawn(worker_loop(
        client.clone(),
        db.clone(),
        queue.clone(),
        statsd.clone(),
      ))
    })
    .collect_vec();

  for w in workers {
    w.await.unwrap();
  }
  println!("All done!");
}
