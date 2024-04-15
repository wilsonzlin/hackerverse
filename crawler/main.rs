mod origin;
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
use dashmap::DashMap;
use db_rpc_client_rs::DbRpcDbClient;
use futures::TryFutureExt;
use itertools::Itertools;
use origin::Origin;
use parse::parse_html;
use queued_client_rs::QueuedQueueClient;
use rand::thread_rng;
use rand::Rng;
use reqwest::header::ACCEPT;
use reqwest::header::ACCEPT_LANGUAGE;
use reqwest::header::CONTENT_TYPE;
use reqwest::Client;
use reqwest::Response;
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

fn reqwest_error_to_code(err: &reqwest::Error) -> String {
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

// If Content-Type is omitted, that's fine, but if it exists, it must be text/html*.
fn is_valid_content_type(ct: Option<&String>) -> bool {
  ct.is_some_and(|ct| !ct.starts_with("text/html"))
}

fn get_content_type(res: &Response) -> Option<String> {
  res
    .headers()
    .get(CONTENT_TYPE)
    .and_then(|v| v.to_str().map(|v| v.to_string()).ok())
}

async fn make_request(client: &Client, url: impl AsRef<str>) -> Result<String, String> {
  let res = client
    .get(url.as_ref())
    .header(ACCEPT, "text/html,application/xhtml+xml")
    .header(ACCEPT_LANGUAGE, "en-US,en;q=0.5")
    .send()
    .await
    .and_then(|res| res.error_for_status())
    .map_err(|err| reqwest_error_to_code(&err))?;
  let ct = get_content_type(&res);
  if !is_valid_content_type(ct.as_ref()) {
    return Err(format!("content_type:{}", ct.unwrap()));
  };
  let raw = res
    .bytes()
    .await
    .map_err(|err| reqwest_error_to_code(&err))?;
  String::from_utf8(raw.to_vec()).map_err(|_| "utf8".to_string())
}

// Return value:
// - Err(reqwest::Error): something went wrong when interacting with the Internet Archive, either the API or the archived content. Make sure to check we're not being rate limited or the server is down, so as to not continue retrying excessively.
// - Ok(None): no archived content is available.
// - Ok(Some(String)): the archived HTML.
async fn try_internet_archive(
  statsd: &Arc<StatsdClient>,
  client: &Client,
  url: impl AsRef<str>,
) -> reqwest::Result<Option<String>> {
  #[derive(Deserialize)]
  #[allow(unused)]
  struct AvailableArchivedSnapshotsClosest {
    status: u16,
    available: bool,
    url: String,
    timestamp: String,
  }
  #[derive(Deserialize)]
  struct AvailableArchivedSnapshots {
    closest: Option<AvailableArchivedSnapshotsClosest>,
  }
  #[derive(Deserialize)]
  #[allow(unused)]
  struct Available {
    url: String,
    archived_snapshots: AvailableArchivedSnapshots,
  }
  let a_started = Instant::now();
  let a = client
    .get("https://archive.org/wayback/available")
    .query(&[("url", url.as_ref())])
    .send()
    .and_then(|res| async { res.error_for_status() })
    .and_then(|res| res.json::<Available>())
    .await;
  statsd
    .time_with_tags(
      "internet_archive_available_api_call_ms",
      a_started.elapsed(),
    )
    .with_tag(
      "error",
      &a.as_ref()
        .err()
        .map(reqwest_error_to_code)
        .unwrap_or_default(),
    )
    .send();
  let a = a?;
  let Some(c) = a
    .archived_snapshots
    .closest
    .filter(|c| c.available && c.status >= 200 && c.status <= 299)
  else {
    return Ok(None);
  };
  let res_started = Instant::now();
  let res = client
    .get(c.url)
    .send()
    .and_then(|res| async { res.error_for_status() })
    .and_then(|res| async {
      let ct = get_content_type(&res);
      res.text().await.map(|text| (ct, text))
    })
    .await;
  statsd
    .time_with_tags("internet_archive_fetch_ms", res_started.elapsed())
    .with_tag(
      "error",
      &res
        .as_ref()
        .err()
        .map(reqwest_error_to_code)
        .unwrap_or_default(),
    )
    .send();
  let (ct, res) = res?;
  if !is_valid_content_type(ct.as_ref()) {
    return Ok(None);
  };
  Ok(Some(res))
}

async fn worker_loop(
  origins: Arc<DashMap<String, Origin>>,
  client: Client,
  db: DbRpcDbClient,
  queue: QueuedQueueClient,
  statsd: Arc<StatsdClient>,
) {
  loop {
    let Some(t) = queue
      .poll_messages(1, Duration::from_secs(60 * 20))
      .await
      .unwrap()
      .messages
      .pop()
    else {
      break;
    };
    let CrawlTask { id, proto, url } = rmp_serde::from_slice(&t.contents).unwrap();

    let origin = url.split_once('/').unwrap().0;
    if !origins.entry(origin.to_string()).or_default().can_request() {
      statsd
        .incr_with_tags("fetch_err")
        .with_tag("error", "rate_limit")
        .send();
      let delay = thread_rng().gen_range(0..60 * 120);
      queue
        .update_message(t.message(), Duration::from_secs(delay))
        .await
        .unwrap();
      continue;
    }

    let fetch_started = Utc::now();
    let fetch_started_i = Instant::now();
    let url_with_proto = format!("{}//{}", proto, url);
    let mut res = make_request(&client, &url_with_proto).await;
    statsd
      .time_with_tags("fetch_ms", fetch_started_i.elapsed())
      .with_tag("result", match res.as_ref().map_err(|v| v.as_str()) {
        Ok(_) => "ok",
        Err("timeout") => "timeout",
        Err(_) => "error",
      })
      .send();
    let mut fetched_via = rmpv::Value::Nil;
    if res.as_ref().is_err_and(|err| {
      matches!(
        err.as_str(),
        "connect"
          | "request"
          | "timeout"
          | "status:401"
          | "status:403"
          | "status:404"
          | "status:429"
      )
    }) {
      // Second chance: try using the Internet Archive.
      match try_internet_archive(&statsd, &client, &url_with_proto).await {
        Err(err) => {
          // TODO
        }
        Ok(None) => {
          // TODO
        }
        Ok(Some(html)) => {
          res = Ok(html);
          fetched_via = rmpv::Value::String("internet_archive".into());
        }
      };
    };
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
          "update url set fetched = ?, fetch_err = NULL, fetched_via = ? where url = ?",
          vec![datetime_to_rmpv(fetch_started), fetched_via, url.into()],
        )
        .await
        .unwrap();
      }
      Err(err) => {
        statsd
          .incr_with_tags("fetch_err")
          .with_tag("error", &err)
          .send();
        // Release lock on DashMap ASAP to avoid deadlocking across await.
        {
          let mut origin = origins.get_mut(origin).unwrap();
          if err == "connect"
            || err == "request"
            || err == "timeout"
            || err == "status:429"
            || err.starts_with("status:5")
          {
            origin.incr_failures();
          } else {
            origin.decr_failures();
          };
        };
        if err == "status:429" {
          // Don't update the DB row, we're not finished.
          // Don't instead create a new message, as that could cause exponential explosion if two workers polled the same message somehow.
          let delay = thread_rng().gen_range(0..60 * 15);
          queue
            .update_message(t.message(), Duration::from_secs(delay))
            .await
            .unwrap();
          continue;
        };
        // Do not overwrite or delete existing text/meta in the KV table if this crawl has failed.
        db.exec(
          "update url set fetched = ?, fetch_err = ?, fetched_via = NULL where url = ?",
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

  let origins = Arc::new(DashMap::new());
  let db = create_db_client();
  let queue = create_queue_client("hndr:crawl");
  let statsd = create_statsd("crawler");
  let client = reqwest::Client::builder()
    .connect_timeout(Duration::from_secs(20))
    .timeout(Duration::from_secs(60))
    .tcp_keepalive(None)
    .user_agent(std::env::var("USER_AGENT").unwrap_or_else(|_| "hndr".to_string()))
    .build()
    .unwrap();

  let workers = (0..CONTENT_CRAWL_CONCURRENCY)
    .map(|_| {
      spawn(worker_loop(
        origins.clone(),
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
