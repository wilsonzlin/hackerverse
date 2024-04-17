use crate::origin::Origin;
use cadence::Counted;
use cadence::CountedExt;
use cadence::StatsdClient;
use cadence::Timed;
use chrono::Utc;
use common::crawl::check_if_already_crawled;
use common::crawl::datetime_to_rmpv;
use common::crawl::get_content_type;
use common::crawl::is_valid_content_type;
use common::crawl::process_crawl;
use common::crawl::reqwest_error_to_code;
use common::crawl::CrawlTask;
use common::crawl::ProcessCrawlArgs;
use dashmap::DashMap;
use db_rpc_client_rs::DbRpcDbClient;
use queued_client_rs::QueuedQueueClient;
use rand::thread_rng;
use rand::Rng;
use reqwest::header::ACCEPT;
use reqwest::header::ACCEPT_LANGUAGE;
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

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

pub(crate) async fn direct_worker_loop(
  origins: Arc<DashMap<String, Origin>>,
  client: Client,
  db: DbRpcDbClient,
  queue: QueuedQueueClient,
  statsd: Arc<StatsdClient>,
) {
  loop {
    let timeout = thread_rng().gen_range(60 * 4..60 * 6);
    let Some(t) = queue
      .poll_messages(1, Duration::from_secs(timeout))
      .await
      .unwrap()
      .messages
      .pop()
    else {
      sleep(Duration::from_secs(3)).await;
      continue;
    };
    let CrawlTask { id, proto, url } = rmp_serde::from_slice(&t.contents).unwrap();

    if check_if_already_crawled(&db, id).await {
      // We've already fetched, either by another crawl task or by a crawl_archive task.
      statsd.count("skipped", 1).unwrap();
    } else {
      let origin = url.split_once('/').unwrap().0;
      if !origins.entry(origin.to_string()).or_default().can_request() {
        statsd
          .incr_with_tags("fetch_err")
          .with_tag("error", "rate_limit")
          .send();
        // Don't delete queue message. Let current timeout delay its processing.
        continue;
      }

      let fetch_started = Utc::now();
      let res = make_request(&client, format!("{}//{}", proto, url)).await;

      let elapsed = (Utc::now() - fetch_started).num_milliseconds().max(0) as u64;
      statsd
        .time_with_tags("fetch_ms", elapsed)
        .with_tag("result", match res.as_ref().map_err(|v| v.as_str()) {
          Ok(_) => "ok",
          Err("timeout") => "timeout",
          Err(_) => "error",
        })
        .send();

      match res {
        Ok(html) => {
          statsd.count("fetch_bytes", html.len() as u64).unwrap();
          process_crawl(ProcessCrawlArgs {
            db: db.clone(),
            fetch_started,
            fetched_via: None,
            html,
            url_id: id,
            url: url.clone(),
          })
          .await;
        }
        Err(err) => {
          statsd
            .incr_with_tags("fetch_err")
            .with_tag("error", &err)
            .send();

          // Do not overwrite or delete existing text/meta in the KV table if this crawl has failed.
          db.exec(
            "update url set fetched = ?, fetch_err = ?, fetched_via = NULL where url = ?",
            vec![
              datetime_to_rmpv(fetch_started),
              err.as_str().into(),
              url.as_str().into(),
            ],
          )
          .await
          .unwrap();

          // NOTE: We cannot enqueue here to crawl_archive queue, as we'll keep enqueuing it there otherwise.

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
            // Don't instead create a new message, as that could cause exponential explosion if two workers polled the same message somehow.
            // Don't delete queue message. Let current timeout delay its processing.
            continue;
          };
        }
      };
    };
    queue.delete_messages([t.message()]).await.unwrap();
  }
}
