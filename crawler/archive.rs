use cadence::Counted;
use cadence::StatsdClient;
use cadence::Timed;
use chrono::DateTime;
use chrono::Utc;
use common::crawl::check_if_already_crawled;
use common::crawl::get_content_type;
use common::crawl::is_valid_content_type;
use common::crawl::process_crawl;
use common::crawl::reqwest_error_to_code;
use common::crawl::CrawlTask;
use common::crawl::ProcessCrawlArgs;
use db_rpc_client_rs::DbRpcDbClient;
use futures::TryFutureExt;
use queued_client_rs::QueuedQueueClient;
use rand::thread_rng;
use rand::Rng;
use reqwest::Client;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use std::cmp::max;
use std::cmp::min;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio::time::Instant;

// Return value:
// - Err(reqwest::Error): something went wrong when interacting with the Internet Archive, either the API or the archived content. Make sure to check we're not being rate limited or the server is down, so as to not continue retrying excessively.
// - Ok(None): no archived content is available, or it isn't HTML.
// - Ok(Some(String)): the archived HTML.
async fn try_internet_archive(
  statsd: &Arc<StatsdClient>,
  client: &Client,
  url: impl AsRef<str>,
) -> reqwest::Result<Option<String>> {
  #[serde_as]
  #[derive(Deserialize)]
  #[allow(unused)]
  struct AvailableArchivedSnapshotsClosest {
    #[serde_as(as = "DisplayFromStr")]
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
  tracing::debug!(
    url = url.as_ref(),
    "checking for Internet Archive availability"
  );
  let a_started = Instant::now();
  let a = client
    .get("https://archive.org/wayback/available")
    .query(&[("url", url.as_ref())])
    .send()
    .and_then(|res| async { res.error_for_status() })
    .and_then(|res| res.text())
    .await;
  let a_dur = a_started.elapsed();
  let a_err = a
    .as_ref()
    .err()
    .map(reqwest_error_to_code)
    .unwrap_or_else(|| "none".to_string());
  statsd
    .time_with_tags("internet_archive_available_api_call_ms", a_dur)
    .with_tag("error", &a_err)
    .send();
  let a = a?;
  let Some(c) = serde_json::from_str::<Available>(&a)
    .expect(&format!("failed to parse Available response: {}", a))
    .archived_snapshots
    .closest
    .filter(|c| c.available && c.status >= 200 && c.status <= 299)
  else {
    return Ok(None);
  };
  tracing::debug!(
    url = url.as_ref(),
    err = a_err,
    ms = a_dur.as_millis(),
    available = c.available,
    status = c.status,
    ts = c.timestamp,
    "received Internet Archive availability response, fetching"
  );
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
  let res_dur = res_started.elapsed();
  let res_err = res
    .as_ref()
    .err()
    .map(reqwest_error_to_code)
    .unwrap_or_else(|| "none".to_string());
  tracing::debug!(
    url = url.as_ref(),
    err = res_err,
    ms = res_dur.as_millis(),
    "fetched from Internet Archive"
  );
  statsd
    .time_with_tags("internet_archive_fetch_ms", res_dur)
    .with_tag("error", &res_err)
    .send();
  let (ct, res) = res?;
  if !is_valid_content_type(ct.as_ref()) {
    return Ok(None);
  };
  Ok(Some(res))
}

#[derive(Default)]
pub struct RateLimiter {
  count: i64,
  until: DateTime<Utc>,
}

impl RateLimiter {
  pub fn incr(&mut self) {
    self.count = min(8, self.count + 1);
    self.until = Utc::now()
      + chrono::Duration::milliseconds(thread_rng().gen_range(0..((1 << self.count) * 1000)));
  }

  pub fn decr(&mut self) {
    self.count = max(0, self.count - 1);
  }

  pub async fn sleep_until_ok(&mut self) -> &mut Self {
    if let Ok(diff) = (self.until - Utc::now()).to_std() {
      tracing::debug!(
        hits = self.count,
        ms = diff.as_millis(),
        "sleeping due to rate limit hits"
      );
      sleep(diff).await;
    };
    self
  }
}

pub(crate) async fn archive_worker_loop(
  client: Client,
  db: DbRpcDbClient,
  queue: QueuedQueueClient,
  statsd: Arc<StatsdClient>,
) {
  let mut rate_limiter = RateLimiter::default();
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

    // If we eventually managed to crawl it directly from the original site in the end, then we can skip this.
    if check_if_already_crawled(&db, id).await {
      statsd.count("skipped", 1).unwrap();
    } else {
      let url_with_proto = format!("{}//{}", proto, url);
      let fetch_started = Utc::now();
      rate_limiter.sleep_until_ok().await;
      let res = try_internet_archive(&statsd, &client, url_with_proto).await;
      match res {
        Ok(html) => {
          let found_in_archive = html.is_some();
          if let Some(html) = html {
            statsd.count("fetch_bytes", html.len() as u64).unwrap();
            process_crawl(ProcessCrawlArgs {
              db: &db,
              fetch_started,
              fetched_via: Some("internet_archive"),
              html,
              url_id: id,
              url: &url,
            })
            .await;
          };
          db.exec("update url set found_in_archive = ? where url = ?", vec![
            found_in_archive.into(),
            url.into(),
          ])
          .await
          .unwrap();
        }
        Err(err) => {
          if err.is_connect()
            || err.is_timeout()
            || err.is_request()
            || err.is_decode()
            || err
              .status()
              .is_some_and(|s| s.is_server_error() || s == StatusCode::TOO_MANY_REQUESTS)
          {
            rate_limiter.incr();
          };
          // Do not update row, fetching from the IA is just an optional bonus if it exists.
          // Don't delete queue message or decrement rate limit hit count.
          continue;
        }
      };
      rate_limiter.decr();
    };
    queue.delete_messages([t.message()]).await.unwrap();
  }
}
