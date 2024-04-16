use cadence::Counted;
use cadence::StatsdClient;
use cadence::Timed;
use chrono::DateTime;
use chrono::Utc;
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
use reqwest::header::ACCEPT;
use reqwest::header::ACCEPT_ENCODING;
use reqwest::header::ACCEPT_LANGUAGE;
use reqwest::header::USER_AGENT;
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
// - Ok(None): no archived content is available.
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
  tracing::info!(
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
  tracing::info!(
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
  tracing::info!(
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

async fn try_archive_today(
  statsd: &Arc<StatsdClient>,
  client: &Client,
  url: impl AsRef<str>,
) -> reqwest::Result<Option<String>> {
  tracing::info!(url = url.as_ref(), "fetching from archive.today");
  let res_started = Instant::now();
  let res = client
    .get(format!("https://archive.is/latest/{}", url.as_ref()))
    .header(
      USER_AGENT,
      std::env::var("ARCHIVE_TODAY_USER_AGENT").unwrap(),
    )
    .header(
      ACCEPT,
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    )
    .header(ACCEPT_ENCODING, "gzip, deflate, br")
    .header(ACCEPT_LANGUAGE, "en")
    .send()
    .and_then(|res| async { res.error_for_status() })
    .and_then(|res| async {
      let ct = get_content_type(&res);
      res.text().await.map(|text| (ct, text))
    })
    .await;
  let fetch_dur = res_started.elapsed();
  let fetch_err = res
    .as_ref()
    .err()
    .map(reqwest_error_to_code)
    .unwrap_or_else(|| "none".to_string());
  tracing::info!(
    url = url.as_ref(),
    err = fetch_err,
    ms = fetch_dur.as_millis(),
    "fetched from archive.today"
  );
  statsd
    .time_with_tags("archive_today_fetch_ms", fetch_dur)
    .with_tag("error", &fetch_err)
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
      tracing::info!(
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
  let mut rate_limiter_at = RateLimiter::default();
  let mut rate_limiter_ia = RateLimiter::default();
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

    #[derive(Deserialize)]
    struct Row {
      fetch_err: Option<String>,
    }
    let existing: Option<Row> = db
      .query(
        "select fetch_err from url where id = ? and fetched is not null",
        vec![id.into()],
      )
      .await
      .unwrap()
      .pop();
    // If we eventually managed to crawl it directly from the original site in the end, then we can skip this.
    let already_successful = existing.is_some_and(|e| e.fetch_err.is_none());
    if already_successful {
      statsd.count("skipped", 1).unwrap();
    } else {
      let url_with_proto = format!("{}//{}", proto, url);
      let fetch_started = Utc::now();
      let (typ, rl) = [
        ("archive_today", &mut rate_limiter_at),
        ("internet_archive", &mut rate_limiter_ia),
      ]
      .into_iter()
      .min_by_key(|m| m.1.until)
      .unwrap();
      rl.sleep_until_ok().await;
      let res = match typ {
        "archive_today" => try_archive_today(&statsd, &client, url_with_proto).await,
        "internet_archive" => try_internet_archive(&statsd, &client, url_with_proto).await,
        _ => unreachable!(),
      };
      match res {
        Ok(Some(html)) => {
          statsd.count("fetch_bytes", html.len() as u64).unwrap();
          process_crawl(ProcessCrawlArgs {
            db: db.clone(),
            fetch_started,
            fetched_via: Some(typ),
            html,
            url,
            url_id: id,
          })
          .await;
        }
        Err(err)
          if err.is_connect()
            || err.is_timeout()
            || err.is_request()
            || err.is_decode()
            || err
              .status()
              .is_some_and(|s| s.is_server_error() || s == StatusCode::TOO_MANY_REQUESTS) =>
        {
          rl.incr();
          // Don't delete queue message or decrement rate limit hit count.
          continue;
        }
        // Errors are already tracked (via StatsD) in the try_internet_archive function, so we don't need to handle them.
        _ => {}
      };
      rl.decr();
    };
    queue.delete_messages([t.message()]).await.unwrap();
  }
}
