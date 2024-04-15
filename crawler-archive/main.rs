use cadence::Counted;
use cadence::StatsdClient;
use cadence::Timed;
use chrono::Utc;
use common::crawl::get_content_type;
use common::crawl::is_valid_content_type;
use common::crawl::process_crawl;
use common::crawl::reqwest_error_to_code;
use common::crawl::ProcessCrawlArgs;
use common::create_db_client;
use common::create_queue_client;
use common::create_statsd;
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
use service_toolkit::panic::set_up_panic_hook;
use std::cmp::max;
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
  let a_started = Instant::now();
  let a = client
    .get("https://archive.org/wayback/available")
    .query(&[("url", url.as_ref())])
    .send()
    .and_then(|res| async { res.error_for_status() })
    .and_then(|res| res.text())
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
        .unwrap_or_else(|| "none".to_string()),
    )
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
        .unwrap_or_else(|| "none".to_string()),
    )
    .send();
  let (ct, res) = res?;
  if !is_valid_content_type(ct.as_ref()) {
    return Ok(None);
  };
  Ok(Some(res))
}

#[derive(Deserialize)]
struct CrawlTask {
  id: u64,
  proto: String,
  url: String,
}

async fn worker_loop(
  client: Client,
  db: DbRpcDbClient,
  queue: QueuedQueueClient,
  statsd: Arc<StatsdClient>,
) {
  let mut rate_limits = 0u64;
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
      let fetch_started = Utc::now();
      match try_internet_archive(&statsd, &client, format!("{}//{}", proto, url)).await {
        Ok(Some(html)) => {
          statsd.count("fetch_bytes", html.len() as u64).unwrap();
          process_crawl(ProcessCrawlArgs {
            db: db.clone(),
            fetch_started,
            fetched_via: Some("internet_archive"),
            html,
            url,
            url_id: id,
          })
          .await;
        }
        Err(err)
          if err
            .status()
            .is_some_and(|s| s.is_server_error() || s == StatusCode::TOO_MANY_REQUESTS) =>
        {
          rate_limits = max(8, rate_limits + 1);
          let delay_ms = thread_rng().gen_range(0..((1 << rate_limits) * 1000));
          sleep(std::time::Duration::from_millis(delay_ms)).await;
          // Don't delete queue message or decrement rate limit hit count.
          continue;
        }
        // Errors are already tracked (via StatsD) in the try_internet_archive function, so we don't need to handle them.
        _ => {}
      };
      rate_limits = rate_limits.saturating_sub(1);
    };
    queue.delete_messages([t.message()]).await.unwrap();
  }
}

#[tokio::main]
async fn main() {
  set_up_panic_hook();

  let db = create_db_client();
  let queue = create_queue_client("hndr:crawl_archive");
  let statsd = create_statsd("crawler_archive");
  let client = reqwest::Client::builder()
    .connect_timeout(Duration::from_secs(20))
    .timeout(Duration::from_secs(60))
    .tcp_keepalive(None)
    .user_agent(std::env::var("USER_AGENT").unwrap_or_else(|_| "hndr".to_string()))
    .build()
    .unwrap();

  // IA has very low rate limits, one worker is enough.
  worker_loop(client.clone(), db.clone(), queue.clone(), statsd.clone()).await;
  println!("All done!");
}
