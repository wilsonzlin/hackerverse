mod archive;
mod direct;
mod origin;

use crate::archive::archive_worker_loop;
use crate::direct::direct_worker_loop;
use common::create_db_client;
use common::create_queue_client;
use common::create_statsd;
use dashmap::DashMap;
use itertools::Itertools;
use service_toolkit::panic::set_up_panic_hook;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::System;
use tokio::spawn;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
  set_up_panic_hook();

  tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .json()
    .init();

  let origins = Arc::new(DashMap::new());
  let db = create_db_client();
  let queue_direct = create_queue_client("hndr:crawl");
  let queue_archive = create_queue_client("hndr:crawl_archive");
  let statsd_direct = create_statsd("crawler");
  let statsd_archive = create_statsd("crawler_archive");
  let client = reqwest::Client::builder()
    .connect_timeout(Duration::from_secs(20))
    .timeout(Duration::from_secs(60))
    .tcp_keepalive(None)
    .user_agent(std::env::var("USER_AGENT").unwrap_or_else(|_| "hndr".to_string()))
    .build()
    .unwrap();

  let mut sys = System::new_all();
  sys.refresh_all();
  // Target around 256 concurrency on a 4 GiB RAM machine.
  let direct_worker_count = sys.total_memory() / 1024 / 1024 / 16;
  let direct_workers = (0..direct_worker_count)
    .map(|_| {
      spawn(direct_worker_loop(
        origins.clone(),
        client.clone(),
        db.clone(),
        queue_direct.clone(),
        statsd_direct.clone(),
      ))
    })
    .collect_vec();

  // Internet Archive, archive.today have very low rate limits, so 2 workers is more than enough.
  let archive_workers = (0..2)
    .map(|_| {
      spawn(archive_worker_loop(
        client.clone(),
        db.clone(),
        queue_archive.clone(),
        statsd_archive.clone(),
      ))
    })
    .collect_vec();

  for w in direct_workers {
    w.await.unwrap();
  }
  for w in archive_workers {
    w.await.unwrap();
  }
  println!("All done!");
}
