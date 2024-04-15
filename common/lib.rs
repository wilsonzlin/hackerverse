use cadence::QueuingMetricSink;
use cadence::StatsdClient;
use cadence::UdpMetricSink;
use db_rpc_client_rs::DbRpcClient;
use db_rpc_client_rs::DbRpcClientCfg;
use db_rpc_client_rs::DbRpcDbClient;
use queued_client_rs::QueuedClient;
use queued_client_rs::QueuedClientCfg;
use queued_client_rs::QueuedQueueClient;
use std::net::UdpSocket;
use std::sync::Arc;

pub mod arrow;
pub mod crawl;
pub mod parse;

pub fn create_db_client() -> DbRpcDbClient {
  let api_key = std::env::var("DB_RPC_API_KEY").unwrap();
  DbRpcClient::new(DbRpcClientCfg {
    api_key: Some(api_key),
    endpoint: "https://db-rpc.posh.wilsonl.in".to_string(),
  })
  .database("hndr")
}

pub fn create_queue_client(q: impl AsRef<str>) -> QueuedQueueClient {
  QueuedClient::new(QueuedClientCfg {
    api_key: Some(std::env::var("QUEUED_API_KEY").unwrap()),
    endpoint: "https://queued.posh.wilsonl.in".to_string(),
  })
  .queue(q.as_ref())
}

pub fn create_statsd(prefix: &'static str) -> Arc<StatsdClient> {
  let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
  socket.set_nonblocking(true).unwrap();
  let sink = UdpMetricSink::from(("127.0.0.1", 8125), socket).unwrap();
  let sink = QueuingMetricSink::from(sink);
  Arc::new(StatsdClient::from_sink(prefix, sink))
}
