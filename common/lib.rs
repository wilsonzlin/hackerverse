use serde::de::DeserializeOwned;
use serde::Serialize;

pub mod arrow;

#[derive(Clone)]
pub struct DbRpcClient {
  client: reqwest::Client,
}

#[derive(Serialize)]
struct DbRpcQuery {
  query: &'static str,
  params: Vec<rmpv::Value>,
}

#[derive(Serialize)]
struct DbRpcBatch {
  query: &'static str,
  params: Vec<Vec<rmpv::Value>>,
}

impl DbRpcClient {
  pub fn new() -> Self {
    Self {
      client: reqwest::Client::new(),
    }
  }

  pub async fn query<R: DeserializeOwned>(
    &self,
    query: &'static str,
    params: Vec<rmpv::Value>,
  ) -> Vec<R> {
    let raw = self
      .client
      .post("https://db-rpc.posh.wilsonl.in/db/hndr/query")
      .header("Authorization", std::env::var("DB_RPC_API_KEY").unwrap())
      .header("Content-Type", "application/msgpack")
      .body(rmp_serde::to_vec_named(&DbRpcQuery { query, params }).unwrap())
      .send()
      .await
      .unwrap()
      .error_for_status()
      .unwrap()
      .bytes()
      .await
      .unwrap();
    rmp_serde::from_slice(&raw).unwrap()
  }

  pub async fn batch(&self, query: &'static str, params: Vec<Vec<rmpv::Value>>) {
    self
      .client
      .post("https://db-rpc.posh.wilsonl.in/db/hndr/batch")
      .header("Authorization", std::env::var("DB_RPC_API_KEY").unwrap())
      .header("Content-Type", "application/msgpack")
      .body(rmp_serde::to_vec_named(&DbRpcBatch { query, params }).unwrap())
      .send()
      .await
      .unwrap()
      .error_for_status()
      .unwrap()
      .bytes()
      .await
      .unwrap();
  }
}
