use ahash::AHashMap;
use axum::extract::Path;
use axum::extract::State;
use axum::routing::get;
use axum::Router;
use axum_msgpack::MsgPack;
use base64::prelude::*;
use futures::TryFutureExt;
use itertools::Itertools;
use reqwest::header::ETAG;
use reqwest::header::IF_NONE_MATCH;
use reqwest::StatusCode;
use rmpv::Value;
use serde::Deserialize;
use serde::Serialize;
use service_toolkit::panic::set_up_panic_hook;
use service_toolkit::server::build_port_server_with_tls;
use service_toolkit::server::TlsCfg;
use std::env::var;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

#[derive(Clone, Serialize)]
#[serde(untagged)]
enum Data {
  Map(AHashMap<String, Box<Data>>),
  Value(Value),
}

struct Ctx {
  data: parking_lot::RwLock<Box<Data>>,
}

async fn endpoint(
  State(ctx): State<Arc<Ctx>>,
  Path(path_raw): Path<String>,
) -> Result<MsgPack<Data>, axum::http::StatusCode> {
  let path = path_raw.split('/').collect_vec();
  let data = ctx.data.read();
  let mut cur = &**data;
  for p in path {
    let Data::Map(m) = cur else {
      return Err(axum::http::StatusCode::NOT_FOUND);
    };
    cur = m.get(p).ok_or(axum::http::StatusCode::NOT_FOUND)?;
  }
  Ok(MsgPack(cur.clone()))
}

#[tokio::main]
async fn main() {
  set_up_panic_hook();

  tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .json()
    .init();

  let ctx = Arc::new(Ctx {
    data: parking_lot::RwLock::new(Box::new(Data::Map(AHashMap::new()))),
  });

  spawn({
    let ctx = ctx.clone();
    let mut etag = String::new();
    let client = reqwest::Client::new();
    async move {
      loop {
        match client
          .get("https://static.wilsonl.in/hndr/data/edge.msgpack")
          .header(IF_NONE_MATCH, &etag)
          .send()
          .and_then(|res| async { res.error_for_status() })
          .and_then(|res| async {
            Ok((
              res.status(),
              res
                .headers()
                .get(ETAG)
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
              res.bytes().await?,
            ))
          })
          .await
        {
          Ok((StatusCode::NOT_MODIFIED, _, _)) => {}
          Ok((new_etag, raw)) => {
            etag = new_etag.clone();
            let v: Value = rmp_serde::from_slice(&raw).unwrap();
            // This is faster than deserializing to the Data enum with serde(untagged).
            fn to_data(v: Value) -> Box<Data> {
              if let Value::Map(m) = v {
                let mut map = AHashMap::<String, Box<Data>>::new();
                for (k, v) in m {
                  map.insert(k.to_string(), to_data(v));
                }
                Box::new(Data::Map(map))
              } else {
                Box::new(Data::Value(v))
              }
            }
            *ctx.data.write() = to_data(v);
            tracing::info!(new_etag, "updated data");
          }
          Err(e) => {
            tracing::error!(error = e.to_string(), "failed to fetch data");
          }
        };
        sleep(Duration::from_secs(30)).await;
      }
    }
  });

  let app = Router::new()
    .route("/healthz", get(|| async { "OK" }))
    .route("/data/*path", get(endpoint))
    .with_state(ctx.clone());

  tracing::info!("server started");
  build_port_server_with_tls(
    "0.0.0.0".parse().unwrap(),
    var("PORT").unwrap().parse().unwrap(),
    &TlsCfg {
      ca: Some(
        BASE64_STANDARD
          .decode(var("EDGE_SSL_CA_BASE64").unwrap())
          .unwrap(),
      ),
      cert: BASE64_STANDARD
        .decode(var("EDGE_SSL_CERT_BASE64").unwrap())
        .unwrap(),
      key: BASE64_STANDARD
        .decode(var("EDGE_SSL_KEY_BASE64").unwrap())
        .unwrap(),
    },
  )
  .serve(app.into_make_service())
  .await
  .unwrap();
}
