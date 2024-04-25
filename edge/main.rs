use ahash::AHashMap;
use axum::extract::Path;
use axum::extract::State;
use axum::routing::get;
use axum::Router;
use axum_msgpack::MsgPack;
use base64::prelude::*;
use futures::TryFutureExt;
use reqwest::header::ETAG;
use reqwest::header::IF_NONE_MATCH;
use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use serde_bytes::ByteBuf;
use service_toolkit::panic::set_up_panic_hook;
use service_toolkit::server::build_port_server;
use service_toolkit::server::TlsCfg;
use std::env::var;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

#[derive(Clone, Deserialize, Serialize)]
struct MapMeta {
  x_min: f32,
  x_max: f32,
  y_min: f32,
  y_max: f32,
  score_min: i16,
  score_max: i16,
  count: u32,
  lod_levels: u8,
}

#[derive(Deserialize, Serialize)]
struct Map {
  meta: MapMeta,
  // One for each LOD level.
  tiles: Vec<AHashMap<String, ByteBuf>>,
}

#[derive(Clone, Deserialize, Serialize)]
struct Point {
  x: f32,
  y: f32,
}

#[derive(Deserialize, Serialize)]
struct Variant {
  umap: AHashMap<u32, Point>,
  map: Map,
}

struct Ctx {
  data: parking_lot::RwLock<AHashMap<String, Variant>>,
}

async fn get_umap_point(
  State(ctx): State<Arc<Ctx>>,
  Path((variant, id)): Path<(String, u32)>,
) -> Result<MsgPack<Point>, axum::http::StatusCode> {
  let data = ctx.data.read();
  let Some(variant) = data.get(&variant) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  let Some(point) = variant.umap.get(&id) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  Ok(MsgPack(point.clone()))
}

async fn get_map_meta(
  State(ctx): State<Arc<Ctx>>,
  Path(variant): Path<String>,
) -> Result<MsgPack<MapMeta>, axum::http::StatusCode> {
  let data = ctx.data.read();
  let Some(variant) = data.get(&variant) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  let meta = variant.map.meta.clone();
  Ok(MsgPack(meta))
}

async fn get_map_tile(
  State(ctx): State<Arc<Ctx>>,
  Path((variant, lod, tile_id)): Path<(String, u8, String)>,
) -> Result<Vec<u8>, axum::http::StatusCode> {
  let data = ctx.data.read();
  let Some(variant) = data.get(&variant) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  let Some(lod) = variant.map.tiles.get(lod as usize) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  let Some(tile) = lod.get(&tile_id) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  Ok(tile.to_vec())
}

#[tokio::main]
async fn main() {
  set_up_panic_hook();

  tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .json()
    .init();

  let ctx = Arc::new(Ctx {
    data: Default::default(),
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
          Ok((_, new_etag, raw)) => {
            etag = new_etag.clone();
            *ctx.data.write() = rmp_serde::from_slice(&raw).unwrap();
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
    .route("/:variant/umap/:id", get(get_umap_point))
    .route("/:variant/map/meta", get(get_map_meta))
    .route("/:variant/map/:lod/:tile_id", get(get_map_tile))
    .with_state(ctx.clone());

  tracing::info!("server started");
  build_port_server("127.0.0.1".parse().unwrap(), 8000)
    .serve(app.into_make_service())
    .await
    .unwrap();
}
