use ahash::AHashMap;
use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::State;
use axum::routing::get;
use axum::routing::post;
use axum::Router;
use axum_msgpack::MsgPack;
use futures::TryFutureExt;
use reqwest::header::ETAG;
use reqwest::header::IF_NONE_MATCH;
use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use serde_bytes::ByteBuf;
use service_toolkit::panic::set_up_panic_hook;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;
use tower_http::cors::CorsLayer;
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

#[derive(Clone, Deserialize, Serialize)]
struct Point {
  x: f32,
  y: f32,
}

#[derive(Deserialize, Serialize)]
struct Map {
  points: AHashMap<u32, Point>,
  meta: MapMeta,
  // One for each LOD level.
  tiles: Vec<AHashMap<String, ByteBuf>>,
  terrain: AHashMap<String, ByteBuf>,
}

#[derive(Clone, Deserialize, Serialize)]
struct Post {
  author: String,
  ts: i64, // UNIX epoch seconds.
  title: String,
  url: String,            // Empty string if NULL.
  proto: String,          // Empty string if NULL.
  found_in_archive: bool, // False if NULL.
}

#[derive(Default, Deserialize)]
struct Data {
  maps: AHashMap<String, Map>,
  posts: AHashMap<u32, Post>,
}

struct Ctx {
  data: parking_lot::RwLock<Data>,
}

async fn get_map_point(
  State(ctx): State<Arc<Ctx>>,
  Path((variant, id)): Path<(String, u32)>,
) -> Result<MsgPack<Point>, axum::http::StatusCode> {
  let data = ctx.data.read();
  let Some(map) = data.maps.get(&variant) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  let Some(point) = map.points.get(&id) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  Ok(MsgPack(point.clone()))
}

async fn get_map_meta(
  State(ctx): State<Arc<Ctx>>,
  Path(variant): Path<String>,
) -> Result<MsgPack<MapMeta>, axum::http::StatusCode> {
  let data = ctx.data.read();
  let Some(map) = data.maps.get(&variant) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  let meta = map.meta.clone();
  Ok(MsgPack(meta))
}

async fn get_map_tile(
  State(ctx): State<Arc<Ctx>>,
  Path((variant, lod, tile_id)): Path<(String, u8, String)>,
) -> Result<Vec<u8>, axum::http::StatusCode> {
  let data = ctx.data.read();
  let Some(map) = data.maps.get(&variant) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  let Some(lod) = map.tiles.get(lod as usize) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  let Some(tile) = lod.get(&tile_id) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  Ok(tile.to_vec())
}

async fn get_map_terrain(
  State(ctx): State<Arc<Ctx>>,
  Path((variant, typ)): Path<(String, String)>,
) -> Result<Vec<u8>, axum::http::StatusCode> {
  let data = ctx.data.read();
  let Some(map) = data.maps.get(&variant) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  let Some(img) = map.terrain.get(&typ) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  Ok(img.to_vec())
}

async fn get_post(
  State(ctx): State<Arc<Ctx>>,
  Path(post_id): Path<u32>,
) -> Result<MsgPack<Post>, axum::http::StatusCode> {
  let data = ctx.data.read();
  let Some(post) = data.posts.get(&post_id) else {
    return Err(axum::http::StatusCode::NOT_FOUND);
  };
  Ok(MsgPack(post.clone()))
}

async fn get_post_title_lengths(
  State(ctx): State<Arc<Ctx>>,
  body: Bytes,
) -> Result<Vec<u8>, axum::http::StatusCode> {
  if body.len() % 4 != 0 {
    return Err(axum::http::StatusCode::BAD_REQUEST);
  };
  let data = ctx.data.read();
  let mut out = Vec::new();
  for id_raw in body.chunks(4) {
    let id = u32::from_le_bytes(id_raw.try_into().unwrap());
    out.push(
      data
        .posts
        .get(&id)
        .map(|post| u8::try_from(post.title.len()).unwrap())
        .unwrap_or_default(),
    );
  }
  Ok(out)
}

async fn get_post_titles(
  State(ctx): State<Arc<Ctx>>,
  body: Bytes,
) -> Result<MsgPack<Vec<String>>, axum::http::StatusCode> {
  if body.len() % 4 != 0 {
    return Err(axum::http::StatusCode::BAD_REQUEST);
  };
  let data = ctx.data.read();
  let mut out = Vec::new();
  for id_raw in body.chunks(4) {
    let id = u32::from_le_bytes(id_raw.try_into().unwrap());
    out.push(
      data
        .posts
        .get(&id)
        .map(|post| post.title.clone())
        .unwrap_or_default(),
    );
  }
  Ok(MsgPack(out))
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

  let cors = CorsLayer::new()
    .allow_methods(tower_http::cors::Any)
    .allow_origin(tower_http::cors::Any)
    .allow_headers(tower_http::cors::Any);

  let app = Router::new()
    .route("/healthz", get(|| async { "OK" }))
    .route("/map/:map/meta", get(get_map_meta))
    .route("/map/:map/point/:id", get(get_map_point))
    .route("/map/:map/terrain/:typ", get(get_map_terrain))
    .route("/map/:map/tile/:lod/:tile_id", get(get_map_tile))
    .route("/post-title-lengths", post(get_post_title_lengths))
    .route("/post-titles", post(get_post_titles))
    .route("/post/:id", get(get_post))
    .layer(cors)
    .with_state(ctx.clone());

  let listener = tokio::net::TcpListener::bind("127.0.0.1:8000")
    .await
    .unwrap();
  tracing::info!("server started");
  axum::serve(listener, app).await.unwrap();
}
