use crate::common::KvRowsFetcher;
use common::mat::MatrixFile;
use db_rpc_client_rs::DbRpcDbClient;

pub async fn export_post_embs(db: DbRpcDbClient) {
  let Some(mut out) = MatrixFile::new("post_embs").await else {
    return;
  };

  let mut fetcher = KvRowsFetcher::new("post/%/emb");
  loop {
    let rows = fetcher.fetch_next(&db).await;
    if rows.is_empty() {
      break;
    };

    for r in rows {
      let id = r.extract_id();
      let emb_raw = r.v;
      out.push(id, &emb_raw).await;
    }
  }

  out.finish().await;
}
