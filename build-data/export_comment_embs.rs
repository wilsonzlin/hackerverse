use crate::common::EmbeddingRow;
use crate::common::KvRowsFetcher;
use crate::common::EMB_SCHEMA;
use common::arrow::ArrowIpcOutput;
use db_rpc_client_rs::DbRpcDbClient;

pub async fn export_comment_embs(db: DbRpcDbClient) {
  let Some(mut out_comment_embs) = ArrowIpcOutput::new(
    "comment_embs",
    EMB_SCHEMA.clone(),
    EmbeddingRow::to_columnar,
  ) else {
    return;
  };

  let mut fetcher = KvRowsFetcher::new("comment/%/emb");
  loop {
    let rows = fetcher.fetch_next(&db).await;
    if rows.is_empty() {
      break;
    };

    for r in rows {
      out_comment_embs.push(EmbeddingRow {
        id: r.extract_id(),
        emb: r.v.try_into().unwrap(),
      });
    }
  }

  out_comment_embs.finish();
}
