use crate::common::KvRowsFetcher;
use crate::common::TextRow;
use crate::common::TEXT_SCHEMA;
use common::arrow::ArrowIpcOutput;
use db_rpc_client_rs::DbRpcDbClient;

pub async fn export_url_texts(db: DbRpcDbClient) {
  let Some(mut out_url_texts) =
    ArrowIpcOutput::new("url_texts", TEXT_SCHEMA.clone(), TextRow::to_columnar)
  else {
    return;
  };

  let mut fetcher = KvRowsFetcher::new("url/%/text");
  loop {
    let rows = fetcher.fetch_next(&db).await;
    if rows.is_empty() {
      break;
    };

    for r in rows {
      out_url_texts.push(TextRow {
        id: r.extract_id(),
        text: String::from_utf8(r.v).unwrap(),
      });
    }
  }

  out_url_texts.finish();
}
