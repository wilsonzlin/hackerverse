use ahash::AHashMap;
use common::DbRpcClient;
use itertools::Itertools;
use serde::Deserialize;

#[tokio::main]
async fn main() {
  let client = DbRpcClient::new();

  #[derive(Deserialize)]
  struct PostRow {
    id: u64,
  }
  let post_ids = client
    .query::<PostRow>("select id from hn_post", vec![])
    .await
    .into_iter()
    .map(|r| r.id)
    .collect_vec();
  println!("Fetched {} posts", post_ids.len());

  // WARNING: Sometimes comments will have IDs less than its parent (e.g. manual moderator re-rooting). Therefore, it's not enough to simply do one pass in `id` order.
  #[derive(Deserialize)]
  struct CommentRow {
    id: u64,
    parent: u64,
  }
  let comments = client
    .query::<CommentRow>("select id, parent from hn_comment", vec![])
    .await;
  let comment_count = comments.len();
  println!("Fetched {} comments", comment_count);
  // Map from parent (post or comment) ID => children (comment) IDs.
  let mut graph = AHashMap::<u64, Vec<u64>>::new();
  for c in comments {
    graph.entry(c.parent).or_default().push(c.id);
  }
  println!("Built graph of {} comments", comment_count);
  // Map from comment ID => post ID.
  let mut comment_post = AHashMap::<u64, u64>::new();
  fn visit_parent_for_post(
    out: &mut AHashMap<u64, u64>,
    graph: &mut AHashMap<u64, Vec<u64>>,
    post_id: u64,
    parent_id: u64,
  ) {
    for comment_id in graph.remove(&parent_id).unwrap_or_default() {
      assert!(out.insert(comment_id, post_id).is_none());
      visit_parent_for_post(out, graph, post_id, comment_id);
    }
  }
  for post_id in post_ids {
    visit_parent_for_post(&mut comment_post, &mut graph, post_id, post_id);
  }
  println!("Calculated post for {} comments", comment_count);
  // We're using MyRocks, avoid updating too many rows at once to avoid OOM.
  for batch_rows in comment_post.into_iter().chunks(100_000).into_iter() {
    let params = batch_rows
      .map(|(comment_id, post_id)| vec![post_id.into(), comment_id.into()])
      .collect_vec();
    let cnt = params.len();
    client
      .batch("update hn_comment set post = ? where id = ?", params)
      .await;
    println!("Updated {} comments", cnt);
  }

  println!("All done!");
}
