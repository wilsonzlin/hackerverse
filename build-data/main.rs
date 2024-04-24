mod common;
mod export_comment_embs;
mod export_comment_sentiments;
mod export_comment_texts;
mod export_comments;
mod export_interactions;
mod export_post_embs;
mod export_post_embs_bgem3_dense;
mod export_post_texts;
mod export_post_titles;
mod export_posts;
mod export_url_metas;
mod export_url_texts;
mod export_urls;
mod export_users;

use ::common::create_db_client;
use dashmap::DashMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::join;

#[tokio::main]
async fn main() {
  let db = create_db_client();

  // Map from user ID => post IDs. We want post IDs to be sorted chronologically.
  let interactions = Arc::new(DashMap::<u32, BTreeSet<u32>>::new());

  join! {
    export_comment_embs::export_comment_embs(db.clone()),
    export_comment_sentiments::export_comment_sentiments(db.clone()),
    export_comment_texts::export_comment_texts(db.clone()),
    export_comments::export_comments(db.clone(), interactions.clone()),
    export_post_embs_bgem3_dense::export_post_embs_bgem3_dense(db.clone()),
    export_post_embs::export_post_embs(db.clone()),
    export_post_texts::export_post_texts(db.clone()),
    export_post_titles::export_post_titles(db.clone()),
    export_posts::export_posts(db.clone(), interactions.clone()),
    export_url_metas::export_url_metas(db.clone()),
    export_url_texts::export_url_texts(db.clone()),
    export_urls::export_urls(db.clone()),
    export_users::export_users(db.clone()),
  };

  export_interactions::export_interactions(&interactions);

  println!("all done!");
}
