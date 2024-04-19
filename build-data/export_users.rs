use arrow::array::ArrayRef;
use arrow::array::StringArray;
use arrow::array::UInt32Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use common::arrow::ArrowIpcOutput;
use db_rpc_client_rs::DbRpcDbClient;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
struct UserRow {
  id: u32,
  username: String,
}

impl UserRow {
  #[rustfmt::skip]
  pub fn to_columnar(users: Vec<UserRow>) -> Vec<ArrayRef> {
    let mut ids = Vec::new();
    let mut usernames = Vec::new();
    for user in users {
      ids.push(user.id);
      usernames.push(user.username);
    };
    vec![
      Arc::new(UInt32Array::from(ids)),
      Arc::new(StringArray::from(usernames)),
    ]
  }
}

pub async fn export_users(db: DbRpcDbClient) {
  let user_schema = Schema::new(vec![
    Field::new("id", DataType::UInt32, false),
    Field::new("username", DataType::Utf8, false),
  ]);

  let Some(mut out_users) = ArrowIpcOutput::new("users", user_schema, UserRow::to_columnar) else {
    return;
  };

  let mut next_usr_id = 0;
  loop {
    let rows = db
      .query::<UserRow>(
        r#"
          select *
          from usr
          where id >= ?
          order by id
          limit 10000000
        "#,
        vec![next_usr_id.into()],
      )
      .await
      .unwrap();
    let n = rows.len();
    if n == 0 {
      println!("end of users");
      break;
    };
    println!("fetch {} users from ID {}", n, next_usr_id);
    next_usr_id = rows.last().unwrap().id + 1;

    for r in rows {
      out_users.push(r);
    }
  }

  out_users.finish();
}
