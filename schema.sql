create table cfg (
  k varchar(512) not null,
  v text not null,
  primary key (k)
);

create table hn_post (
  id bigint not null,

  deleted boolean not null default false,
  dead boolean not null default false,
  score int not null default 0,
  title text not null,
  text longtext not null,
  -- These can all be NULL for one reason or another.
  author varchar(100),
  ts datetime,
  parent bigint,
  url text,

  emb_dense_title longblob, -- Packed f32 little endian.
  emb_sparse_title longblob, -- MsgPack, Record<string, number>.
  emb_dense_text longblob, -- Packed f32 little endian.
  emb_sparse_text longblob, -- MsgPack, Record<string, number>.

  primary key (id)
);

create table hn_comment (
  id bigint not null,

  deleted boolean not null default false,
  dead boolean not null default false,
  score int not null default 0,
  text longtext not null,
  -- These can all be NULL for one reason or another.
  author varchar(100),
  ts datetime,
  post bigint,

  emb_dense_text longblob, -- Packed f32 little endian.
  emb_sparse_text longblob, -- MsgPack, Record<string, number>.

  primary key (id)
);
