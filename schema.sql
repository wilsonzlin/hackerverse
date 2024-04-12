create table cfg (
  k varchar(512) not null,
  v text not null,
  primary key (k)
);

create table kv (
  k varchar(512) not null,
  v longblob not null,
  primary key (k)
);

create table usr (
  id bigint not null auto_increment,
  username varchar(100) not null,
  primary key (id),
  unique (username)
);

-- Stored in KV:
-- - title
-- - text
-- - all embeddings and their inputs
create table post (
  id bigint not null,

  deleted boolean not null default false,
  dead boolean not null default false,
  score int not null default 0,
  -- These can all be NULL for one reason or another.
  author bigint,
  ts datetime,
  url text,

  page_fetched boolean not null default false,

  primary key (id)
);
create index post_author on post (author);
create index post_url on post (url);

-- Stored in KV:
-- - text
-- - all embeddings and their inputs
create table comment (
  id bigint not null,

  deleted boolean not null default false,
  dead boolean not null default false,
  score int not null default 0,
  parent bigint not null,
  -- These can all be NULL for one reason or another.
  author bigint,
  ts datetime,
  post bigint,

  primary key (id)
);
create index comment_parent on comment (parent);
create index comment_author on comment (author);
