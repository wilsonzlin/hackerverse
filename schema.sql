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
-- - meta
-- - text
create table url (
  id bigint not null auto_increment,
  -- Without protocol. We use varbinary as an indexed column can only have around ~3000 bytes, and since we use utf8mb4, the limit is too low (~750) for URLs. We can't just use ascii charset as URLs can contain non-ASCII.
  url varbinary(3000) not null,
  proto varchar(20) not null,
  fetched datetime,
  fetch_err varchar(128),
  fetched_via varchar(128), -- We may have fetched this page via an alternative source, like the Internet Archive.
  found_in_archive boolean, -- NULL means we haven't checked yet, and is different to `false`.
  primary key (id),
  unique (url)
);

create index url_fetch_err on url (fetch_err);

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
  url bigint,

  emb_missing_page boolean not null default false, -- The embedding for this URL (i.e. not text-based) post was generated without the crawled page as part of the input, likely because the crawl was missing/failed at the time.

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
