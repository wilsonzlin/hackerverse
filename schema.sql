create table cfg (
  k text not null,
  v text not null,
  primary key (k)
);

create table hn_post (
  id bigint not null,

  deleted boolean not null default false,
  dead boolean not null default false,
  score int not null default 0,
  title text not null, -- HTML
  text text not null, -- HTML
  -- These can all be NULL for one reason or another.
  author text,
  ts timestamp,
  parent bigint,
  url text,

  primary key (id)
);

create table hn_comment (
  id bigint not null,

  deleted boolean not null default false,
  dead boolean not null default false,
  score int not null default 0,
  text text not null, -- HTML
  -- These can all be NULL for one reason or another.
  author text,
  ts timestamp,
  post bigint,

  primary key (id)
);
