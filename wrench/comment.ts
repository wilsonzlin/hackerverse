import {
  VBoolean,
  VBytes,
  VDate,
  VInteger,
  VOptional,
  VString,
  VStruct,
  VUtf8Bytes,
} from "@wzlin/valid";
import decodeUtf8 from "@xtjs/lib/js/decodeUtf8";
import mapExists from "@xtjs/lib/js/mapExists";
import chalk from "chalk";
import { Command } from "sacli";
import { db } from "../common/res";

const cli = Command.new("comment");

cli
  .subcommand("list")
  .boolean("byScore")
  .optional("parent", Number)
  .optional("post", Number)
  .optional("limit", Number)
  .action(async (args) => {
    const res = await db.query(
      `
        select c.*, u.username as authorName
        from comment c
          left join usr u on u.id = c.author
        where true
          ${mapExists(args.parent, (p) => `and parent = ${p}`) ?? ""}
          ${mapExists(args.post, (p) => `and post = ${p}`) ?? ""}
        order by ${args.byScore ? "score" : "id"} desc
        limit ${args.limit ?? 10}
      `,
      [],
      new VStruct({
        id: new VInteger(1),
        score: new VInteger(),
        deleted: new VBoolean(),
        author: new VOptional(new VInteger()),
        ts: new VOptional(new VDate()),
        post: new VOptional(new VInteger()),
        parent: new VInteger(),
        dead: new VBoolean(),
        authorName: new VOptional(new VUtf8Bytes(new VString())),
      }),
    );
    console.table(res);
  });

cli
  .subcommand("show")
  .required("id", String, { default: true })
  .action(async (args) => {
    const [meta] = await db.query(
      `
        select c.*, u.username as authorName
        from comment c
          left join usr u on u.id = c.author
        where c.id = ${args.id}
      `,
      [],
      new VStruct({
        id: new VInteger(1),
        score: new VInteger(),
        deleted: new VBoolean(),
        author: new VOptional(new VInteger()),
        ts: new VOptional(new VDate()),
        post: new VOptional(new VInteger()),
        parent: new VInteger(),
        dead: new VBoolean(),
        authorName: new VOptional(new VUtf8Bytes(new VString())),
      }),
    );
    for (const [k, v] of Object.entries(meta)) {
      console.log(chalk.bold(k));
      console.log("  ", v);
    }
    const kvs = await db.query(
      `select k, v from kv where k like 'comment/${args.id}/%' order by k`,
      [],
      new VStruct({
        k: new VUtf8Bytes(new VString()),
        v: new VBytes(),
      }),
    );
    for (const { k, v } of kvs) {
      console.log(chalk.bold(k));
      console.log("  ", decodeUtf8(v));
    }
  });

cli.eval(process.argv.slice(2));
