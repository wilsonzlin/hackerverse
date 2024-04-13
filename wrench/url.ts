import { decode } from "@msgpack/msgpack";
import {
  VBytes,
  VDate,
  VInteger,
  VOptional,
  VString,
  VStruct,
  VUtf8Bytes,
} from "@wzlin/valid";
import mapExists from "@xtjs/lib/js/mapExists";
import { Command } from "sacli";
import { db } from "../common/res";

const cli = Command.new("url");

cli
  .subcommand("get")
  .required("url", String, { default: true })
  .action(async (args) => {
    const [row] = await db.query(
      "select * from url where url = ?",
      [args.url],
      new VStruct({
        id: new VInteger(0),
        url: new VUtf8Bytes(new VString()),
        proto: new VUtf8Bytes(new VString()),
        fetched: new VOptional(new VDate()),
        fetch_err: new VOptional(new VUtf8Bytes(new VString())),
      }),
    );
    if (!row) {
      throw new Error("URL not found");
    }
    const meta = await db
      .query(
        "select v from kv where k = ?",
        [`url/${row.id}/meta`],
        new VStruct({
          v: new VBytes(),
        }),
      )
      .then((r) => mapExists(r.at(0), (r) => decode(r.v) as any));
    const text = await db
      .query(
        "select v from kv where k = ?",
        [`url/${row.id}/text`],
        new VStruct({
          v: new VUtf8Bytes(new VString()),
        }),
      )
      .then((r) => r.at(0)?.v);
    console.table({ ...row, ...meta });
    console.log(text);
  });

cli.eval(process.argv.slice(2));
