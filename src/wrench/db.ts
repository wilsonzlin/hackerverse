import { VUnknown } from "@wzlin/valid";
import { Command } from "sacli";
import { db } from "../res";

const cli = Command.new("db");

cli
  .subcommand("query")
  .required("query", String, { default: true })
  .action(async (args) => {
    console.log(await db.query(args.query, [], new VUnknown()));
  });

cli.eval(process.argv.slice(2));
