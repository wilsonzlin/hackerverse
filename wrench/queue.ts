import { Command } from "sacli";
import { QUEUE_CRAWL } from "../common/res";

const cli = Command.new("queue");

cli.subcommand("metrics").action(async () => {
  console.table(await QUEUE_CRAWL.metrics());
});

cli.eval(process.argv.slice(2));
