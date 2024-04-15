import { EC2 } from "@aws-sdk/client-ec2";
import assertExists from "@xtjs/lib/js/assertExists";
import defined from "@xtjs/lib/js/defined";
import derivedComparator from "@xtjs/lib/js/derivedComparator";
import reversedComparator from "@xtjs/lib/js/reversedComparator";
import { readFile } from "fs/promises";
import { Command } from "sacli";

const client = new EC2();

const cli = Command.new("crawler");

cli
  .subcommand("terminate")
  .optional("service", String)
  .action(async (args) => {
    const service = args.service ?? "crawler";
    const res = await client.describeInstances({
      Filters: [
        { Name: "instance-state-name", Values: ["pending", "running"] },
        { Name: "tag:hndr", Values: [service] },
      ],
    });
    const instIds =
      res.Reservations?.flatMap(
        (r) => r.Instances?.map((i) => i.InstanceId) ?? [],
      ).filter(defined) ?? [];
    console.log("Terminating", instIds.length);
    await client.terminateInstances({
      InstanceIds: instIds,
    });
  });

cli
  .subcommand("launch")
  .required("count", Number, { default: true })
  .optional("service", String)
  .action(async (args) => {
    const service = args.service ?? "crawler";
    const image = await client
      .describeImages({
        Owners: ["amazon"],
        Filters: [
          {
            Name: "name",
            Values: ["al2023-*"],
          },
          {
            Name: "architecture",
            Values: ["arm64"],
          },
          {
            Name: "state",
            Values: ["available"],
          },
        ],
      })
      .then((r) =>
        assertExists(
          r.Images?.sort(
            reversedComparator(derivedComparator((i) => i.CreationDate)),
          ).at(0),
        ),
      );
    const telegrafConf = await readFile(
      `${__dirname}/../telegraf/telegraf.conf`,
      "utf-8",
    );
    const promtailConf = await readFile(
      `${__dirname}/../promtail.yaml`,
      "utf-8",
    );
    await client.runInstances({
      ImageId: assertExists(image.ImageId),
      InstanceInitiatedShutdownBehavior: "terminate",
      InstanceType: "t4g.nano",
      KeyName: process.env["AWS_SSH_KEY_NAME"],
      MaxCount: args.count,
      MinCount: 1,
      SecurityGroupIds: process.env["AWS_SECURITY_GROUP_IDS"]?.split(","),
      SubnetId: assertExists(process.env["AWS_SUBNET_ID"]),
      IamInstanceProfile: {
        Arn: process.env["AWS_IAM_INSTANCE_PROFILE_ARN"],
      },
      TagSpecifications: [
        { ResourceType: "instance", Tags: [{ Key: "hndr", Value: service }] },
      ],
      UserData: Buffer.from(
        `
#!/bin/bash

set -Eeuo pipefail
shopt -s nullglob

self_terminate() {
  # Wait for any pending Telegraf exports. NOTE: 5 seconds is not long enough.
  # Ensure newline at end of log file, or else exporter may not export last (unterminated) line. This often happens on crashes and panics.
  echo '' >>/app.log
  sleep 10

  echo 'Self terminating...'
  poweroff

  exit 1
}

trap 'self_terminate' ERR EXIT

dnf -y install docker
systemctl --now enable docker

export MAIN='${service}'

# The archive has paths like "./telegraf-*/usr/bin", so we need to strip 2 leading components.
curl -fLSs 'https://dl.influxdata.com/telegraf/releases/telegraf-1.30.1_linux_arm64.tar.gz' | tar -zxf - --strip-components 2 -C /
cat <<'EndOfTelegrafConf' >/telegraf.conf
${telegrafConf}
EndOfTelegrafConf
export INFLUXDB_ENDPOINT='${assertExists(process.env["INFLUXDB_ENDPOINT"])}'
export INFLUXDB_TOKEN='${assertExists(process.env["INFLUXDB_TOKEN"])}'
telegraf --config /telegraf.conf &

dnf -y install 'https://github.com/grafana/loki/releases/download/v2.9.7/promtail-2.9.7.aarch64.rpm'
cat <<'EndOfPromtailConf' >/promtail.yaml
${promtailConf}
EndOfPromtailConf
export LOKI_ENDPOINT='${assertExists(process.env["LOKI_ENDPOINT"])}'
export LOKI_BASICAUTH_USER='${assertExists(process.env["LOKI_BASICAUTH_USER"])}'
export LOKI_BASICAUTH_PASSWORD='${assertExists(process.env["LOKI_BASICAUTH_PASSWORD"])}'
promtail -config.file /promtail.yaml -config.expand-env=true &
# Wait for log collector to start, as it won't export existing log entries before it starts.
sleep 5

docker run \\
  --name hndr \\
  -d \\
  -e ARCHIVE_TODAY_USER_AGENT='${process.env["ARCHIVE_TODAY_USER_AGENT"]}' \\
  -e DB_RPC_API_KEY='${process.env["DB_RPC_API_KEY"]}' \\
  -e MAIN="$MAIN" \\
  -e QUEUED_API_KEY='${process.env["QUEUED_API_KEY"]}' \\
  -e USER_AGENT='${process.env["USER_AGENT"]}' \\
  wilsonzlin/hndr-rust-base

docker logs -f hndr &> /app.log
        `.trim(),
      ).toString("base64"),
    });
    console.log("Launched", args.count, "instances");
  });

cli.eval(process.argv.slice(2));
