import assertExists from "@xtjs/lib/js/assertExists";
import withoutUndefined from "@xtjs/lib/js/withoutUndefined";
import { ConfigFileAuthenticationDetailsProvider } from "oci-common";
import { ContainerInstanceClient } from "oci-containerinstances";
import { ContainerInstance } from "oci-containerinstances/lib/model";
import { Command } from "sacli";

const compartmentId = assertExists(process.env["OCI_COMPARTMENT_OCID"]);
const availabilityDomain = assertExists(process.env["OCI_AVAILABILITY_DOMAIN"]);

const ci = new ContainerInstanceClient({
  authenticationDetailsProvider: new ConfigFileAuthenticationDetailsProvider(
    process.env["OCI_CLI_CONFIG_FILE"],
  ),
});

const cli = Command.new("crawler");

cli
  .subcommand("terminate")
  .optional("service", String)
  .action(async (args) => {
    const service = args.service ?? "crawler";
    let page;
    do {
      const res = await ci.listContainerInstances({
        compartmentId,
        displayName: `hndr-${service}`,
        page,
      });
      const insts = res.containerInstanceCollection.items.filter(
        (i) => i.lifecycleState != "DELETED" && i.lifecycleState != "DELETING",
      );
      console.log("Terminating", insts.length);
      await Promise.all(
        insts.map((i) =>
          ci.deleteContainerInstance({
            containerInstanceId: i.id,
          }),
        ),
      );
      page = res.opcNextPage;
    } while (page);
  });

cli
  .subcommand("launch")
  .required("count", Number, { default: true })
  .optional("service", String)
  .action(async (args) => {
    const service = args.service ?? "crawler";
    await Promise.all(
      Array.from({ length: args.count }, () =>
        ci.createContainerInstance({
          createContainerInstanceDetails: {
            availabilityDomain,
            compartmentId,
            containerRestartPolicy:
              ContainerInstance.ContainerRestartPolicy.Never,
            containers: [
              {
                displayName: "main",
                imageUrl: "docker.io/wilsonzlin/hndr-rust-base",
                environmentVariables: withoutUndefined({
                  DB_RPC_API_KEY: assertExists(process.env["DB_RPC_API_KEY"]),
                  MAIN: service,
                  QUEUED_API_KEY: assertExists(process.env["QUEUED_API_KEY"]),
                  USER_AGENT: process.env["USER_AGENT"],
                }),
              },
              // https://docs.oracle.com/en/learn/manage-oci-container-instances/#introduction:~:text=When%20you%20have,as%20configured%20above.
              // > When you have multiple containers within a container instance, they share the compute resources like the CPU and memory that is available to the container instance. They also share the networking namespace, which is why the WordPress container can connect to the MySQL container over localhost or the loopback address 127.0.0.1 as configured above.
              {
                displayName: "telegraf",
                imageUrl: "docker.io/wilsonzlin/hndr-telegraf",
                environmentVariables: {
                  INFLUXDB_ENDPOINT: assertExists(
                    process.env["INFLUXDB_ENDPOINT"],
                  ),
                  INFLUXDB_TOKEN: assertExists(process.env["INFLUXDB_TOKEN"]),
                },
              },
            ],
            displayName: `hndr-${service}`,
            shape: "CI.Standard.A1.Flex",
            shapeConfig: {
              ocpus: 1,
              memoryInGBs: 4,
            },
            vnics: [
              {
                isPublicIpAssigned: true,
                subnetId: assertExists(process.env["OCI_SUBNET_OCID"]),
              },
            ],
          },
        }),
      ),
    );
    console.log("Launched", args.count, "containers");
  });

cli.eval(process.argv.slice(2));
