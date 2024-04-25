import assertExists from "@xtjs/lib/assertExists";
import { ConfigFileAuthenticationDetailsProvider, Region } from "oci-common";
import { ContainerInstanceClient } from "oci-containerinstances";
import { ContainerInstance } from "oci-containerinstances/lib/model";
import { Command } from "sacli";

const compartmentId = assertExists(process.env["OCI_COMPARTMENT_OCID"]);

const ci = new ContainerInstanceClient({
  authenticationDetailsProvider: new ConfigFileAuthenticationDetailsProvider(
    process.env["OCI_CLI_CONFIG_FILE"],
  ),
});

const cli = Command.new("crawler");

cli
  .subcommand("launch")
  .required("region", String)
  .action(async (args) => {
    ci.region = Region.fromRegionId(args.region);
    const envSfx = args.region.toUpperCase().replace(/-/g, "_");
    const availabilityDomain = assertExists(
      process.env[`OCI_AVAILABILITY_DOMAIN_${envSfx}`],
    );
    await ci.createContainerInstance({
      createContainerInstanceDetails: {
        availabilityDomain,
        compartmentId,
        containerRestartPolicy: ContainerInstance.ContainerRestartPolicy.Always, // Restart on OOM.
        containers: [
          {
            displayName: "main",
            imageUrl: "docker.io/wilsonzlin/hndr-edge",
          },
          {
            displayName: "caddy",
            imageUrl: "docker.io/wilsonzlin/hndr-edge-caddy",
            environmentVariables: {
              EDGE_CADDY_ACME_EMAIL: assertExists(
                process.env["EDGE_CADDY_ACME_EMAIL"],
              ),
              EDGE_DOMAIN: `${args.region}.${assertExists(process.env["EDGE_DOMAIN_SUFFIX"])}`,
            },
          },
        ],
        displayName: `hndr-edge-${args.region}`,
        shape: "CI.Standard.A1.Flex",
        shapeConfig: {
          ocpus: 1,
          memoryInGBs: 6,
        },
        vnics: [
          {
            isPublicIpAssigned: true,
            subnetId: assertExists(process.env[`OCI_SUBNET_OCID_${envSfx}`]),
            nsgIds: assertExists(process.env[`OCI_NSG_OCIDS_${envSfx}`]).split(
              ",",
            ),
          },
        ],
      },
    });
  });

cli.eval(process.argv.slice(2));
