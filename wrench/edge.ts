import assertExists from "@xtjs/lib/assertExists";
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

cli.subcommand("launch").action(async () => {
  await ci.createContainerInstance({
    createContainerInstanceDetails: {
      availabilityDomain,
      compartmentId,
      containerRestartPolicy: ContainerInstance.ContainerRestartPolicy.Never,
      containers: [
        {
          displayName: "main",
          imageUrl: "docker.io/wilsonzlin/hndr-edge",
          environmentVariables: {
            EDGE_SSL_CA_BASE64: assertExists(process.env["EDGE_SSL_CA_BASE64"]),
            EDGE_SSL_CERT_BASE64: assertExists(
              process.env["EDGE_SSL_CERT_BASE64"],
            ),
            EDGE_SSL_KEY_BASE64: assertExists(
              process.env["EDGE_SSL_KEY_BASE64"],
            ),
            PORT: "443",
          },
        },
      ],
      displayName: "hndr-edge",
      shape: "CI.Standard.A1.Flex",
      shapeConfig: {
        ocpus: 1,
        memoryInGBs: 8,
      },
      vnics: [
        {
          isPublicIpAssigned: true,
          subnetId: assertExists(process.env["OCI_SUBNET_OCID"]),
        },
      ],
    },
  });
});

cli.eval(process.argv.slice(2));
