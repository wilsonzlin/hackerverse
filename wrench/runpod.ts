import { VArray, VString, VStruct } from "@wzlin/valid";
import Semaphore from "@xtjs/lib/Semaphore";
import { Command } from "sacli";

const req = async (query: string, variables?: any) => {
  const res = await fetch(
    "https://api.runpod.io/graphql?api_key=" +
      encodeURIComponent(process.env["RUNPOD_API_KEY"]!),
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ query, variables }),
    },
  );
  const raw = await res.json();
  if (!res.ok || raw["errors"]?.length) {
    throw new Error(
      `Request failed with status ${res.status}: ${JSON.stringify(raw, null, 2)}`,
    );
  }
  return raw["data"];
};

const listTemplates = async () => {
  const {
    myself: { podTemplates },
  } = await req(
    `
      query myself {
        myself {
          podTemplates {
            advancedStart
            containerDiskInGb
            containerRegistryAuthId
            dockerArgs
            earned
            id
            imageName
            isPublic
            isRunpod
            isServerless
            name
            ports
            readme
            runtimeInMin
            startJupyter
            startScript
            startSsh
            volumeInGb
            volumeMountPath
          }
        }
      }
    `,
  );
  return podTemplates.filter((t: any) => !t.isPublic);
};

const cli = Command.new("runpod");

cli.subcommand("list-templates").action(async () => {
  console.log(await listTemplates());
});

cli
  .subcommand("launch")
  .required("template", String)
  .required("count", Number)
  .boolean("securecloud")
  .action(async (args) => {
    const templateId = await listTemplates().then(
      (res) => res.find((t: any) => t.name === args.template)?.id,
    );
    if (!templateId) {
      throw new Error(`Template not found: ${args.template}`);
    }
    const q = new Semaphore(100);
    await Promise.all(
      Array.from({ length: args.count }, () =>
        q.add(async () => {
          // NOTE: `gpuTypeIdList` is intentionally ordered in order of performance-per-dollar, do not sort alphabetically.
          await req(
            `
              mutation {
                podFindAndDeployOnDemand(
                  input: {
                    cloudType: ${args.securecloud ? "SECURE" : "COMMUNITY"}
                    containerDiskInGb: 0
                    dockerArgs: ""
                    env: []
                    gpuCount: 1
                    gpuTypeIdList: [
                      "NVIDIA GeForce RTX 3080 Ti",
                      "NVIDIA GeForce RTX 3080",
                      "NVIDIA GeForce RTX 3090",
                      "NVIDIA GeForce RTX 3090 Ti",
                      "Tesla V100-SXM2-16GB",
                      "NVIDIA RTX A4500",
                      "NVIDIA RTX A5000",
                      "NVIDIA GeForce RTX 4070 Ti",
                      "Tesla V100-FHHL-16GB",
                      "NVIDIA RTX A4000",
                      "NVIDIA RTX 4000 Ada Generation",
                      "NVIDIA A30",
                      "NVIDIA GeForce RTX 4090",
                    ]
                    minMemoryInGb: 4
                    minVcpuCount: 1
                    name: ${JSON.stringify(args.template)}
                    startJupyter: false
                    startSsh: true
                    supportPublicIp: true
                    templateId: "${templateId}"
                    volumeInGb: 0
                  }
                ) {
                  id
                }
              }
            `,
          );
        }),
      ),
    );
  });

cli.subcommand("terminate").action(async () => {
  const raw = await req(
    `
      query myself {
        myself {
          pods {
            id
          }
        }
      }
    `,
  );
  const pods = new VStruct({
    myself: new VStruct({
      pods: new VArray(
        new VStruct({
          id: new VString(),
        }),
      ),
    }),
  }).parseRoot(raw).myself.pods;
  console.log("Terminating", pods.length, "pods");
  const q = new Semaphore(100);
  await Promise.all(
    pods.map((p) =>
      q.add(async () => {
        await req(
          `
            mutation podTerminate($input: PodTerminateInput!) {
              podTerminate(input: $input)
            }
          `,
          {
            input: {
              podId: p.id,
            },
          },
        );
      }),
    ),
  );
});

cli.eval(process.argv.slice(2));
