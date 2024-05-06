const { S3 } = require("@aws-sdk/client-s3");
const { readFile } = require("node:fs/promises");
const util = require("node:util");
const zlib = require("node:zlib");
const pathExtension = require("@xtjs/lib/pathExtension").default;

const brotli = util.promisify(zlib.brotliCompress);

const CF_ACCOUNT_ID = process.env["CF_ACCOUNT_ID"];
const CF_ACCESS_KEY_ID = process.env["CF_R2_ACCESS_KEY_ID"];
const CF_SECRET_ACCESS_KEY = process.env["CF_R2_SECRET_ACCESS_KEY"];

const s3 = new S3({
  region: "auto",
  endpoint: `https://${CF_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: CF_ACCESS_KEY_ID,
    secretAccessKey: CF_SECRET_ACCESS_KEY,
  },
});

const FILES = [
  "index.html",
  "dist/index.js",
  "dist/index.css",
  "dist/worker.PointLabels.js",
];

Promise.all(
  FILES.map(async (f) => {
    const raw = await readFile(`${__dirname}/${f}`);
    console.log(`Compressing ${f}`);
    const compressed = await brotli(raw, {
      params: {
        [zlib.constants.BROTLI_PARAM_QUALITY]:
          zlib.constants.BROTLI_MAX_QUALITY,
        [zlib.constants.BROTLI_PARAM_MODE]: zlib.constants.BROTLI_MODE_TEXT,
        [zlib.constants.BROTLI_PARAM_SIZE_HINT]: raw.length,
      },
    });
    console.log(`Uploading ${f}`);
    await s3.putObject({
      Bucket: "wilsonl-in-hn",
      Key: f,
      Body: compressed,
      ContentType: {
        css: "text/css",
        html: "text/html",
        js: "text/javascript",
      }[pathExtension(f)],
      ContentEncoding: "br",
    });
    console.log(`Uploaded ${f}`);
  }),
);
