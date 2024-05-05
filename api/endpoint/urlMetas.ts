import { decode } from "@msgpack/msgpack";
import {
  VArray,
  VInteger,
  VString,
  VStruct,
  VUtf8Bytes,
  Valid,
} from "@wzlin/valid";
import { UrlMeta, vUrlMeta } from "../../common/const";
import { db, getKvRow } from "../../common/res";

const input = new VStruct({
  // Must be normalized.
  urls: new VArray(new VString(), 1, 250),
});

export const endpointUrlMetas = {
  input,
  handler: async ({ urls }: Valid<typeof input>) => {
    const ids = await db.query(
      `select id, url from url where url in (${urls.map(() => "?").join(",")})`,
      urls,
      new VStruct({
        id: new VInteger(1),
        url: new VUtf8Bytes(new VString()),
      }),
    );
    const res: Record<string, UrlMeta> = {};
    await Promise.all(
      ids.map(async ({ id, url }) => {
        const raw = await getKvRow.execute(`url/${id}/meta`);
        if (!raw) {
          return;
        }
        res[url] = vUrlMeta.parseRoot(decode(raw));
      }),
    );
    return res;
  },
};
