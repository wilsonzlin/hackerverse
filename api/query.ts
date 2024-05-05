import { decode, encode } from "@msgpack/msgpack";
import { Validator } from "@wzlin/valid";
import UnreachableError from "@xtjs/lib/UnreachableError";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import assertState from "@xtjs/lib/assertState";
import bigIntToNumber from "@xtjs/lib/bigIntToNumber";
import isArrayOf from "@xtjs/lib/isArrayOf";
import map from "@xtjs/lib/map";
const { Float16Array } = require("@petamoriken/float16");

type ColVal = string | number;
// A column's raw data could be a packed array of floats/ints, or a MessagePack-encoded list of strings. If the packed array is of 64-bit integers, it's converted into an array of numbers.
type Col = ArrayLike<ColVal> & Iterable<ColVal>;

export class QueryGroupByOutput {
  constructor(
    private readonly groupsArray: Col,
    private readonly colArrays: Record<string, Col>,
  ) {}

  get length() {
    return this.groupsArray.length;
  }

  *groups<G>(groupValidator: Validator<G>) {
    for (const raw of this.groupsArray) {
      yield groupValidator.parseRoot(raw);
    }
  }

  *column<C extends ColVal>(name: string, columnValidator: Validator<C>) {
    for (const raw of this.colArrays[name]) {
      yield columnValidator.parseRoot(raw);
    }
  }

  *entries<G extends ColVal, R extends Record<string, ColVal>>(
    groupValidator: Validator<G>,
    columnValidators: { [name in keyof R]: Validator<R[name]> },
  ) {
    for (let i = 0; i < this.length; i++) {
      yield [
        groupValidator.parseRoot(this.groupsArray[i]),
        Object.fromEntries(
          Object.entries(this.colArrays).map(([col, vals]) => [
            col,
            columnValidators[col as keyof R].parseRoot(vals[i]),
          ]),
        ) as unknown as R,
      ] as const;
    }
  }
}

export class QueryItemsOutput {
  private length: number = -1;

  constructor(private readonly colArrays: Record<string, Col>) {
    for (const [col, vals] of Object.entries(colArrays)) {
      if (this.length == -1) {
        this.length = vals.length;
      }
      assertState(this.length === vals.length, `${col} has invalid length`);
    }
    assertState(this.length > -1);
  }

  *items<R extends Record<string, ColVal>>(columnValidators: {
    [name in keyof R]: Validator<R[name]>;
  }) {
    for (let i = 0; i < this.length; i++) {
      yield Object.fromEntries(
        Object.entries(this.colArrays).map(([col, vals]) => [
          col,
          columnValidators[col].parseRoot(vals[i]),
        ]),
      ) as unknown as R;
    }
  }
}

export class QueryHeatmapOutput {
  constructor(readonly raw: ArrayBuffer) {}

  blob() {
    return new Blob([this.raw], { type: "image/webp" });
  }

  url() {
    return URL.createObjectURL(this.blob());
  }
}

export type QueryInputOutput =
  | {
      group_by: {
        by: string;
        bucket?: number;
        cols: Array<[string, "mean" | "min" | "max" | "sum" | "count"]>;
        order_by?: string;
        order_asc?: boolean;
        limit?: number;
      };
    }
  | {
      items: {
        cols: Array<string>;
        order_by?: string;
        order_asc?: boolean;
        limit?: number;
      };
    }
  | {
      heatmap: {
        density: number;
        color: readonly [number, number, number];
        alpha_scale?: number;
        sigma?: number;
        upscale?: number;
      };
    };

export type QueryClip = {
  min: number;
  max: number;
};

export type QueryInput = {
  dataset: "comment" | "post" | "toppost";
  outputs: Array<QueryInputOutput>;
  queries: Array<string>;

  sim_agg?: "mean" | "min" | "max";

  ts_decay?: number;

  pre_filter_ann?: number;

  scales?: Record<string, QueryClip>;

  thresholds?: Record<string, number>;

  weights?: Record<string, string | number>;

  post_filter_clip?: Record<string, QueryClip>;
};

export const makeQuery = async (q: QueryInput) => {
  const res = await fetch(`http://localhost:6050/${q.dataset}`, {
    method: "POST",
    body: encode(q),
  });
  if (!res.ok) {
    throw new Error(`Failed to query: ${res.status}`);
  }
  const payloadBuf = assertInstanceOf(
    decode(await res.arrayBuffer()),
    Uint8Array,
  );
  // We want `payload` to be an ArrayBuffer, not an Uint8Array, as the latter is treated as a list of numbers, not the underlying raw bytes, when passed to a TypedArray or DataView constructor.
  const payload = payloadBuf.buffer.slice(
    payloadBuf.byteOffset,
    payloadBuf.byteOffset + payloadBuf.byteLength,
  );
  const dv = new DataView(payload);
  let i = 0;
  const out = q.outputs.map((out) => {
    const unpackRows = (count: number, cols: string[]) => {
      const colArrays: Record<string, Col> = {};
      for (const col of cols) {
        const kind = String.fromCharCode(dv.getUint8(i++));
        if (kind === "O") {
          const rawLen = dv.getUint32(i, true);
          i += 4;
          const decoded = decode(new Uint8Array(payload, i, rawLen));
          if (
            !Array.isArray(decoded) ||
            !isArrayOf(decoded, (v): v is string => typeof v == "string")
          ) {
            throw new TypeError(`Object column "${col}" isn't list of strings`);
          }
          colArrays[col] = decoded;
          i += rawLen;
        } else {
          const itemSize = dv.getUint8(i++);
          const k = `${kind}${itemSize}`;
          const ctor = {
            f2: Float16Array,
            f4: Float32Array,
            f8: Float64Array,
            i1: Int8Array,
            i2: Int16Array,
            i4: Int32Array,
            i8: BigInt64Array,
            u1: Uint8Array,
            u2: Uint16Array,
            u4: Uint32Array,
            u8: BigUint64Array,
          }[k];
          if (!ctor) {
            throw new TypeError(`Unrecognised column "${col}" dtype "${k}"`);
          }
          // We must slice as the offset may not be aligned.
          const raw = new ctor(payload.slice(i, (i += count * itemSize)));
          if (raw instanceof BigUint64Array || raw instanceof BigInt64Array) {
            colArrays[col] = Array.from(map(raw, bigIntToNumber));
          } else {
            colArrays[col] = raw;
          }
        }
      }
      return colArrays;
    };
    if ("group_by" in out) {
      const count = dv.getUint32(i, true);
      i += 4;
      const { group, ...colArrays } = unpackRows(count, [
        "group",
        ...out.group_by.cols.map((c) => c[0]),
      ]);
      return new QueryGroupByOutput(group, colArrays);
    }
    if ("heatmap" in out) {
      const rawLen = dv.getUint32(i, true);
      i += 4;
      const raw = payload.slice(i, (i += rawLen));
      return new QueryHeatmapOutput(raw);
    }
    if ("items" in out) {
      const count = dv.getUint32(i, true);
      i += 4;
      const colArrays = unpackRows(count, out.items.cols);
      return new QueryItemsOutput(colArrays);
    }
    throw new UnreachableError();
  });
  assertState(i === payload.byteLength);
  return out;
};
