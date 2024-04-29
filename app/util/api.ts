import Dict from "@xtjs/lib/Dict";
import UnreachableError from "@xtjs/lib/UnreachableError";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import assertState from "@xtjs/lib/assertState";
import bigIntToNumber from "@xtjs/lib/bigIntToNumber";
import map from "@xtjs/lib/map";

export type Clip = { min: number; max: number };

export class ApiGroupByOutput {
  constructor(
    private readonly groupsArray: Int32Array,
    private readonly colArrays: Record<
      string,
      ArrayLike<number> & Iterable<number>
    >,
  ) {}

  get length() {
    return this.groupsArray.length;
  }

  *groups() {
    yield* this.groupsArray;
  }

  *column(name: string) {
    yield* this.colArrays[name];
  }

  *entries() {
    for (let i = 0; i < this.length; i++) {
      yield [
        this.groupsArray[i],
        Object.fromEntries(
          Object.entries(this.colArrays).map(([col, vals]) => [col, vals[i]]),
        ),
      ] as const;
    }
  }

  object() {
    return Object.fromEntries(this.entries());
  }

  dict() {
    return new Dict(this.entries());
  }
}

export class ApiItemsOutput {
  private length: number = -1;

  constructor(private readonly colArrays: Record<string, ArrayLike<number>>) {
    for (const [col, vals] of Object.entries(colArrays)) {
      if (this.length == -1) {
        this.length = vals.length;
      }
      assertState(this.length === vals.length, `${col} has invalid length`);
    }
    assertState(this.length > -1);
  }

  *items() {
    for (let i = 0; i < this.length; i++) {
      yield Object.fromEntries(
        Object.entries(this.colArrays).map(([col, vals]) => [col, vals[i]]),
      );
    }
  }

  object() {
    return Object.fromEntries(map(this.items(), (e) => [e.id, e]));
  }

  dict() {
    return new Dict(map(this.items(), (e) => [e.id, e]));
  }
}

export class ApiHeatmapOutput {
  constructor(private readonly rawWebp: ArrayBuffer) {}

  blob() {
    return new Blob([this.rawWebp], { type: "image/webp" });
  }

  url() {
    return URL.createObjectURL(this.blob());
  }
}

export const apiCall = async (
  signal: AbortSignal,
  req: {
    dataset: string;
    queries: string[];
    sim_agg?: "mean" | "min" | "max";
    ts_decay?: number;
    pre_filter_hnsw?: number;
    pre_filter_clip?: Record<string, Clip>;
    scales?: Record<string, Clip>;
    thresholds?: Record<string, number>;
    weights?: Record<string, string | number>;
    post_filter_clip?: Record<string, Clip>;
    outputs: Array<
      | {
          group_by: {
            by: string;
            bucket?: number;
            cols: [string, "mean" | "min" | "max" | "sum" | "count"][];
          };
        }
      | {
          heatmap: {
            density: number;
            color: [number, number, number];
            alpha_scale?: number;
            sigma?: number;
            upscale?: number;
          };
        }
      | {
          items: {
            cols: string[];
            order_by?: string;
            order_asc?: boolean;
            limit?: number;
          };
        }
    >;
  },
) => {
  const res = await fetch("https://api-hndr.wilsonl.in/", {
    signal,
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(req),
  });
  if (!res.ok) {
    throw new Error(`Bad status ${res.status}: ${await res.text()}`);
  }
  const payload = await res.arrayBuffer();
  const dv = new DataView(payload);
  let i = 0;
  const out = req.outputs.map((out) => {
    const unpackRows = (count: number, cols: string[]) => {
      const colArrays: Record<string, ArrayLike<number> & Iterable<number>> =
        {};
      for (const col of cols) {
        const kind = String.fromCharCode(dv.getUint8(i++));
        const itemSize = dv.getUint8(i++);
        const k = `${kind}${itemSize}`;
        const ctor = {
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
      return colArrays;
    };
    if ("group_by" in out) {
      const count = dv.getUint32(i, true);
      i += 4;
      const { group, ...colArrays } = unpackRows(count, [
        "group",
        ...out.group_by.cols.map((c) => c[0]),
      ]);
      return new ApiGroupByOutput(
        assertInstanceOf(group, Int32Array),
        colArrays,
      );
    }
    if ("heatmap" in out) {
      const rawLen = dv.getUint32(i, true);
      i += 4;
      const raw = payload.slice(i, (i += rawLen));
      return new ApiHeatmapOutput(raw);
    }
    if ("items" in out) {
      const count = dv.getUint32(i, true);
      i += 4;
      const colArrays = unpackRows(count, out.items.cols);
      return new ApiItemsOutput(colArrays);
    }
    throw new UnreachableError();
  });
  assertState(i === payload.byteLength);
  return out;
};

export type ApiResponse = Awaited<ReturnType<typeof apiCall>>;
