import assertState from "@xtjs/lib/assertState";
import Dict from "@xtjs/lib/Dict";
import map from "@xtjs/lib/map";
import UnreachableError from "@xtjs/lib/UnreachableError";

export type Clip = { min: number; max: number };

export class ApiGroupByOutput {
  constructor(
    private readonly groupsArray: Uint32Array,
    private readonly scoresArray: Float32Array,
  ) {
    assertState(groupsArray.length === scoresArray.length);
  }

  get length() {
    return this.groupsArray.length;
  }

  *groups() {
    yield* this.groupsArray;
  }

  *entries() {
    for (let i = 0; i < this.length; i++) {
      yield [this.groupsArray[i], this.scoresArray[i]] as const;
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
  constructor(
    private readonly idsArray: Uint32Array,
    private readonly xsArray: Float32Array,
    private readonly ysArray: Float32Array,
    private readonly scoresArray: Float32Array,
  ) {
    assertState(idsArray.length === xsArray.length);
    assertState(idsArray.length === ysArray.length);
    assertState(idsArray.length === scoresArray.length);
  }

  get length() {
    return this.idsArray.length;
  }

  *ids() {
    yield* this.idsArray;
  }

  *items() {
    for (let i = 0; i < this.length; i++) {
      yield {
        id: this.idsArray[i],
        x: this.xsArray[i],
        y: this.ysArray[i],
        score: this.scoresArray[i],
      };
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
    sim_scale: Clip;
    sim_agg?: "mean" | "min" | "max";
    ts_weight_decay?: number;
    filter_hnsw?: number;
    filter_clip?: Record<string, Clip>;
    weights: Record<string, number>;
    outputs: Array<
      | {
          group_by: {
            group_by: string;
            group_bucket?: number;
            group_final_score_agg?: "mean" | "min" | "max" | "sum" | "count";
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
    if ("group_by" in out) {
      const count = dv.getUint32(i, true);
      i += 4;
      const groups = new Uint32Array(payload, i, count);
      i += count * 4;
      const scores = new Float32Array(payload, i, count);
      i += count * 4;
      return new ApiGroupByOutput(groups, scores);
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
      const ids = new Uint32Array(payload, i, count);
      i += count * 4;
      const xs = new Float32Array(payload, i, count);
      i += count * 4;
      const ys = new Float32Array(payload, i, count);
      i += count * 4;
      const scores = new Float32Array(payload, i, count);
      i += count * 4;
      return new ApiItemsOutput(ids, xs, ys, scores);
    }
    throw new UnreachableError();
  });
  assertState(i === payload.byteLength);
  return out;
};

export type ApiResponse = Awaited<ReturnType<typeof apiCall>>;
