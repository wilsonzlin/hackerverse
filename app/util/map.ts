import { decode } from "@msgpack/msgpack";
import { VArray, VString, Valid } from "@wzlin/valid";
import Dict from "@xtjs/lib/Dict";
import UnreachableError from "@xtjs/lib/UnreachableError";
import assertExists from "@xtjs/lib/assertExists";
import assertState from "@xtjs/lib/assertState";
import derivedComparator from "@xtjs/lib/derivedComparator";
import propertyComparator from "@xtjs/lib/propertyComparator";
import reversedComparator from "@xtjs/lib/reversedComparator";
import slices from "@xtjs/lib/slices";
import RBush, { BBox } from "rbush";
import {
  MapStateInit,
  ViewportState,
  vPointLabelsMessageToMain,
  vPointLabelsMessageToWorker,
} from "./const";
import { CACHED_FETCH_404, cachedFetch } from "./fetch";
import { DEFAULT_EDGE } from "./item";

export const MAP_DATASET = "toppost";

export const ZOOM_PER_LOD = 3;

export type Point = { id: number; x: number; y: number; score: number };

export class PointTree extends RBush<Point> {
  toBBox(item: Point): BBox {
    return {
      minX: item.x,
      maxX: item.x,
      maxY: item.y,
      minY: item.y,
    };
  }
  compareMinX(a: Point, b: Point): number {
    return a.x - b.x;
  }
  compareMinY(a: Point, b: Point): number {
    return a.y - b.y;
  }
}

export class MapState {
  readonly lodLevels: number;
  readonly scoreMax: number;
  readonly scoreMin: number;
  readonly xMaxPt: number;
  readonly xMinPt: number;
  readonly yMaxPt: number;
  readonly yMinPt: number;

  constructor(args: MapStateInit) {
    this.lodLevels = args.lodLevels;
    this.scoreMax = args.scoreMax;
    this.scoreMin = args.scoreMin;
    this.xMaxPt = args.xMaxPt;
    this.xMinPt = args.xMinPt;
    this.yMaxPt = args.yMaxPt;
    this.yMinPt = args.yMinPt;
  }

  get scoreRange() {
    return this.scoreMax - this.scoreMin + 1;
  }

  get xRangePt() {
    return this.xMaxPt - this.xMinPt;
  }

  get yRangePt() {
    return this.yMaxPt - this.yMinPt;
  }

  // This is mostly arbitrary, but we should have a value because some calculations depend on a fixed upper limit.
  get zoomMax() {
    return this.lodLevels * ZOOM_PER_LOD + 1;
  }

  calcLod(z: number | ViewportState) {
    const zoom = typeof z === "number" ? z : z.zoom;
    return Math.min(this.lodLevels - 1, Math.floor(zoom / ZOOM_PER_LOD));
  }

  viewportScale({
    heightPx,
    widthPx,
    zoom,
  }: {
    heightPx: number;
    widthPx: number;
    zoom: number;
  }) {
    // Add some padding around edges of "virtual" map so that at zoom 0 (i.e. fully zooomed out), points aren't on the edges of the screen.
    const PADDING_PX = 32;
    let pxPerPtBase;
    if (heightPx < widthPx) {
      pxPerPtBase = (heightPx - PADDING_PX * 2) / this.yRangePt;
    } else {
      pxPerPtBase = (widthPx - PADDING_PX * 2) / this.xRangePt;
    }

    // This should grow exponentially with zoom, as otherwise the distances between points shrink the more you zoom in, due to min point distances exponentially increasing with each LOD level.
    const pxPerPt = pxPerPtBase * Math.pow(1 + 1 / ZOOM_PER_LOD, zoom);
    const pxToPt = (px: number) => px / pxPerPt;
    const ptToPx = (pt: number) => pt * pxPerPt;
    return {
      pxPerPtBase,
      pxToPt,
      ptToPx,
      scaled({ x0Pt, y0Pt }: { x0Pt: number; y0Pt: number }) {
        const x1Pt = x0Pt + pxToPt(widthPx);
        const y1Pt = y0Pt + pxToPt(heightPx);
        return {
          x1Pt,
          y1Pt,
        };
      },
    };
  }

  viewportTiles(vp: ViewportState) {
    const lod = this.calcLod(vp);

    const axisTileCount = 2 ** lod;
    const tileWidthPt = this.xRangePt / axisTileCount;
    const tileHeightPt = this.yRangePt / axisTileCount;

    const scale = this.viewportScale(vp);
    const { x1Pt, y1Pt } = scale.scaled(vp);

    const tileXMin = Math.max(
      0,
      Math.floor((vp.x0Pt - this.xMinPt) / tileWidthPt),
    );
    const tileXMax = Math.min(
      axisTileCount - 1,
      Math.floor((x1Pt - this.xMinPt) / tileWidthPt),
    );
    const tileYMin = Math.max(
      0,
      Math.floor((vp.y0Pt - this.yMinPt) / tileHeightPt),
    );
    const tileYMax = Math.min(
      axisTileCount - 1,
      Math.floor((y1Pt - this.yMinPt) / tileHeightPt),
    );

    return {
      tileXMin,
      tileXMax,
      tileYMin,
      tileYMax,
    };
  }
}

export const parseTileData = (raw: ArrayBuffer) => {
  let i = 0;
  const count = new Uint32Array(raw, i, 1)[0];
  i += 4;
  const ids = new Uint32Array(raw, i, count);
  i += 4 * count;
  const xs = new Float32Array(raw, i, count);
  i += 4 * count;
  const ys = new Float32Array(raw, i, count);
  i += 4 * count;
  const scores = new Int16Array(raw, i, count);
  i += 2 * count;
  const res = Array.from({ length: count }, (_, j) => ({
    id: ids[j],
    x: xs[j],
    y: ys[j],
    score: scores[j],
  }));
  assertState(i === raw.byteLength);
  return res;
};

export const cachedFetchTile = async (
  signal: AbortSignal | undefined,
  edge: string,
  lod: number,
  x: number,
  y: number,
) => {
  const res = await cachedFetch(
    `https://${edge}.edge-hndr.wilsonl.in/map/${MAP_DATASET}/tile/${lod}/${x}-${y}`,
    signal,
    // Not all tiles exist (i.e. no points exist).
    "except-404",
  );
  return res.body === CACHED_FETCH_404 ? [] : parseTileData(res.body);
};

const postTitleLengthFetchStarted = new Set<number>();
const postTitleLengths: Record<number, number> = {};
export const ensureFetchedPostTitleLengths = async (
  edge: string,
  ids: number[],
) => {
  const missing = [];
  for (const id of ids) {
    if (!postTitleLengthFetchStarted.has(id)) {
      postTitleLengthFetchStarted.add(id);
      missing.push(id);
    }
  }
  if (missing.length) {
    const res = await fetch(
      `https://${edge}.edge-hndr.wilsonl.in/post-title-lengths`,
      {
        method: "POST",
        body: new Uint32Array(missing),
      },
    );
    if (!res.ok) {
      throw new Error(
        `Failed to fetch post title lengths with status ${res.status}`,
      );
    }
    const rawBuf = await res.arrayBuffer();
    const raw = new Uint8Array(rawBuf);
    assertState(raw.length === missing.length);
    for (const [i, id] of missing.entries()) {
      postTitleLengths[id] = raw[i];
    }
  }
};

const LABEL_FONT_SIZE = 13;
const LABEL_POINT_GAP = 4;
const LABEL_MARGIN = 13;

export const calcLabelBBox = (map: MapState, vp: ViewportState, p: Point) => {
  const scale = map.viewportScale(vp);
  const canvasX = scale.ptToPx(p.x);
  const canvasY = scale.ptToPx(p.y);
  const titleLen = assertExists(postTitleLengths[p.id]);
  const box: BBox = {
    minX: canvasX - LABEL_MARGIN,
    // Guessed approximate width based on title length.
    // (We can't reasonably fetch millions of titles just to measure their text and filter down to ~10.)
    maxX: canvasX + titleLen * (LABEL_FONT_SIZE / 1.6) + LABEL_MARGIN,
    minY: canvasY - LABEL_MARGIN,
    maxY: canvasY + LABEL_FONT_SIZE + LABEL_MARGIN,
  };
  return box;
};

const renderImage = ({
  canvas,
  context: ctx,
  image,
  map,
  viewport: vp,
}: {
  canvas: HTMLCanvasElement;
  context: CanvasRenderingContext2D;
  map: MapState;
  viewport: ViewportState;
  image: ImageBitmap;
}) => {
  const scale = map.viewportScale(vp);
  const scaled = scale.scaled(vp);

  const vpRatioX0 = (vp.x0Pt - map.xMinPt) / map.xRangePt;
  const vpRatioY0 = (vp.y0Pt - map.yMinPt) / map.yRangePt;
  const vpRatioX1 = (scaled.x1Pt - map.xMinPt) / map.xRangePt;
  const vpRatioY1 = (scaled.y1Pt - map.yMinPt) / map.yRangePt;

  const dx = 0;
  const dy = 0;
  const dWidth = canvas.width;
  const dHeight = canvas.height;
  const sx = image.width * vpRatioX0;
  const sy = image.height * vpRatioY0;
  const sWidth = image.width * (vpRatioX1 - vpRatioX0);
  const sHeight = image.height * (vpRatioY1 - vpRatioY0);

  ctx.drawImage(image, sx, sy, sWidth, sHeight, dx, dy, dWidth, dHeight);
};

export const createCanvasPointMap = ({
  canvas,
  map,
}: {
  canvas: HTMLCanvasElement;
  map: MapState;
}) => {
  let edge = DEFAULT_EDGE;
  const abortController = new AbortController();
  const postTitleFetchStarted = new Set<number>();
  const postTitles = new Dict<number, string>();
  const lodTrees = Array.from({ length: map.lodLevels }, () => ({
    tree: new PointTree(),
    loadedTiles: new Set<string>(),
  }));
  let latestRenderRequestId = 0;
  let curPoints = Array<Point>(); // This must always be sorted by score descending.
  let curViewport: ViewportState | undefined;
  let theme: "land" | "space" = "space";
  let heatmaps: ImageBitmap[] = [];
  let resultPoints: { x: number; y: number }[] = [];

  let terrain = Array<{ level: number; points: { x: number; y: number }[] }>();
  (async () => {
    const res = await fetch(
      `https://${edge}.edge-hndr.wilsonl.in/map/${MAP_DATASET}/terrain`,
    );
    if (!res.ok) {
      throw new Error(`Failed to fetch terrain with error ${res.status}`);
    }
    const raw = await res.arrayBuffer();
    const dv = new DataView(raw);
    let i = 0;
    const paths = [];
    while (i < raw.byteLength) {
      const level = dv.getUint32(i, true);
      i += 4;
      const pathCount = dv.getUint32(i, true);
      i += 4;
      for (let j = 0; j < pathCount; j++) {
        const pathLen = dv.getUint32(i, true);
        i += 4;
        const pointsRaw = new Float32Array(
          raw.slice(i, (i += pathLen * 4 * 2)),
        );
        paths.push({
          level,
          points: slices(pointsRaw, 2).map(([x, y]) => ({ x, y })),
        });
      }
    }
    assertState(i === raw.byteLength);
    // Render level 1, then 2 on top, then 3, etc. However, render 0 last, because those are holes.
    terrain = paths.sort(derivedComparator((e) => e.level || Infinity));
    // @ts-expect-error This is not used before initialization.
    render();
  })();

  // Zoom (integer) level => point IDs.
  const labelledPoints = new Dict<
    number,
    {
      points: Set<number>;
      cities: Array<{
        label: string;
        x: number;
        y: number;
      }>;
    }
  >();

  const worker = new Worker("/dist/worker.PointLabels.js");
  worker.addEventListener("message", (e) => {
    const msg = vPointLabelsMessageToMain.parseRoot(e.data);
    if (msg.$type === "update") {
      labelledPoints.set(msg.zoom, {
        points: msg.picked,
        cities: msg.cities,
      });
      const missing = [];
      for (const id of msg.picked) {
        if (!postTitleFetchStarted.has(id)) {
          postTitleFetchStarted.add(id);
          missing.push(id);
        }
      }
      (async () => {
        if (missing.length) {
          const res = await fetch(
            `https://${edge}.edge-hndr.wilsonl.in/post-titles`,
            {
              method: "POST",
              body: new Uint32Array(missing),
            },
          );
          if (!res.ok) {
            throw new Error(
              `Failed to fetch post titles with status ${res.status}`,
            );
          }
          const raw = await res.arrayBuffer();
          const titles = new VArray(
            new VString(),
            missing.length,
            missing.length,
          ).parseRoot(decode(raw));
          for (const [i, id] of missing.entries()) {
            postTitles.set(id, titles[i]);
          }
        }
        render();
      })();
    } else {
      throw new UnreachableError();
    }
  });
  const initMsg: Valid<typeof vPointLabelsMessageToWorker> = {
    $type: "init",
    edge,
    mapInit: map,
  };
  worker.postMessage(initMsg);

  let raf: ReturnType<typeof requestAnimationFrame> | undefined;
  const render = () => {
    if (raf != undefined) {
      cancelAnimationFrame(raf);
    }
    raf = requestAnimationFrame(() => {
      const vp = curViewport;
      if (!vp) {
        return;
      }
      const pointRadius = {
        land: 3,
        space: 2,
      }[theme];
      const scale = map.viewportScale(vp);
      const ctx = assertExists(canvas.getContext("2d"));
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = {
        land: "#6cd2e7",
        space: "black",
      }[theme];
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      for (const { level, points } of terrain) {
        ctx.beginPath();
        const toCanvasPos = ({ x, y }: { x: number; y: number }) =>
          [scale.ptToPx(x - vp.x0Pt), scale.ptToPx(y - vp.y0Pt)] as const;
        ctx.moveTo(...toCanvasPos(points[0]));
        for (const p of points.slice(1)) {
          ctx.lineTo(...toCanvasPos(p));
        }
        ctx.closePath();
        if (theme == "land") {
          ctx.fillStyle = ["#6cd2e7", "#bbecd8", "#a7e6cc", "#90e0be"][level];
          ctx.fill();
        } else if (theme == "space") {
          ctx.lineWidth = 1;
          ctx.strokeStyle = `rgba(255, 255, 255, ${level / 8})`;
          ctx.stroke();
        } else {
          throw new UnreachableError();
        }
      }
      for (const heatmap of heatmaps) {
        renderImage({
          canvas,
          context: ctx,
          image: heatmap,
          map,
          viewport: vp,
        });
      }
      const lp = labelledPoints.get(Math.floor(vp.zoom));
      for (const p of curPoints) {
        const scoreWeight = Math.max(
          0,
          Math.log(p.score - map.scoreMin) / Math.log(map.scoreRange),
        );
        const labelled = lp?.points.has(p.id);
        const canvasX = scale.ptToPx(p.x - vp.x0Pt);
        const canvasY = scale.ptToPx(p.y - vp.y0Pt);
        const minAlpha = 0.25 * (vp.zoom / map.zoomMax + 1);
        const alpha =
          (scoreWeight * (1 - minAlpha) + minAlpha) *
          (labelled ? 1 : 0.6 * (vp.zoom / map.zoomMax) + 0.15);
        ctx.fillStyle = {
          land: {
            true: `rgba(3, 165, 252, ${alpha})`,
            false: `rgba(120, 120, 120, ${alpha})`,
          },
          space: {
            true: `rgba(255, 255, 255, ${alpha})`,
            false: `rgba(140, 140, 140, ${alpha})`,
          },
        }[theme][`${!!labelled}`];
        ctx.beginPath();
        ctx.arc(canvasX, canvasY, pointRadius, 0, Math.PI * 2);
        ctx.fill();
        if (labelled) {
          ctx.lineWidth = 1;
          ctx.strokeStyle = "white";
          ctx.stroke();
        }
      }
      // Draw result points.
      for (const p of resultPoints) {
        const canvasX = scale.ptToPx(p.x - vp.x0Pt);
        const canvasY = scale.ptToPx(p.y - vp.y0Pt);
        ctx.fillStyle = `rgb(252, 123, 3)`;
        ctx.beginPath();
        ctx.arc(canvasX, canvasY, pointRadius, 0, Math.PI * 2);
        ctx.fill();
      }
      // Draw labels over points.
      for (const p of curPoints) {
        if (!lp?.points.has(p.id)) {
          continue;
        }
        const label = postTitles.get(p.id);
        if (!label) {
          continue;
        }
        const canvasX =
          scale.ptToPx(p.x - vp.x0Pt) + pointRadius / 2 + LABEL_POINT_GAP;
        const canvasY =
          scale.ptToPx(p.y - vp.y0Pt) + LABEL_FONT_SIZE / 2 - pointRadius / 2;
        ctx.font = `${LABEL_FONT_SIZE}px InterVariable, sans-serif`;
        ctx.strokeStyle = {
          land: "white",
          space: "black",
        }[theme];
        ctx.lineWidth = 2;
        ctx.strokeText(label, canvasX, canvasY);
        ctx.fillStyle = {
          land: "black",
          space: "white",
        }[theme];
        ctx.fillText(label, canvasX, canvasY);
      }
      // Draw cities.
      for (const p of lp?.cities ?? []) {
        const fontSize = 14.5;
        const canvasX = scale.ptToPx(p.x - vp.x0Pt);
        const canvasY = scale.ptToPx(p.y - vp.y0Pt);
        ctx.font = `550 ${fontSize}px InterVariable, sans-serif`;
        ctx.strokeStyle = {
          land: "white",
          space: "black",
        }[theme];
        ctx.lineWidth = 2;
        ctx.strokeText(p.label, canvasX, canvasY);
        ctx.fillStyle = {
          land: "black",
          space: "white",
        }[theme];
        ctx.fillText(p.label, canvasX, canvasY);
      }
    });
  };

  return {
    destroy: () => {
      abortController.abort();
    },
    setEdge: (e: string) => {
      edge = e;
    },
    setTheme: (t: "land" | "space") => {
      theme = t;
      render();
    },
    setHeatmaps: (hm: ImageBitmap[]) => {
      heatmaps = hm;
      render();
    },
    setResultPoints: (points: { x: number; y: number }[]) => {
      resultPoints = points;
      render();
    },
    // Render the points at LOD `lod` from (ptX0, ptY0) to (ptX1, ptY1) (inclusive) on the canvas.
    render: async (newViewport: ViewportState) => {
      const requestId = ++latestRenderRequestId;

      const lod = map.calcLod(newViewport);
      const scale = map.viewportScale(newViewport);
      const { x1Pt, y1Pt } = scale.scaled(newViewport);

      curViewport = newViewport;
      // Remain responsive to user interaction by redrawing the map with the current points at the new pan/zoom, even if these won't be the final points. Don't just look up in the tree immediately, since it may be a different level and the points may not be loaded yet, so the map will suddenly go blank.
      const msg: Valid<typeof vPointLabelsMessageToWorker> = {
        $type: "calculate",
        viewport: newViewport,
      };
      worker.postMessage(msg);
      render();

      // Ensure all requested tiles are fetched.
      const { tileXMax, tileXMin, tileYMax, tileYMin } =
        map.viewportTiles(newViewport);
      await Promise.all(
        (function* () {
          for (let x = tileXMin; x <= tileXMax; x++) {
            for (let y = tileYMin; y <= tileYMax; y++) {
              yield (async () => {
                const points = await cachedFetchTile(
                  abortController.signal,
                  edge,
                  lod,
                  x,
                  y,
                );
                const lt = lodTrees[lod];
                const k = `${x}-${y}`;
                if (!lt.loadedTiles.has(k)) {
                  lt.loadedTiles.add(k);
                  lt.tree.load(points);
                }
              })();
            }
          }
        })(),
      );
      if (requestId !== latestRenderRequestId) {
        // This render request is now outdated.
        return;
      }
      // Render the final points.
      curPoints = lodTrees[lod].tree
        .search({
          minX: newViewport.x0Pt,
          maxX: x1Pt,
          minY: newViewport.y0Pt,
          maxY: y1Pt,
        })
        .sort(reversedComparator(propertyComparator("score")));
      render();
    },
  };
};
