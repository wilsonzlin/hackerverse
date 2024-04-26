import Dict from "@xtjs/lib/Dict";
import assertExists from "@xtjs/lib/assertExists";
import assertState from "@xtjs/lib/assertState";
import propertyComparator from "@xtjs/lib/propertyComparator";
import reversedComparator from "@xtjs/lib/reversedComparator";
import RBush, { BBox } from "rbush";
import { CACHED_FETCH_404, cachedFetch } from "./fetch";
import { cachedFetchEdgePost } from "./item";

export const PX_PER_PT_BASE = 64;
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

  constructor(args: {
    lodLevels: number;
    scoreMax: number;
    scoreMin: number;
    xMaxPt: number;
    xMinPt: number;
    yMaxPt: number;
    yMinPt: number;
  }) {
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

  viewportTiles(vp: ViewportState) {
    const lod = this.calcLod(vp);

    const axisTileCount = 2 ** lod;
    const tileWidthPt = this.xRangePt / axisTileCount;
    const tileHeightPt = this.yRangePt / axisTileCount;

    const scale = viewportScale(vp);
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

type ViewportState = {
  heightPx: number;
  widthPx: number;
  x0Pt: number;
  y0Pt: number;
  zoom: number;
};

export const viewportScale = (z: number | ViewportState) => {
  const zoom = typeof z === "number" ? z : z.zoom;
  // This should grow exponentially with zoom, as otherwise the distances between points shrink the more you zoom in, due to min point distances exponentially increasing with each LOD level.
  const pxPerPt = PX_PER_PT_BASE * Math.pow(1 + 1 / ZOOM_PER_LOD, zoom);
  const pxToPt = (px: number) => px / pxPerPt;
  const ptToPx = (pt: number) => pt * pxPerPt;
  return {
    pxToPt,
    ptToPx,
    scaled(vp: ViewportState) {
      const x1Pt = vp.x0Pt + pxToPt(vp.widthPx);
      const y1Pt = vp.y0Pt + pxToPt(vp.heightPx);
      return {
        x1Pt,
        y1Pt,
      };
    },
  };
};

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
  return Array.from({ length: count }, (_, j) => ({
    id: ids[j],
    x: xs[j],
    y: ys[j],
    score: scores[j],
  }));
};

export const cachedFetchTile = async (
  signal: AbortSignal,
  edge: string,
  lod: number,
  x: number,
  y: number,
) => {
  const res = await cachedFetch(
    `https://${edge}.edge-hndr.wilsonl.in/map/hnsw/tile/${lod}/${x}-${y}`,
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

const LABEL_FONT_SIZE = 12;
const LABEL_FONT_STYLE = `${LABEL_FONT_SIZE}px sans-serif`;
const LABEL_MARGIN = 12;

const calcLabelBBox = (zoom: number, vp: ViewportState, p: Point) => {
  const scale = viewportScale(zoom);
  const canvasX = scale.ptToPx(p.x - vp.x0Pt);
  const canvasY = scale.ptToPx(p.y - vp.y0Pt);
  // This must exist because we've already computed the previous zoom level.
  const titleLen = assertExists(postTitleLengths[p.id]);
  const box: BBox = {
    minX: scale.pxToPt(canvasX - LABEL_MARGIN),
    // Guessed approximate width based on title length.
    maxX: scale.pxToPt(
      canvasX + titleLen * (LABEL_FONT_SIZE / 1.6) + LABEL_MARGIN,
    ),
    minY: scale.pxToPt(canvasY - LABEL_MARGIN),
    maxY: scale.pxToPt(canvasY + LABEL_FONT_SIZE + LABEL_MARGIN),
  };
  return box;
};

export const createCanvasPointMap = ({
  canvas,
  edge,
  map,
}: {
  canvas: HTMLCanvasElement;
  edge: string;
  map: MapState;
}) => {
  const abortController = new AbortController();
  const postTitles = new Dict<number, string>();
  const lodTrees = Array.from({ length: map.lodLevels }, () => ({
    tree: new PointTree(),
    loadedTiles: new Set<string>(),
  }));
  let latestRenderRequestId = 0;
  let curPoints = Array<Point>(); // This must always be sorted by score descending.
  let curViewport: ViewportState | undefined;

  // We don't want to constantly calculate labelled points, as it causes jarring flashes of text. Instead, we want to keep the same points labelled for a few seconds.
  // The exception is when we're changing LOD level, as that's when many points will enter/exit, so we want to recompute immediately as the intermediate state looks weird.
  type LabelledPoint = {
    point: Point;
    picked: boolean; // Not picked if collided.
  };
  // One for each integer zoom level [0, map.zoomMax] (inclusive).
  const labelledPoints = Array.from({ length: map.zoomMax + 1 }, () => ({
    processedTiles: new Set<string>(),
    points: new Dict<number, LabelledPoint>(),
    tree: new RBush<BBox>(),
  }));
  let latestPickLabelledPointsReqId = 0;
  let pickingLabelledPoints = false;
  const pickLabelledPoints = () => {
    if (pickingLabelledPoints) {
      latestPickLabelledPointsReqId++;
      return;
    }
    pickingLabelledPoints = true;
    (async () => {
      while (true) {
        const req = latestPickLabelledPointsReqId;
        const vp = assertExists(curViewport);
        for (let z = 0; z <= vp.zoom; z++) {
          const lod = map.calcLod(z);
          const { tileXMax, tileXMin, tileYMax, tileYMin } = map.viewportTiles({
            ...vp,
            zoom: z,
          });
          const lp = labelledPoints[z];
          const points = await Promise.all(
            (function* () {
              for (let x = tileXMin; x <= tileXMax; x++) {
                for (let y = tileYMin; y <= tileYMax; y++) {
                  const k = `${x}-${y}`;
                  if (lp.processedTiles.has(k)) {
                    continue;
                  }
                  lp.processedTiles.add(k);
                  yield cachedFetchTile(
                    abortController.signal,
                    edge,
                    lod,
                    x,
                    y,
                  );
                }
              }
            })(),
          ).then((res) =>
            res.flat().sort(reversedComparator(propertyComparator("score"))),
          );
          // Do not bail out early here in case request has now expired, as we've marked the tiles as processed (but the processing is up next).
          await ensureFetchedPostTitleLengths(
            edge,
            points.map((p) => p.id),
          );
          for (const p of points) {
            // We still need to check existence in and update lp.points despite processedTiles, because of transferring points between zoom levels.
            if (lp.points.has(p.id)) {
              continue;
            }
            const box = calcLabelBBox(z, vp, p);
            let picked;
            if (lp.tree.collides(box)) {
              // Avoid recalculating over and over again.
              picked = false;
            } else {
              cachedFetchEdgePost(abortController.signal, edge, p.id).then(
                (post) => {
                  postTitles.set(p.id, post?.title ?? "");
                  renderPoints();
                },
              );
              picked = true;
            }
            const e = { point: p, picked };
            // Propagate to all further zoom levels. This is simpler to keep in sync and get correct than trying to pull from previous zoom levels.
            for (let zn = z; zn < labelledPoints.length; zn++) {
              const lpn = labelledPoints[zn];
              if (picked) {
                // We can't just cache and reuse the previous zoom's BBox values for points, because each zoom has different margin pt. sizes.
                lpn.tree.insert(calcLabelBBox(zn, vp, p));
              }
              lpn.points.putIfAbsentOrThrow(p.id, e);
            }
          }
        }
        renderPoints();
        if (req == latestPickLabelledPointsReqId) {
          break;
        }
      }
      pickingLabelledPoints = false;
    })();
  };

  let raf: ReturnType<typeof requestAnimationFrame> | undefined;
  const renderPoints = () => {
    if (raf != undefined) {
      cancelAnimationFrame(raf);
    }
    raf = requestAnimationFrame(() => {
      const vp = curViewport;
      if (!vp) {
        return;
      }
      const ctx = assertExists(canvas.getContext("2d"));
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      const scale = viewportScale(vp);
      const lp = labelledPoints.at(Math.floor(vp.zoom));
      for (const p of curPoints) {
        const canvasX = scale.ptToPx(p.x - vp.x0Pt);
        const canvasY = scale.ptToPx(p.y - vp.y0Pt);
        if (lp?.points.get(p.id)?.picked) {
          const label = postTitles.get(p.id);
          if (label) {
            ctx.font = LABEL_FONT_STYLE;
            ctx.fillStyle = "#333";
            ctx.fillText(label, canvasX, canvasY);
          }
        }
        const MIN_ALPHA = 0.7;
        const alpha =
          ((p.score - map.scoreMin) / map.scoreRange) * (1 - MIN_ALPHA) +
          MIN_ALPHA;
        ctx.fillStyle = `rgba(3, 165, 252, ${alpha})`;
        ctx.beginPath();
        ctx.arc(canvasX, canvasY, 3, 0, Math.PI * 2);
        ctx.fill();
      }
    });
  };

  return {
    destroy: () => {
      abortController.abort();
    },
    // Render the points at LOD `lod` from (ptX0, ptY0) to (ptX1, ptY1) (inclusive) on the canvas.
    render: async (newViewport: ViewportState) => {
      const requestId = ++latestRenderRequestId;

      const lod = map.calcLod(newViewport);
      const scale = viewportScale(newViewport);
      const { x1Pt, y1Pt } = scale.scaled(newViewport);

      curViewport = newViewport;
      // Remain responsive to user interaction by redrawing the map with the current points at the new pan/zoom, even if these won't be the final points. Don't just look up in the tree immediately, since it may be a different level and the points may not be loaded yet, so the map will suddenly go blank.
      pickLabelledPoints();
      renderPoints();

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
      renderPoints();
    },
  };
};
