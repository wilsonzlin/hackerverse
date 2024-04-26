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

const postLengths = new Dict<number, Promise<number>>();
export const cachedFetchPostTitleLengths = async (
  edge: string,
  ids: number[],
) => {
  const out: Record<number, number> = {};
  const existing = Array<Promise<unknown>>();
  const missing = Array<number>();
  for (const id of ids) {
    const ex = postLengths.get(id);
    if (ex) {
      existing.push(ex.then((l) => (out[id] = l)));
    } else {
      missing.push(id);
    }
  }
  const f = fetch(`https://${edge}.edge-hndr.wilsonl.in/post-title-lengths`, {
    method: "POST",
    body: new Uint32Array(missing),
  })
    .then((res) => res.arrayBuffer())
    .then((rawBuf) => {
      const raw = new Uint8Array(rawBuf);
      assertState(raw.length === missing.length);
      for (const [i, id] of missing.entries()) {
        out[id] = raw[i];
      }
      return raw;
    });
  for (const [i, id] of missing.entries()) {
    postLengths.set(
      id,
      f.then((raw) => raw[i]),
    );
  }
  await Promise.all([...existing, f]);
  return out;
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
  const LABEL_FONT_SIZE = 12;
  const LABEL_FONT_STYLE = `${LABEL_FONT_SIZE}px sans-serif`;
  const LABEL_MARGIN = 12;
  // One for each integer zoom level.
  const labelledPoints = Array<{
    idToBbox: Dict<number, BBox>;
    skipped: Set<number>;
    tree: RBush<BBox>;
  }>();
  const getLabelledPointsForZoom = (zoom: number) => {
    let prev;
    for (let i = 0; i <= zoom; i++) {
      if (!labelledPoints[i]) {
        const idToBbox = new Dict<number, BBox>();
        const tree = new RBush<BBox>();
        for (const [id, bbox] of prev?.idToBbox ?? []) {
          idToBbox.set(id, bbox);
          tree.insert(bbox);
        }
        labelledPoints[i] = {
          idToBbox,
          skipped: new Set(prev?.skipped),
          tree,
        };
      }
      prev = labelledPoints[i];
    }
    return assertExists(prev);
  };
  let latestPickLabelledPointsReqId = 0;
  let pickingLabelledPoints = false;
  const pickLabelledPoints = () => {
    if (pickingLabelledPoints) {
      latestPickLabelledPointsReqId++;
      return;
    }
    pickingLabelledPoints = true;
    (async () => {
      outer: while (true) {
        const req = latestPickLabelledPointsReqId;
        const vp = assertExists(curViewport);
        for (let z = 0; z <= vp.zoom; z++) {
          const lod = map.calcLod(z);
          const scale = viewportScale(z);
          const { tileXMax, tileXMin, tileYMax, tileYMin } = map.viewportTiles({
            ...vp,
            zoom: z,
          });
          const points = await Promise.all(
            (function* () {
              for (let x = tileXMin; x <= tileXMax; x++) {
                for (let y = tileYMin; y <= tileYMax; y++) {
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
          // Bail out early if the request has expired since our await.
          if (req != latestPickLabelledPointsReqId) {
            continue outer;
          }
          const titleLengths = await cachedFetchPostTitleLengths(
            edge,
            points.map((p) => p.id),
          );
          const lp = getLabelledPointsForZoom(z);
          for (const p of points) {
            const titleLen = titleLengths[p.id];
            if (lp.idToBbox.has(p.id) || lp.skipped.has(p.id)) {
              continue;
            }
            const canvasX = scale.ptToPx(p.x - vp.x0Pt);
            const canvasY = scale.ptToPx(p.y - vp.y0Pt);
            const box: BBox = {
              minX: scale.pxToPt(canvasX - LABEL_MARGIN),
              // Guessed approximate width based on title length.
              maxX: scale.pxToPt(
                canvasX + (titleLen * LABEL_FONT_SIZE) / 1.6 + LABEL_MARGIN,
              ),
              minY: scale.pxToPt(canvasY - LABEL_MARGIN),
              maxY: scale.pxToPt(canvasY + LABEL_FONT_SIZE + LABEL_MARGIN),
            };
            if (lp.tree.collides(box)) {
              // Avoid recalculating over and over again.
              lp.skipped.add(p.id);
            } else {
              lp.tree.insert(box);
              lp.idToBbox.putIfAbsentOrThrow(p.id, box);
              cachedFetchEdgePost(abortController.signal, edge, p.id).then(
                (post) => {
                  postTitles.set(p.id, post?.title ?? "");
                  renderPoints();
                },
              );
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
        if (lp?.idToBbox.has(p.id)) {
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
