import { Item } from "@wzlin/crawler-toolkit-hn";
import Dict from "@xtjs/lib/Dict";
import arrayEquals from "@xtjs/lib/arrayEquals";
import assertExists from "@xtjs/lib/assertExists";
import nativeOrdering from "@xtjs/lib/nativeOrdering";
import propertyComparator from "@xtjs/lib/propertyComparator";
import RBush, { BBox } from "rbush";
import { fetchEdgePost } from "./item";

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

export const fetchTile = async (
  signal: AbortSignal,
  edge: string,
  lod: number,
  x: number,
  y: number,
) => {
  const res = await fetch(
    `https://${edge}.edge-hndr.wilsonl.in/map/hnsw/tile/${lod}/${x}-${y}`,
    { signal },
  );
  // Not all tiles exist (i.e. no points exist).
  if (res.status === 404) {
    return [];
  }
  if (!res.ok) {
    throw new Error(`Failed to fetch tile ${x}-${y} with status ${res.status}`);
  }
  const raw = await res.arrayBuffer();
  return parseTileData(raw);
};

export const mapCalcs = (zoom: number) => {
  // This should grow exponentially with zoom, as otherwise the distances between points shrink the more you zoom in, due to min point distances exponentially increasing with each LOD level.
  const pxPerPt = PX_PER_PT_BASE * Math.pow(1 + 1 / ZOOM_PER_LOD, zoom);
  const pxToPt = (px: number) => px / pxPerPt;
  const ptToPx = (pt: number) => pt * pxPerPt;
  return {
    pxPerPt,
    pxToPt,
    ptToPx,
  };
};

export const calcLod = (lodLevels: number, zoom: number) =>
  Math.min(lodLevels - 1, Math.floor(zoom / ZOOM_PER_LOD));

export const createCanvasPointMap = () => {
  let tilesLoadAbortController: AbortController | undefined;
  const tileLoadPromises = new Dict<string, Promise<any>>();
  const itemLoadPromises = new Dict<number, Promise<any>>();
  const items = new Dict<number, Item>();
  const lodTrees = Array<PointTree>(); // One for each LOD level.
  let latestRenderRequestId = -1;
  let canvas: HTMLCanvasElement;
  let curPoints = Array<Point>(); // This must always be sorted by y ascending.
  let raf: ReturnType<typeof requestAnimationFrame> | undefined;
  let curViewport:
    | {
        edge: string;
        lod: number;
        zoom: number;
        scoreMin: number;
        scoreMax: number;
        x0Pt: number;
        y0Pt: number;
      }
    | undefined;
  // We don't want to constantly calculate labelled points, as it causes jarring flashes of text. Instead, we want to keep the same points labelled for a few seconds.
  // The exception is when we're changing LOD level, as that's when many points will enter/exit, so we want to recompute immediately as the intermediate state looks weird.
  const LABEL_FONT_SIZE = 12;
  const MIN_GAP_BETWEEN_LABELS = LABEL_FONT_SIZE * 3;
  const labelledPoints = Array<number>(); // Point IDs, sorted for comparability.
  let lastPickedLabelledPoints = 0;
  let pickLabelledPointsTimeout: ReturnType<typeof setTimeout> | undefined;
  const pickLabelledPoints = () => {
    const delay = Math.max(0, lastPickedLabelledPoints + 750 - Date.now());
    clearTimeout(pickLabelledPointsTimeout);
    pickLabelledPointsTimeout = setTimeout(() => {
      const vp = curViewport;
      if (!vp) {
        return;
      }
      const c = mapCalcs(vp.zoom);
      const prevLabelledPoints = labelledPoints.splice(0);
      const idByRow: Record<number, Point> = {};
      for (const p of curPoints) {
        const canvasY = c.ptToPx(p.y - vp.y0Pt);
        const row = Math.floor(canvasY / MIN_GAP_BETWEEN_LABELS);
        if (!idByRow[row] || idByRow[row].score < p.score) {
          // We mark this as labelled point, even if we don't immediately label (not loaded, blank title, etc.), as otherwise the points that are labelled constantly change as items asynchronously load.
          idByRow[row] = p;
          itemLoadPromises.computeIfAbsent(p.id, async (id) => {
            items.putIfAbsentOrThrow(id, await fetchEdgePost(vp.edge, id));
            renderPoints();
          });
        }
      }
      labelledPoints.push(
        ...Object.values(idByRow)
          .map((p) => p.id)
          .sort(nativeOrdering),
      );
      if (!arrayEquals(prevLabelledPoints, labelledPoints)) {
        renderPoints();
      }
      lastPickedLabelledPoints = Date.now();
    }, delay);
  };

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
      const c = mapCalcs(vp.zoom);
      pickLabelledPoints();
      for (const p of curPoints) {
        const canvasX = c.ptToPx(p.x - vp.x0Pt);
        const canvasY = c.ptToPx(p.y - vp.y0Pt);
        if (labelledPoints.includes(p.id)) {
          const label = items.get(p.id)?.title;
          if (label) {
            ctx.font = `${LABEL_FONT_SIZE}px sans-serif`;
            ctx.fillStyle = "#333";
            ctx.fillText(label, canvasX, canvasY);
          }
        }
        const MIN_ALPHA = 0.7;
        const alpha =
          ((p.score - vp.scoreMin) / (vp.scoreMax - vp.scoreMin + 1)) *
            (1 - MIN_ALPHA) +
          MIN_ALPHA;
        ctx.fillStyle = `rgba(3, 165, 252, ${alpha})`;
        ctx.beginPath();
        ctx.arc(canvasX, canvasY, 3, 0, Math.PI * 2);
        ctx.fill();
      }
    });
  };

  return {
    init: (c: HTMLCanvasElement) => {
      canvas = c;
    },
    reset: (lodLevels: number) => {
      console.warn("Resetting");
      tilesLoadAbortController?.abort();
      tilesLoadAbortController = new AbortController();
      tileLoadPromises.clear();
      lodTrees.splice(0);
      curPoints = [];
      for (let i = 0; i < lodLevels; i++) {
        lodTrees.push(new PointTree());
      }
    },
    // Render the points at LOD `lod` from (ptX0, ptY0) to (ptX1, ptY1) (inclusive) on the canvas.
    render: async (msg: {
      requestId: number;
      edge: string;
      lod: number;
      zoom: number;

      viewportWidthPx: number;
      viewportHeightPx: number;

      x0Pt: number;
      x1Pt: number;
      y0Pt: number;
      y1Pt: number;

      // These represent the whole map.
      xMaxPt: number;
      xMinPt: number;
      yMaxPt: number;
      yMinPt: number;
      scoreMin: number;
      scoreMax: number;
    }) => {
      latestRenderRequestId = msg.requestId;

      const { xMaxPt, xMinPt, yMaxPt, yMinPt } = msg;

      // Since this worker thread has control, we can't set the height/width from the JSX attributes, so we must do it here.
      canvas.width = msg.viewportWidthPx;
      canvas.height = msg.viewportHeightPx;

      const axisTileCount = 2 ** msg.lod;
      const xRangePt = xMaxPt - xMinPt;
      const yRangePt = yMaxPt - yMinPt;
      const tileWidthPt = xRangePt / axisTileCount;
      const tileHeightPt = yRangePt / axisTileCount;

      const tileXMin = Math.max(
        0,
        Math.floor((msg.x0Pt - xMinPt) / tileWidthPt),
      );
      const tileXMax = Math.min(
        axisTileCount - 1,
        Math.floor((msg.x1Pt - xMinPt) / tileWidthPt),
      );
      const tileYMin = Math.max(
        0,
        Math.floor((msg.y0Pt - yMinPt) / tileHeightPt),
      );
      const tileYMax = Math.min(
        axisTileCount - 1,
        Math.floor((msg.y1Pt - yMinPt) / tileHeightPt),
      );

      const lodChanged = curViewport?.lod !== msg.lod;
      curViewport = {
        edge: msg.edge,
        lod: msg.lod,
        scoreMax: msg.scoreMax,
        scoreMin: msg.scoreMin,
        x0Pt: msg.x0Pt,
        y0Pt: msg.y0Pt,
        zoom: msg.zoom,
      };
      // Remain responsive to user interaction by redrawing the map with the current points at the new pan/zoom, even if these won't be the final points. Don't just look up in the tree immediately, since it may be a different level and the points may not be loaded yet, so the map will suddenly go blank.
      if (lodChanged) {
        lastPickedLabelledPoints = 0;
      }
      renderPoints();

      // Ensure all requested tiles are fetched.
      await Promise.all(
        (function* () {
          for (let x = tileXMin; x <= tileXMax; x++) {
            for (let y = tileYMin; y <= tileYMax; y++) {
              const key = `${msg.lod}-${x}-${y}`;
              // We must save the Promise as we still want to await them on subsequent render calls even if they're not the ones to initialize the request. (Otherwise, we will not have awaited on all tiles that need to be loaded, and the map will be partially blank.)
              yield tileLoadPromises.computeIfAbsent(key, async () => {
                const points = await fetchTile(
                  tilesLoadAbortController!.signal,
                  msg.edge,
                  msg.lod,
                  x,
                  y,
                );
                lodTrees[msg.lod].load(points);
              });
            }
          }
        })(),
      );
      if (msg.requestId !== latestRenderRequestId) {
        // This render request is now outdated.
        return;
      }
      // Render the final points.
      curPoints = lodTrees[msg.lod]
        .search({
          minX: msg.x0Pt,
          maxX: msg.x1Pt,
          minY: msg.y0Pt,
          maxY: msg.y1Pt,
        })
        .sort(propertyComparator("y"));
      renderPoints();
    },
  };
};
