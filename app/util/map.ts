import Dict from "@xtjs/lib/Dict";
import assertExists from "@xtjs/lib/assertExists";
import RBush, { BBox } from "rbush";

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
    `https://${edge}.edge-hndr.wilsonl.in/hnsw/map/${lod}/${x}-${y}`,
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
  const tilesLoadPromises = new Dict<string, Promise<any>>();
  const lodTrees = Array<PointTree>(); // One for each LOD level.
  let latestRenderRequestId = -1;
  let canvas: HTMLCanvasElement;
  let curPoints = Array<Point>();

  const renderPoints = (msg: {
    zoom: number;
    scoreMin: number;
    scoreMax: number;
    x0Pt: number;
    y0Pt: number;
  }) => {
    const ctx = assertExists(canvas.getContext("2d"));
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    const c = mapCalcs(msg.zoom);
    for (const p of curPoints) {
      const MIN_ALPHA = 0.7;
      const alpha =
        ((p.score - msg.scoreMin) / (msg.scoreMax - msg.scoreMin + 1)) *
          (1 - MIN_ALPHA) +
        MIN_ALPHA;
      ctx.fillStyle = `rgba(3, 165, 252, ${alpha})`;
      ctx.beginPath();
      ctx.arc(
        c.ptToPx(p.x - msg.x0Pt),
        c.ptToPx(p.y - msg.y0Pt),
        3,
        0,
        Math.PI * 2,
      );
      ctx.fill();
    }
  };

  return {
    init: (c: HTMLCanvasElement) => {
      canvas = c;
    },
    reset: (lodLevels: number) => {
      console.warn("Resetting");
      tilesLoadAbortController?.abort();
      tilesLoadAbortController = new AbortController();
      tilesLoadPromises.clear();
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

      windowWidthPx: number;
      windowHeightPx: number;

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
      canvas.width = msg.windowWidthPx;
      canvas.height = msg.windowHeightPx;

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

      // Remain responsive to user interaction by redrawing the map with the current points at the new pan/zoom, even if these won't be the final points. Don't just look up in the tree immediately, since it may be a different level and the points may not be loaded yet, so the map will suddenly go blank.
      renderPoints(msg);

      // Ensure all requested tiles are fetched.
      await Promise.all(
        (function* () {
          for (let x = tileXMin; x <= tileXMax; x++) {
            for (let y = tileYMin; y <= tileYMax; y++) {
              const key = `${msg.lod}-${x}-${y}`;
              // We must save the Promise as we still want to await them on subsequent render calls even if they're not the ones to initialize the request. (Otherwise, we will not have awaited on all tiles that need to be loaded, and the map will be partially blank.)
              yield tilesLoadPromises.computeIfAbsent(key, async () => {
                const points = await fetchTile(
                  tilesLoadAbortController!.signal,
                  msg.edge,
                  msg.lod,
                  x,
                  y,
                );
                console.log(
                  "Fetched LOD",
                  msg.lod,
                  "tile",
                  x,
                  y,
                  "with",
                  points.length,
                  "points",
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
      curPoints = lodTrees[msg.lod].search({
        minX: msg.x0Pt,
        maxX: msg.x1Pt,
        minY: msg.y0Pt,
        maxY: msg.y1Pt,
      });
      renderPoints(msg);
    },
  };
};
