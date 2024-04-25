import Dict from "@xtjs/lib/Dict";
import assertExists from "@xtjs/lib/assertExists";
import { mapCalcs, vWorkerPointMapMessageToWorker } from "./util/map";

type Point = { id: number; x: number; y: number; score: number };

const parseData = (raw: ArrayBuffer) => {
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

const fetchTile = async (
  signal: AbortSignal,
  lod: number,
  x: number,
  y: number,
) => {
  const res = await fetch(
    `https://ap-sydney-1.edge-hndr.wilsonl.in/hnsw/map/${lod}/${x}-${y}`,
    { signal },
  );
  if (!res.ok) {
    throw new Error(`Failed to fetch tile ${x}-${y} with status ${res.status}`);
  }
  const raw = await res.arrayBuffer();
  return parseData(raw);
};

let tilesLoadingAbortController = new AbortController();
const tiles = new Dict<string, Promise<Point[]>>();
let curPoints = Array<Point>();
let latestRenderRequestId = -1;
let canvas: OffscreenCanvas;

const renderPoints = (msg: {
  zoom: number;
  scoreMin: number;
  scoreMax: number;
  x0Pt: number;
  x1Pt: number;
  y0Pt: number;
  y1Pt: number;
}) => {
  const ctx = assertExists(canvas.getContext("2d"));
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  const c = mapCalcs({ zoom: msg.zoom });
  for (const p of curPoints) {
    if (p.x < msg.x0Pt || p.x > msg.x1Pt || p.y < msg.y0Pt || p.y > msg.y1Pt) {
      continue;
    }
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

addEventListener("message", async (e) => {
  const msg = vWorkerPointMapMessageToWorker.parseRoot(e.data);
  if (msg.$type === "init") {
    canvas = msg.canvas;
  } else if (msg.$type === "reset") {
    tilesLoadingAbortController.abort();
    tilesLoadingAbortController = new AbortController();
    tiles.clear();
  } else if (msg.$type === "render") {
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

    const tileXMin = Math.max(0, Math.floor((msg.x0Pt - xMinPt) / tileWidthPt));
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

    // Remain responsive to user interaction by redrawing the map with the current points at the new pan/zoom, even if these won't be the final points.
    renderPoints(msg);

    // Ensure all requested tiles are fetched.
    const signal = tilesLoadingAbortController.signal;
    const newPoints = await Promise.all(
      (function* () {
        for (let x = tileXMin; x <= tileXMax; x++) {
          for (let y = tileYMin; y <= tileYMax; y++) {
            const key = `${x}-${y}`;
            yield tiles.computeIfAbsent(key, async () => {
              console.log("Fetching tile", x, y);
              const points = await fetchTile(signal, msg.lod, x, y);
              console.log(
                "Fetched tile",
                x,
                y,
                "with",
                points.length,
                "points",
              );
              return points;
            });
          }
        }
      })(),
    ).then((r) => r.flat());
    if (msg.requestId !== latestRenderRequestId) {
      // This render request is now outdated.
      return;
    }
    // Only assign AFTER verifying latestRenderRequestId, otherwise multiple requests could write to `curPoints` out of order.
    curPoints = newPoints;
    // Render the final points.
    renderPoints(msg);
  }
});
