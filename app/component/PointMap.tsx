import { VFiniteNumber, VInteger, VStruct, Valid } from "@wzlin/valid";
import UnreachableError from "@xtjs/lib/UnreachableError";
import assertExists from "@xtjs/lib/assertExists";
import { useEffect, useRef, useState } from "react";
import { useMeasure } from "../util/dom";
import "./PointMap.css";

const MAP_WIDTH_BASE = 1600;
const MAP_HEIGHT_BASE = 1600;
const LOD_LEVELS = 8;
const ZOOM_PER_LOD = 3;
const BASE_AXIS_TILES = 1;

type Point = { id: number; x: number; y: number; score: number };

const vMapMeta = new VStruct({
  x_min: new VFiniteNumber(),
  x_max: new VFiniteNumber(),
  y_min: new VFiniteNumber(),
  y_max: new VFiniteNumber(),
  count: new VInteger(0),
});

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
  zoomLevel: number,
  x: number,
  y: number,
) => {
  const res = await fetch(
    `http://${location.hostname}:8080/map/z${zoomLevel}/${x}-${y}.bin`,
    { signal },
  );
  const raw = await res.arrayBuffer();
  return parseData(raw);
};

const fetchWhole = async (signal: AbortSignal, zoomLevel: number) => {
  const res = await fetch(
    `http://${location.hostname}:8080/map_z${zoomLevel}.bin`,
    { signal },
  );
  const raw = await res.arrayBuffer();
  return parseData(raw);
};

const zoomCalcs = ({
  xMaxPt,
  xMinPt,
  yMaxPt,
  yMinPt,
  zoom,
}: {
  xMaxPt: number;
  xMinPt: number;
  yMaxPt: number;
  yMinPt: number;
  zoom: number;
}) => {
  const canvasWidth = MAP_WIDTH_BASE * zoom;
  const canvasHeight = MAP_HEIGHT_BASE * zoom;
  const xRangePt = xMaxPt - xMinPt;
  const yRangePt = yMaxPt - yMinPt;
  const pxPerXPt = canvasWidth / xRangePt;
  const pxPerYPt = canvasHeight / yRangePt;
  const xPxToPt = (px: number) => px / pxPerXPt;
  const yPxToPt = (px: number) => px / pxPerYPt;
  const xPtToPx = (pt: number) => pt * pxPerXPt;
  const yPtToPx = (pt: number) => pt * pxPerYPt;
  const xPtToCanvasPos = (pt: number) => xPtToPx(pt - xMinPt);
  const yPtToCanvasPos = (pt: number) => yPtToPx(pt - yMinPt);
  return {
    canvasHeight,
    canvasWidth,
    pxPerXPt,
    pxPerYPt,
    xRangePt,
    yRangePt,
    xPxToPt,
    yPxToPt,
    xPtToPx,
    yPtToPx,
    xPtToCanvasPos,
    yPtToCanvasPos,
  };
};

export const PointMap = () => {
  const $window = useRef<HTMLDivElement>(null);
  const wdwRect = useMeasure($window.current);

  const [meta, setMeta] = useState<Valid<typeof vMapMeta>>();
  const xMaxPt = meta?.x_max ?? 0;
  const xMinPt = meta?.x_min ?? 0;
  const yMaxPt = meta?.y_max ?? 0;
  const yMinPt = meta?.y_min ?? 0;
  useEffect(() => {
    (async () => {
      setMeta(
        vMapMeta.parseRoot(
          await fetch(`http://${location.hostname}:8080/map.json`).then((res) =>
            res.json(),
          ),
        ),
      );
    })();
  }, []);

  const [wdwXPt, setWdwXPt] = useState(0);
  const [wdwYPt, setWdwYPt] = useState(0);
  const [zoom, setZoom] = useState(1);
  const z = zoomCalcs({
    xMaxPt,
    xMinPt,
    yMaxPt,
    yMinPt,
    zoom,
  });
  const lod = Math.min(LOD_LEVELS - 1, Math.floor((zoom - 1) / ZOOM_PER_LOD));
  const wdwWidthPt = z.xPxToPt(wdwRect?.width ?? 0);
  const wdwHeightPt = z.xPxToPt(wdwRect?.height ?? 0);
  const axisTileCount = BASE_AXIS_TILES * 2 ** lod;
  const tileWidthPt = z.xRangePt / axisTileCount;
  const tileHeightPt = z.yRangePt / axisTileCount;

  const ptrPos = useRef<{ clientX: number; clientY: number }>();
  const tilesLoaded = useRef<{
    abortController: AbortController;
    tiles: Set<string>;
  }>();
  const [points, setPoints] = useState(Array<Point>());
  useEffect(() => {
    console.log("LOD changed to", lod);
    tilesLoaded.current?.abortController.abort();
    tilesLoaded.current = {
      abortController: new AbortController(),
      tiles: new Set(),
    };
    // TODO Clearing points is important:
    // - When zooming out, we need to reset the density of the map.
    // - When zooming in, we need to evict out-of-bounds and duplicate points which causes lag when redrawing.
    // However, we should try to avoid clearing points when first zooming in, to not lose the existing points while loading the next LOD, which is distracting.
    setPoints([]);
  }, [lod]);
  useEffect(() => {
    const tl = assertExists(tilesLoaded.current);
    const signal = tl.abortController.signal;
    const tileXMin = Math.floor((wdwXPt - xMinPt) / tileWidthPt);
    const tileXMax = Math.floor((wdwXPt - xMinPt + wdwWidthPt) / tileWidthPt);
    const tileYMin = Math.floor((wdwYPt - yMinPt) / tileHeightPt);
    const tileYMax = Math.floor((wdwYPt - yMinPt + wdwHeightPt) / tileHeightPt);
    const promises = [];
    for (let x = tileXMin; x <= tileXMax; x++) {
      for (let y = tileYMin; y <= tileYMax; y++) {
        const key = `${x}-${y}`;
        if (tl.tiles.has(key)) {
          continue;
        }
        tl.tiles.add(key);
        promises.push(
          (async () => {
            const points = await fetchTile(signal, lod, x, y);
            console.log("Fetched tile", x, y, "with", points.length, "points");
            setPoints((p) => [...p, ...points]);
          })(),
        );
      }
    }
  }, [lod, wdwXPt, wdwYPt, wdwWidthPt, wdwHeightPt]);

  const $canvas = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    if (!$canvas.current) {
      return;
    }
    console.log("Redrawing all", points.length, "points on canvas");
    const ctx = assertExists($canvas.current.getContext("2d"));
    ctx.clearRect(0, 0, z.canvasWidth, z.canvasHeight);
    for (const point of points) {
      ctx.fillStyle = `rgba(0, 0, 0, 1)`;
      ctx.beginPath();
      ctx.arc(
        z.xPtToCanvasPos(point.x),
        z.yPtToCanvasPos(point.y),
        2,
        0,
        Math.PI * 2,
      );
      ctx.fill();
    }
  }, [points, zoom]);

  return (
    <div className="PointMap">
      <div className="controls flex-row">
        <p>
          ({wdwXPt.toFixed(2)}, {wdwYPt.toFixed(2)})
        </p>
        <div className="grow" />
        <p>LOD: {lod}</p>
        <div className="grow" />
        <p>Zoom: {zoom.toFixed(2)}</p>
      </div>
      <div
        ref={$window}
        className="window"
        onPointerDown={(e) => {
          e.currentTarget.setPointerCapture(e.pointerId);
          ptrPos.current = e;
        }}
        onPointerMove={(e) => {
          if (!ptrPos.current) {
            return;
          }
          e.preventDefault();
          const dXPt = z.xPxToPt(ptrPos.current.clientX - e.clientX);
          const dYPt = z.yPxToPt(ptrPos.current.clientY - e.clientY);
          ptrPos.current = e;
          setWdwXPt(wdwXPt + dXPt);
          setWdwYPt(wdwYPt + dYPt);
        }}
        onPointerCancel={() => {
          ptrPos.current = undefined;
        }}
        onPointerUp={() => {
          ptrPos.current = undefined;
        }}
        onWheel={(e) => {
          let delta;
          switch (e.deltaMode) {
            case WheelEvent.DOM_DELTA_PIXEL:
              delta = e.deltaY / 250;
              break;
            case WheelEvent.DOM_DELTA_LINE:
              delta = e.deltaY;
              break;
            case WheelEvent.DOM_DELTA_PAGE:
              delta = e.deltaY * ZOOM_PER_LOD;
              break;
            default:
              throw new UnreachableError();
          }
          const newZoom = Math.max(zoom - delta, 1);
          setZoom(newZoom);
          const nz = zoomCalcs({
            xMaxPt,
            xMinPt,
            yMaxPt,
            yMinPt,
            zoom: newZoom,
          });

          // Get mouse position relative to element.
          const rect = e.currentTarget.getBoundingClientRect();
          const relX = e.clientX - rect.left;
          const relY = e.clientY - rect.top;
          // The point position of the cursor at the current zoom level.
          const curZoomTgtPosX = wdwXPt + z.xPxToPt(relX);
          const curZoomTgtPosY = wdwYPt + z.yPxToPt(relY);
          // How to keep the cursor at the same point after zooming:
          // - If we set the window top-left to the `curZoomTgtPos*`, we'd see the cursor's position at the top left.
          // - Therefore, all we need to do is to then shift by the same amount of absolute pixels back at the new zoom level.
          setWdwXPt(curZoomTgtPosX - nz.xPxToPt(relX));
          setWdwYPt(curZoomTgtPosY - nz.yPxToPt(relY));
        }}
      >
        <canvas
          ref={$canvas}
          className="canvas"
          width={z.canvasWidth}
          height={z.canvasHeight}
          style={{
            top: `${-z.yPtToCanvasPos(wdwYPt)}px`,
            left: `${-z.xPtToCanvasPos(wdwXPt)}px`,
          }}
        />
      </div>
    </div>
  );
};
