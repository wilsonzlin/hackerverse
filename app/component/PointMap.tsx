import { decode } from "@msgpack/msgpack";
import { VFiniteNumber, VInteger, VStruct, Valid } from "@wzlin/valid";
import UnreachableError from "@xtjs/lib/UnreachableError";
import { useEffect, useRef, useState } from "react";
import {
  ZOOM_PER_LOD,
  calcLod,
  mapCalcs,
  vWorkerPointMapMessageToWorker,
} from "../util/map";
import "./PointMap.css";

const vMapMeta = new VStruct({
  x_min: new VFiniteNumber(),
  x_max: new VFiniteNumber(),
  y_min: new VFiniteNumber(),
  y_max: new VFiniteNumber(),
  score_min: new VInteger(),
  score_max: new VInteger(),
  count: new VInteger(0),
  lod_levels: new VInteger(0),
});

const worker = new Worker("/dist/worker.PointMap.js");

const EDGES = [
  "ap-sydney-1",
  "uk-london-1",
  "us-ashburn-1",
  "us-sanjose-1",
] as const;

export const PointMap = ({
  height: wdwHeightPx,
  width: wdwWidthPx,
}: {
  height: number;
  width: number;
}) => {
  const [meta, setMeta] = useState<Valid<typeof vMapMeta>>();
  const xMaxPt = meta?.x_max ?? 0;
  const xMinPt = meta?.x_min ?? 0;
  const yMaxPt = meta?.y_max ?? 0;
  const yMinPt = meta?.y_min ?? 0;
  const lodLevels = meta?.lod_levels ?? 1;
  useEffect(() => {
    const ac = new AbortController();
    (async () => {
      const meta = await fetch(
        `https://us-ashburn-1.edge-hndr.wilsonl.in/hnsw/map/meta`,
        { signal: ac.signal },
      )
        .then((res) => res.arrayBuffer())
        .then((res) => vMapMeta.parseRoot(decode(res)));
      setMeta(meta);
    })();
    return () => ac.abort();
  }, []);

  const edge = useRef("us-ashburn-1");
  useEffect(() => {
    const ac = new AbortController();
    (async () => {
      const ITERATIONS = 3;
      // Use Promise.race so we don't wait for the slow ones.
      edge.current = await Promise.race(
        EDGES.map(async (edge) => {
          // Run a few times to avoid potential cold start biases.
          for (let i = 0; i < ITERATIONS; i++) {
            await fetch(`https://${edge}.edge-hndr.wilsonl.in/healthz`, {
              signal: ac.signal,
            });
          }
          return edge;
        }),
      );
      console.log("Closest edge:", edge.current);
      ac.abort();
    })();
    return () => ac.abort();
  }, []);

  const [wdwXPt, setWdwXPt] = useState(0);
  const [wdwYPt, setWdwYPt] = useState(0);
  const [zoom, setZoom] = useState(0);
  const lod = calcLod(lodLevels, zoom);
  const c = mapCalcs({
    zoom,
  });
  const wdwWidthPt = c.pxToPt(wdwWidthPx);
  const wdwHeightPt = c.pxToPt(wdwHeightPx);

  const nextRenderReqId = useRef(0);
  const $canvas = useRef<HTMLCanvasElement>(null);
  // The OffscreenCanvas must only be sent once: https://stackoverflow.com/a/57762984.
  useEffect(() => {
    if (!$canvas.current) {
      return;
    }
    const offscreen = $canvas.current.transferControlToOffscreen();
    const msg: Valid<typeof vWorkerPointMapMessageToWorker> = {
      $type: "init",
      canvas: offscreen,
    };
    worker.postMessage(msg, [offscreen]);
  }, []);
  const ptrPos = useRef<{ clientX: number; clientY: number }>();
  useEffect(() => {
    const msg: Valid<typeof vWorkerPointMapMessageToWorker> = {
      $type: "reset",
      lodLevels,
    };
    worker.postMessage(msg);
  }, [lodLevels]);
  useEffect(() => {
    if (!meta) {
      return;
    }
    const msg: Valid<typeof vWorkerPointMapMessageToWorker> = {
      $type: "render",
      requestId: nextRenderReqId.current++,
      edge: edge.current,
      lod,
      zoom,

      windowHeightPx: wdwHeightPx,
      windowWidthPx: wdwWidthPx,

      x0Pt: wdwXPt,
      x1Pt: wdwXPt + wdwWidthPt,
      y0Pt: wdwYPt,
      y1Pt: wdwYPt + wdwHeightPt,

      xMaxPt,
      xMinPt,
      yMaxPt,
      yMinPt,
      scoreMin: meta.score_min,
      scoreMax: meta.score_max,
    };
    worker.postMessage(msg, []);
  }, [meta, lod, wdwXPt, wdwYPt, wdwWidthPt, wdwHeightPt]);

  return (
    <div className="PointMap">
      <canvas
        ref={$canvas}
        className="canvas"
        onPointerDown={(e) => {
          e.currentTarget.setPointerCapture(e.pointerId);
          ptrPos.current = e;
        }}
        onPointerMove={(e) => {
          if (!ptrPos.current) {
            return;
          }
          e.preventDefault();
          const dXPt = c.pxToPt(ptrPos.current.clientX - e.clientX);
          const dYPt = c.pxToPt(ptrPos.current.clientY - e.clientY);
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
          const newZoom = Math.max(zoom - delta, 0);
          setZoom(newZoom);
          const nz = mapCalcs({
            zoom: newZoom,
          });

          // Get mouse position relative to element.
          const rect = e.currentTarget.getBoundingClientRect();
          const relX = e.clientX - rect.left;
          const relY = e.clientY - rect.top;
          // The point position of the cursor at the current zoom level.
          const curZoomTgtPosX = wdwXPt + c.pxToPt(relX);
          const curZoomTgtPosY = wdwYPt + c.pxToPt(relY);
          // How to keep the cursor at the same point after zooming:
          // - If we set the window top-left to the `curZoomTgtPos*`, we'd see the cursor's position at the top left.
          // - Therefore, all we need to do is to then shift by the same amount of absolute pixels back at the new zoom level.
          setWdwXPt(curZoomTgtPosX - nz.pxToPt(relX));
          setWdwYPt(curZoomTgtPosY - nz.pxToPt(relY));
        }}
      />

      <div className="info">
        <p>
          ({wdwXPt.toFixed(2)}, {wdwYPt.toFixed(2)})
        </p>
        <p>LOD: {lod == lodLevels - 1 ? "max" : lod}</p>
        <p>Zoom: {zoom.toFixed(2)}</p>
      </div>
    </div>
  );
};
