import { decode } from "@msgpack/msgpack";
import { VFiniteNumber, VInteger, VStruct, Valid } from "@wzlin/valid";
import UnreachableError from "@xtjs/lib/UnreachableError";
import { useEffect, useRef, useState } from "react";
import {
  ZOOM_PER_LOD,
  calcLod,
  createCanvasPointMap,
  mapCalcs,
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

const EDGES = [
  "ap-sydney-1",
  "uk-london-1",
  "us-ashburn-1",
  "us-sanjose-1",
] as const;

export const PointMap = ({
  height: vpHeightPx,
  width: vpWidthPx,
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
        `https://us-ashburn-1.edge-hndr.wilsonl.in/map/hnsw/meta`,
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

  const [vpXPt, setVpXPt] = useState(0);
  const [vpYPt, setVpYPt] = useState(0);
  const [zoom, setZoom] = useState(0);
  const lod = calcLod(lodLevels, zoom);
  const c = mapCalcs(zoom);
  const vpWidthPt = c.pxToPt(vpWidthPx);
  const vpHeightPt = c.pxToPt(vpHeightPx);

  const nextRenderReqId = useRef(0);
  const $canvas = useRef<HTMLCanvasElement>(null);
  const map = useRef<ReturnType<typeof createCanvasPointMap>>();
  useEffect(() => {
    if (!$canvas.current) {
      return;
    }
    map.current ??= createCanvasPointMap();
    map.current.init($canvas.current);
  }, []);
  const ptrPos = useRef<{ clientX: number; clientY: number }>();
  useEffect(() => {
    map.current?.reset(lodLevels);
  }, [lodLevels]);
  useEffect(() => {
    if (!meta) {
      return;
    }
    map.current?.render({
      requestId: nextRenderReqId.current++,
      edge: edge.current,
      lod,
      zoom,

      viewportHeightPx: vpHeightPx,
      viewportWidthPx: vpWidthPx,

      x0Pt: vpXPt,
      x1Pt: vpXPt + vpWidthPt,
      y0Pt: vpYPt,
      y1Pt: vpYPt + vpHeightPt,

      xMaxPt,
      xMinPt,
      yMaxPt,
      yMinPt,
      scoreMin: meta.score_min,
      scoreMax: meta.score_max,
    });
  }, [meta, lod, vpXPt, vpYPt, vpWidthPt, vpHeightPt]);

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
          setVpXPt(vpXPt + dXPt);
          setVpYPt(vpYPt + dYPt);
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
          const nz = mapCalcs(newZoom);

          // Get mouse position relative to element.
          const rect = e.currentTarget.getBoundingClientRect();
          const relX = e.clientX - rect.left;
          const relY = e.clientY - rect.top;
          // The point position of the cursor at the current zoom level.
          const curZoomTgtPosX = vpXPt + c.pxToPt(relX);
          const curZoomTgtPosY = vpYPt + c.pxToPt(relY);
          // How to keep the cursor at the same point after zooming:
          // - If we set the viewport's top-left to the `curZoomTgtPos*`, we'd see the cursor's position at the top left.
          // - Therefore, all we need to do is to then shift by the same amount of absolute pixels back at the new zoom level.
          setVpXPt(curZoomTgtPosX - nz.pxToPt(relX));
          setVpYPt(curZoomTgtPosY - nz.pxToPt(relY));
        }}
      />

      <div className="info">
        <p>
          ({vpXPt.toFixed(2)}, {vpYPt.toFixed(2)})
        </p>
        <p>LOD: {lod == lodLevels - 1 ? "max" : lod}</p>
        <p>Zoom: {zoom.toFixed(2)}</p>
      </div>
    </div>
  );
};
