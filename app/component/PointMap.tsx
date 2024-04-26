import { decode } from "@msgpack/msgpack";
import { VFiniteNumber, VInteger, VStruct } from "@wzlin/valid";
import UnreachableError from "@xtjs/lib/UnreachableError";
import assertExists from "@xtjs/lib/assertExists";
import { useEffect, useRef, useState } from "react";
import {
  MapState,
  ZOOM_PER_LOD,
  createCanvasPointMap,
  viewportScale,
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
  const $canvas = useRef<HTMLCanvasElement>(null);
  const [map, setMap] = useState<ReturnType<typeof createCanvasPointMap>>();
  const [meta, setMeta] = useState<MapState>();
  useEffect(() => {
    const ac = new AbortController();

    // Use Promise.race so we don't wait for the slow ones.
    const findClosestEdge = () =>
      Promise.race(
        EDGES.map(async (edge) => {
          // Run a few times to avoid potential cold start biases.
          for (let i = 0; i < 3; i++) {
            await fetch(`https://${edge}.edge-hndr.wilsonl.in/healthz`, {
              signal: ac.signal,
            });
          }
          return edge;
        }),
      );

    const fetchMeta = async () => {
      const res = await fetch(
        `https://us-ashburn-1.edge-hndr.wilsonl.in/map/hnsw/meta`,
        { signal: ac.signal },
      );
      const raw = await res.arrayBuffer();
      const meta = vMapMeta.parseRoot(decode(raw));
      return new MapState({
        lodLevels: meta.lod_levels,
        scoreMax: meta.score_max,
        scoreMin: meta.score_min,
        xMaxPt: meta.x_max,
        xMinPt: meta.x_min,
        yMaxPt: meta.y_max,
        yMinPt: meta.y_min,
      });
    };

    (async () => {
      const [edge, meta] = await Promise.all([findClosestEdge(), fetchMeta()]);
      console.log("Closest edge:", edge);
      console.log("Map metadata:", meta);
      setMeta(meta);
      const map = createCanvasPointMap({
        canvas: assertExists($canvas.current),
        edge,
        map: meta,
      });
      setMap(map);
    })();

    return () => ac.abort();
  }, []);

  const [vpXPt, setVpXPt] = useState(0);
  const [vpYPt, setVpYPt] = useState(0);
  const [zoom, setZoom] = useState(0);
  const lod = meta?.calcLod(zoom) ?? 0;
  const scale = viewportScale(zoom);

  const ptrPos = useRef<{ clientX: number; clientY: number }>();
  useEffect(() => {
    map?.render({
      heightPx: vpHeightPx,
      widthPx: vpWidthPx,
      x0Pt: vpXPt,
      y0Pt: vpYPt,
      zoom,
    });
  }, [map, vpHeightPx, vpWidthPx, vpXPt, vpYPt, zoom]);

  return (
    <div className="PointMap">
      <canvas
        ref={$canvas}
        className="canvas"
        width={vpWidthPx}
        height={vpHeightPx}
        onPointerDown={(e) => {
          e.currentTarget.setPointerCapture(e.pointerId);
          ptrPos.current = e;
        }}
        onPointerMove={(e) => {
          if (!ptrPos.current || !scale) {
            return;
          }
          e.preventDefault();
          const dXPt = scale.pxToPt(ptrPos.current.clientX - e.clientX);
          const dYPt = scale.pxToPt(ptrPos.current.clientY - e.clientY);
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
          if (!scale) {
            return;
          }
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
          const nz = viewportScale(newZoom);

          // Get mouse position relative to element.
          const rect = e.currentTarget.getBoundingClientRect();
          const relX = e.clientX - rect.left;
          const relY = e.clientY - rect.top;
          // The point position of the cursor at the current zoom level.
          const curZoomTgtPosX = vpXPt + scale.pxToPt(relX);
          const curZoomTgtPosY = vpYPt + scale.pxToPt(relY);
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
        <p>LOD: {lod == (meta?.lodLevels ?? 0) - 1 ? "max" : lod}</p>
        <p>Zoom: {zoom.toFixed(2)}</p>
      </div>
    </div>
  );
};
