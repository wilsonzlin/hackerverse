import { decode } from "@msgpack/msgpack";
import { VFiniteNumber, VInteger, VStruct } from "@wzlin/valid";
import Dict from "@xtjs/lib/Dict";
import UnreachableError from "@xtjs/lib/UnreachableError";
import assertExists from "@xtjs/lib/assertExists";
import bounded from "@xtjs/lib/bounded";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  MAP_DATASET,
  MapState,
  ZOOM_PER_LOD,
  createCanvasPointMap,
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
  heatmaps,
  height: vpHeightPx,
  width: vpWidthPx,
}: {
  heatmaps: ImageBitmap[];
  height: number;
  width: number;
}) => {
  const $canvas = useRef<HTMLCanvasElement>(null);
  const [map, setMap] = useState<ReturnType<typeof createCanvasPointMap>>();
  useEffect(() => map?.setHeatmaps(heatmaps), [map, heatmaps]);
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
        `https://us-ashburn-1.edge-hndr.wilsonl.in/map/${MAP_DATASET}/meta`,
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
      setVpCtrXPt(meta.xMinPt + meta.xRangePt / 2);
      setVpCtrYPt(meta.yMinPt + meta.yRangePt / 2);
      const map = createCanvasPointMap({
        canvas: assertExists($canvas.current),
        edge,
        map: meta,
      });
      setMap(map);
    })();

    return () => ac.abort();
  }, []);

  const [vpCtrXPt, setVpCtrXPt] = useState(0);
  const [vpCtrYPt, setVpCtrYPt] = useState(0);
  const [zoom, setZoom] = useState(0);
  const boundZoom = (zoom: number) => bounded(zoom, 0, meta?.zoomMax ?? 0);
  const lod = useMemo(() => meta?.calcLod(zoom) ?? 0, [meta, zoom]);
  const scale = useMemo(
    () =>
      meta?.viewportScale({
        heightPx: vpHeightPx,
        widthPx: vpWidthPx,
        zoom,
      }),
    [meta, vpHeightPx, vpWidthPx, zoom],
  );
  const vp = useMemo(() => {
    if (!scale) {
      return;
    }
    return {
      heightPx: vpHeightPx,
      widthPx: vpWidthPx,
      x0Pt: vpCtrXPt - scale.pxToPt(vpWidthPx) / 2,
      y0Pt: vpCtrYPt - scale.pxToPt(vpHeightPx) / 2,
      zoom,
    };
  }, [scale, vpHeightPx, vpWidthPx, vpCtrXPt, vpCtrYPt, zoom]);
  useEffect(() => vp && void map?.render(vp), [map, vp]);

  const ptrsRef = useRef(
    new Dict<
      number,
      {
        start: PointerEvent;
        prev: PointerEvent;
        cur: PointerEvent;
      }
    >(),
  );
  const ptrs = ptrsRef.current;

  return (
    <div className="PointMap">
      <canvas
        ref={$canvas}
        className="canvas"
        width={vpWidthPx}
        height={vpHeightPx}
        onPointerDown={(e) => {
          e.currentTarget.setPointerCapture(e.pointerId);
          ptrs.putIfAbsentOrThrow(e.pointerId, {
            start: e.nativeEvent,
            prev: e.nativeEvent,
            cur: e.nativeEvent,
          });
        }}
        onPointerMove={(e) => {
          // May not exist (pointer started elsewhere, moved to over this element).
          const ptr = ptrs.get(e.pointerId);
          if (!ptr) {
            return;
          }
          if (!scale) {
            return;
          }
          e.preventDefault();
          ptr.prev = ptr.cur;
          ptr.cur = e.nativeEvent;
          if (ptrs.size == 1) {
            // Pan.
            const dXPt = scale.pxToPt(ptr.prev.clientX - ptr.cur.clientX);
            const dYPt = scale.pxToPt(ptr.prev.clientY - ptr.cur.clientY);
            setVpCtrXPt(vpCtrXPt + dXPt);
            setVpCtrYPt(vpCtrYPt + dYPt);
          } else if (ptrs.size == 2) {
            // Pinch to zoom.
            const [a, b] = ptrs.values();
            const prevDist = Math.hypot(
              a.prev.clientX - b.prev.clientX,
              a.prev.clientY - b.prev.clientY,
            );
            const curDist = Math.hypot(
              a.cur.clientX - b.cur.clientX,
              a.cur.clientY - b.cur.clientY,
            );
            const diff = curDist - prevDist;
            // TODO Tune.
            setZoom(boundZoom(zoom + diff / 48));
          }
        }}
        onPointerCancel={(e) => {
          // Might not exist if started elsewhere or already removed by onPointerUp.
          ptrs.remove(e.pointerId);
        }}
        onPointerUp={(e) => {
          // Might not exist if started elsewhere or already removed by onPointerCancel.
          ptrs.remove(e.pointerId);
        }}
        onWheel={(e) => {
          if (!scale || !meta || !vp) {
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
          const newZoom = boundZoom(zoom - delta);
          setZoom(newZoom);
          const nz = meta.viewportScale({
            ...vp,
            zoom: newZoom,
          });

          // Get mouse position in pixels relative to the center element.
          const rect = e.currentTarget.getBoundingClientRect();
          const relX = e.clientX - rect.left - vpWidthPx / 2;
          const relY = e.clientY - rect.top - vpHeightPx / 2;
          // The point position of the cursor at the current zoom level.
          const curZoomTgtPosX = vpCtrXPt + scale.pxToPt(relX);
          const curZoomTgtPosY = vpCtrYPt + scale.pxToPt(relY);
          // How to keep the cursor at the same point after zooming:
          // - If we set the viewport's center to the `curZoomTgtPos*`, we'd see the cursor's position at the center.
          // - Therefore, all we need to do is to then shift by the same amount of absolute pixels back at the new zoom level.
          setVpCtrXPt(curZoomTgtPosX - nz.pxToPt(relX));
          setVpCtrYPt(curZoomTgtPosY - nz.pxToPt(relY));
        }}
      />

      <div className="info">
        <p>
          ({vpCtrXPt.toFixed(2)}, {vpCtrYPt.toFixed(2)})
        </p>
        <p>LOD: {lod == (meta?.lodLevels ?? 0) - 1 ? "max" : lod}</p>
        <p>Zoom: {zoom.toFixed(2)}</p>
      </div>
    </div>
  );
};
