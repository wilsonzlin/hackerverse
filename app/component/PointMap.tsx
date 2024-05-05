import { decode } from "@msgpack/msgpack";
import { VFiniteNumber, VInteger, VStruct } from "@wzlin/valid";
import Dict from "@xtjs/lib/Dict";
import UnreachableError from "@xtjs/lib/UnreachableError";
import assertExists from "@xtjs/lib/assertExists";
import bounded from "@xtjs/lib/bounded";
import {
  MutableRefObject,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Point } from "../util/const";
import { EdgeContext } from "../util/item";
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

export type PointMapController = {
  animate(
    vp: {
      x0Pt: number;
      x1Pt: number;
      y0Pt: number;
      y1Pt: number;
    },
    ms: number,
  ): void;
  cancelAnimation(): void;
};

export const PointMap = ({
  controllerRef,
  heatmaps,
  height: vpHeightPx,
  nearbyQuery,
  onNearbyQuery,
  onNearbyQueryResults,
  resultPoints,
  width: vpWidthPx,
}: {
  controllerRef?: MutableRefObject<PointMapController | undefined>;
  heatmaps: ImageBitmap[];
  height: number;
  nearbyQuery: { x: number; y: number } | undefined;
  onNearbyQuery: (pt: { x: number; y: number } | undefined) => void;
  onNearbyQueryResults: (points: Point[] | undefined) => void;
  resultPoints: undefined | { x: number; y: number }[];
  width: number;
}) => {
  const nextAnimId = useRef(0);
  const curAnim = useRef<number>();
  const cancelAnimation = () => {
    curAnim.current = undefined;
  };
  if (controllerRef) {
    controllerRef.current = {
      animate: (vp, ms) => {
        if (!scale) {
          return;
        }
        const xRangePt = vp.x1Pt - vp.x0Pt;
        const yRangePt = vp.y1Pt - vp.y0Pt;
        const tgtCtrXPt = (vp.x0Pt + vp.x1Pt) / 2;
        const tgtCtrYPt = (vp.y0Pt + vp.y1Pt) / 2;
        let tgtPxPerPt;
        if (xRangePt > yRangePt) {
          tgtPxPerPt = vpWidthPx / xRangePt;
        } else {
          tgtPxPerPt = vpHeightPx / yRangePt;
        }
        const tgtZoom =
          Math.log(tgtPxPerPt / scale.pxPerPtBase) /
          Math.log(1 + 1 / ZOOM_PER_LOD);
        const initCtrXPt = vpCtrXPt;
        const initCtrYPt = vpCtrYPt;
        const initZoom = zoom;
        const animId = nextAnimId.current++;
        let started: number | undefined;
        const animate = (ts: number) => {
          if (curAnim.current !== animId) {
            return;
          }
          started ??= ts;
          const elapsed = ts - started;
          const progress = bounded(elapsed / ms, 0, 1);
          const easedProgress = 1 - Math.pow(1 - progress, 2);
          setVpCtrXPt(initCtrXPt + (tgtCtrXPt - initCtrXPt) * easedProgress);
          setVpCtrYPt(initCtrYPt + (tgtCtrYPt - initCtrYPt) * easedProgress);
          setZoom(initZoom + (tgtZoom - initZoom) * easedProgress);
          if (progress == 1) {
            return;
          }
          requestAnimationFrame(animate);
        };
        curAnim.current = animId;
        requestAnimationFrame(animate);
      },
      cancelAnimation,
    };
  }

  const [theme, setTheme] = useState<"land" | "space">(
    localStorage.getItem("hndr-map-theme") || ("land" as any),
  );
  useEffect(() => localStorage.setItem("hndr-map-theme", theme), [theme]);

  const edge = useContext(EdgeContext);
  const $canvas = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    const $c = $canvas.current;
    if (!$c) {
      return;
    }
    // https://web.dev/articles/canvas-hidipi
    $c.width = vpWidthPx * devicePixelRatio;
    $c.height = vpHeightPx * devicePixelRatio;
    const ctx = $c.getContext("2d");
    ctx!.scale(devicePixelRatio, devicePixelRatio);
    $c.style.width = `${vpWidthPx}px`;
    $c.style.height = `${vpHeightPx}px`;
  }, [vpWidthPx, vpHeightPx]);
  const [meta, setMeta] = useState<MapState>();
  const [map, setMap] = useState<ReturnType<typeof createCanvasPointMap>>();
  useEffect(() => map?.setEdge(edge), [map, edge]);
  useEffect(() => map?.setHeatmaps(heatmaps), [map, heatmaps]);
  useEffect(() => map?.setTheme(theme), [map, theme]);
  useEffect(() => {
    map?.setNearbyQuery(nearbyQuery);
    return () => map?.setNearbyQuery(undefined);
  }, [map, nearbyQuery]);
  useEffect(() => {
    map?.onNearbyQueryResults(onNearbyQueryResults);
    return () => map?.offNearbyQueryResults();
  }, [map, onNearbyQueryResults]);
  useEffect(() => {
    const ac = new AbortController();
    (async () => {
      const res = await fetch(
        `https://us-ashburn-1.edge-hndr.wilsonl.in/map/${MAP_DATASET}/meta`,
        { signal: ac.signal },
      );
      const raw = await res.arrayBuffer();
      const metaInit = vMapMeta.parseRoot(decode(raw));
      const meta = new MapState({
        lodLevels: metaInit.lod_levels,
        scoreMax: metaInit.score_max,
        scoreMin: metaInit.score_min,
        xMaxPt: metaInit.x_max,
        xMinPt: metaInit.x_min,
        yMaxPt: metaInit.y_max,
        yMinPt: metaInit.y_min,
      });
      console.log("Map metadata:", meta);
      setMeta(meta);
      setVpCtrXPt(meta.xMinPt + meta.xRangePt / 2);
      setVpCtrYPt(meta.yMinPt + meta.yRangePt / 2);
      const map = createCanvasPointMap({
        canvas: assertExists($canvas.current),
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

  useEffect(
    () => void map?.setResultPoints(resultPoints ?? []),
    [map, resultPoints],
  );

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
        onPointerDown={(e) => {
          e.currentTarget.setPointerCapture(e.pointerId);
          // Weirdly, duplicate keys can happen (either pointer can have multiple pointerdown events, or pointerId values can be reused), so don't assert it doesn't exist.
          ptrs.set(e.pointerId, {
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
          // May not exist (pointer started elsewhere, moved to over this element).
          const ptr = ptrs.remove(e.pointerId);
          if (!ptr) {
            return;
          }
          if (!scale) {
            return;
          }
          e.preventDefault();
          ptr.prev = ptr.cur;
          ptr.cur = e.nativeEvent;
          if (
            Math.hypot(
              ptr.start.clientX - ptr.cur.clientX,
              ptr.start.clientY - ptr.cur.clientY,
            ) < 10
          ) {
            // Did not move much, so it's a click.
            const pt = {
              x: vpCtrXPt + scale.pxToPt(ptr.cur.clientX - vpWidthPx / 2),
              y: vpCtrYPt + scale.pxToPt(ptr.cur.clientY - vpHeightPx / 2),
            };
            onNearbyQuery(pt);
          }
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

      <div className="controls">
        <button
          className="theme"
          style={{
            background:
              theme == "land"
                ? "black"
                : "linear-gradient(35deg, #6cd2e7 0 50%, #bbecd8 50% 100%)",
          }}
          onClick={() => {
            setTheme(theme === "land" ? "space" : "land");
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
    </div>
  );
};
