import { VFiniteNumber, VInteger, VStruct, Valid } from "@wzlin/valid";
import { useEffect, useRef, useState } from "react";
import "./PointMap.css";

const MAP_WIDTH = 1600;
const MAP_HEIGHT = 1600;
const ZOOM_LEVELS = 8;

type Point = { id: number; x: number; y: number; score: number };

const vMapMeta = new VStruct({
  x_min: new VFiniteNumber(),
  x_max: new VFiniteNumber(),
  y_min: new VFiniteNumber(),
  y_max: new VFiniteNumber(),
  count: new VInteger(0),
});

export const PointMap = () => {
  const [meta, setMeta] = useState<Valid<typeof vMapMeta>>();
  const xMax = meta?.x_max ?? 0;
  const xMin = meta?.x_min ?? 0;
  const yMax = meta?.y_max ?? 0;
  const yMin = meta?.y_min ?? 0;
  useEffect(() => {
    (async () => {
      setMeta(
        vMapMeta.parseRoot(
          await fetch("http://localhost:8080/map.json").then((res) =>
            res.json(),
          ),
        ),
      );
    })();
  }, []);

  const [zoom, setZoom] = useState(0);
  const [points, setPoints] = useState(Array<Point>());
  useEffect(() => {
    (async () => {
      const res = await fetch(`http://localhost:8080/map_z${zoom}.bin`);
      const raw = await res.arrayBuffer();
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
      const points = Array.from({ length: count }, (_, j) => ({
        id: ids[j],
        x: xs[j],
        y: ys[j],
        score: scores[j],
      }));
      setPoints(points);
    })();
  }, [zoom]);

  const $canvas = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    if (!$canvas.current) {
      return;
    }
    const scaleX = MAP_WIDTH / (xMax - xMin);
    const scaleY = MAP_HEIGHT / (yMax - yMin);
    const ctx = $canvas.current.getContext("2d")!;
    ctx.clearRect(0, 0, MAP_WIDTH, MAP_HEIGHT);
    for (const point of points) {
      ctx.fillStyle = `rgba(0, 0, 0, 0)})`;
      ctx.beginPath();
      ctx.arc(
        (point.x - xMin) * scaleX,
        (point.y - yMin) * scaleY,
        2,
        0,
        Math.PI * 2,
      );
      ctx.fill();
    }
  }, [points]);

  return (
    <div className="PointMap">
      <div className="controls">
        <div>{zoom}</div>
        <button
          disabled={zoom == ZOOM_LEVELS - 1}
          onClick={() => setZoom(zoom + 1)}
        >
          Zoom in
        </button>
      </div>
      <div className="window">
        <canvas ref={$canvas} width={MAP_WIDTH} height={MAP_HEIGHT} />
      </div>
    </div>
  );
};
