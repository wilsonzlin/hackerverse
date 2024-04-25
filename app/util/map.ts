import {
  VFiniteNumber,
  VInstanceOf,
  VInteger,
  VString,
  VStruct,
  VTagged,
} from "@wzlin/valid";

export const PX_PER_PT_BASE = 64;
export const ZOOM_PER_LOD = 3;

export const vWorkerPointMapMessageToWorker = new VTagged("$type", {
  init: new VStruct({
    canvas: new VInstanceOf(OffscreenCanvas),
  }),
  reset: new VStruct({}),
  // Render the points at LOD `lod` from (ptX0, ptY0) to (ptX1, ptY1) (inclusive) on the canvas.
  render: new VStruct({
    requestId: new VInteger(0),
    edge: new VString(),
    lod: new VInteger(0),
    zoom: new VFiniteNumber(0),

    windowWidthPx: new VFiniteNumber(),
    windowHeightPx: new VFiniteNumber(),

    x0Pt: new VFiniteNumber(),
    x1Pt: new VFiniteNumber(),
    y0Pt: new VFiniteNumber(),
    y1Pt: new VFiniteNumber(),

    // These represent the whole map.
    xMaxPt: new VFiniteNumber(),
    xMinPt: new VFiniteNumber(),
    yMaxPt: new VFiniteNumber(),
    yMinPt: new VFiniteNumber(),
    scoreMin: new VInteger(),
    scoreMax: new VInteger(),
  }),
});

export const mapCalcs = ({ zoom }: { zoom: number }) => {
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
