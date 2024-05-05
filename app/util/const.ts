import {
  VArray,
  VFiniteNumber,
  VInteger,
  VOptional,
  VSet,
  VString,
  VStruct,
  VTagged,
  Valid,
} from "@wzlin/valid";

export const vMapStateInit = new VStruct({
  lodLevels: new VFiniteNumber(),
  scoreMax: new VFiniteNumber(),
  scoreMin: new VFiniteNumber(),
  xMaxPt: new VFiniteNumber(),
  xMinPt: new VFiniteNumber(),
  yMaxPt: new VFiniteNumber(),
  yMinPt: new VFiniteNumber(),
});
export type MapStateInit = Valid<typeof vMapStateInit>;

export const vViewportState = new VStruct({
  heightPx: new VFiniteNumber(),
  widthPx: new VFiniteNumber(),
  x0Pt: new VFiniteNumber(),
  y0Pt: new VFiniteNumber(),
  zoom: new VFiniteNumber(),
});
export type ViewportState = Valid<typeof vViewportState>;

export const vPointLabelsMessageToWorker = new VTagged("$type", {
  init: new VStruct({
    edge: new VString(),
    mapInit: vMapStateInit,
  }),
  calculate: new VStruct({
    viewport: vViewportState,
  }),
});

export const vPointLabelsMessageToMain = new VTagged("$type", {
  update: new VStruct({
    zoom: new VInteger(0),
    picked: new VSet(new VInteger(0)),
    cities: new VArray(
      new VStruct({
        label: new VString(),
        x: new VFiniteNumber(),
        y: new VFiniteNumber(),
      }),
    ),
    // Only set if DEBUG_BBOX.
    bboxes: new VOptional(
      new VArray(
        new VStruct({
          minX: new VFiniteNumber(),
          maxX: new VFiniteNumber(),
          minY: new VFiniteNumber(),
          maxY: new VFiniteNumber(),
        }),
      ),
    ),
  }),
});
