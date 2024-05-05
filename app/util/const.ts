import {
  VArray,
  VFiniteNumber,
  VInteger,
  VOptional,
  VSet,
  VString,
  VStruct,
  VTagged,
  VUnion,
  Valid,
} from "@wzlin/valid";
import RBush, { BBox } from "rbush";

export const vPoint = new VStruct({
  id: new VInteger(1),
  x: new VFiniteNumber(),
  y: new VFiniteNumber(),
  score: new VInteger(),
});
export type Point = Valid<typeof vPoint>;

export const vCity = new VStruct({
  label: new VString(),
  x: new VFiniteNumber(),
  y: new VFiniteNumber(),
});
export type City = Valid<typeof vCity>;

export class PointTree extends RBush<Point> {
  toBBox(item: Point): BBox {
    return {
      minX: item.x,
      maxX: item.x,
      maxY: item.y,
      minY: item.y,
    };
  }
  compareMinX(a: Point, b: Point): number {
    return a.x - b.x;
  }
  compareMinY(a: Point, b: Point): number {
    return a.y - b.y;
  }
}

export class PointOrCityTree extends RBush<Point | City> {
  toBBox(item: Point | City): BBox {
    return {
      minX: item.x,
      maxX: item.x,
      maxY: item.y,
      minY: item.y,
    };
  }
  compareMinX(a: Point | City, b: Point | City): number {
    return a.x - b.x;
  }
  compareMinY(a: Point | City, b: Point | City): number {
    return a.y - b.y;
  }
}

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
  nearby: new VStruct({
    requestId: new VInteger(),
    lod: new VInteger(0),
    xPt: new VFiniteNumber(),
    yPt: new VFiniteNumber(),
  }),
});

export const vPointLabelsMessageToMain = new VTagged("$type", {
  nearby: new VStruct({
    requestId: new VInteger(),
    points: new VArray(new VUnion(vPoint, vCity)),
  }),
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
