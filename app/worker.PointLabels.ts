import { decode } from "@msgpack/msgpack";
import {
  VArray,
  VFiniteNumber,
  VInteger,
  VString,
  VStruct,
  Valid,
} from "@wzlin/valid";
import UnreachableError from "@xtjs/lib/UnreachableError";
import assertExists from "@xtjs/lib/assertExists";
import propertyComparator from "@xtjs/lib/propertyComparator";
import reversedComparator from "@xtjs/lib/reversedComparator";
import RBush, { BBox } from "rbush";
import {
  MapStateInit,
  ViewportState,
  vPointLabelsMessageToMain,
  vPointLabelsMessageToWorker,
} from "./util/const";
import {
  MAP_DATASET,
  MapState,
  ZOOM_PER_LOD,
  cachedFetchTile,
  calcLabelBBox,
  ensureFetchedPostTitleLengths,
} from "./util/map";

const createPointLabelsPicker = ({
  edge,
  mapInit,
}: {
  edge: string;
  mapInit: MapStateInit;
}) => {
  const map = new MapState(mapInit);
  let curViewport: ViewportState | undefined;
  let citiesLoadPromise: Promise<any> | undefined;

  // One for each integer zoom level [0, map.zoomMax] (inclusive).
  const labelledPoints = Array.from({ length: map.zoomMax + 1 }, () => ({
    processedTiles: new Set<string>(),
    picked: new Set<number>(),
    skipped: new Set<number>(), // Not picked if collided.
    tree: new RBush<BBox>(),
    cities: Array<{
      label: string;
      x: number;
      y: number;
    }>(),
  }));
  const sendUpdate = (zoom: number) => {
    const z = Math.floor(zoom);
    const msg: Valid<typeof vPointLabelsMessageToMain> = {
      $type: "update",
      zoom: z,
      picked: labelledPoints[z].picked,
      cities: labelledPoints[z].cities,
    };
    self.postMessage(msg);
  };
  let latestPickLabelledPointsReqId = 0;
  let pickingLabelledPoints = false;
  const pickLabelledPoints = () => {
    if (pickingLabelledPoints) {
      latestPickLabelledPointsReqId++;
      return;
    }
    pickingLabelledPoints = true;
    (async () => {
      // Calculate cities first to ensure they always show.
      await (citiesLoadPromise ??= (async () => {
        const res = await fetch(
          `https://${edge}.edge-hndr.wilsonl.in/map/${MAP_DATASET}/cities`,
        );
        if (!res.ok) {
          throw new Error(`Failed to fetch terrain with error ${res.status}`);
        }
        const raw = decode(await res.arrayBuffer());
        const vCities = new VArray(
          new VStruct({
            lod: new VInteger(0),
            cities: new VArray(
              new VStruct({
                label: new VString(),
                x: new VFiniteNumber(),
                y: new VFiniteNumber(),
              }),
            ),
          }),
        );
        for (const { lod, cities } of vCities.parseRoot(raw)) {
          for (const city of cities) {
            for (let z = lod * ZOOM_PER_LOD; z < labelledPoints.length; z++) {
              const lp = labelledPoints[z];
              const scaled = map.viewportScale({
                ...curViewport!,
                zoom: z,
              });
              lp.cities.push(city);
              const Y_PX_PER_CHAR = 24;
              const X_PX_PER_CHAR = Y_PX_PER_CHAR * (9 / 16);
              lp.tree.insert({
                minX: scaled.ptToPx(city.x),
                minY: scaled.ptToPx(city.y),
                maxX: scaled.ptToPx(city.x) + city.label.length * X_PX_PER_CHAR,
                maxY: scaled.ptToPx(city.y) + Y_PX_PER_CHAR,
              });
            }
          }
        }
      })());

      while (true) {
        const req = latestPickLabelledPointsReqId;
        const vp = assertExists(curViewport);
        for (let z = 0; z <= vp.zoom; z++) {
          const lod = map.calcLod(z);
          const { tileXMax, tileXMin, tileYMax, tileYMin } = map.viewportTiles({
            ...vp,
            zoom: z,
          });
          const lp = assertExists(labelledPoints[z]);
          const points = await Promise.all(
            (function* () {
              for (let x = tileXMin; x <= tileXMax; x++) {
                for (let y = tileYMin; y <= tileYMax; y++) {
                  const k = `${x}-${y}`;
                  if (lp.processedTiles.has(k)) {
                    continue;
                  }
                  lp.processedTiles.add(k);
                  yield cachedFetchTile(undefined, edge, lod, x, y);
                }
              }
            })(),
          ).then((res) =>
            res.flat().sort(reversedComparator(propertyComparator("score"))),
          );
          // Do not bail out early here in case request has now expired, as we've marked the tiles as processed (but the processing is up next).
          await ensureFetchedPostTitleLengths(
            edge,
            points.map((p) => p.id),
          );
          for (const p of points) {
            // We still need to check existence in and update lp.{picked,skipped} despite processedTiles, because of transferring points between zoom levels.
            if (lp.picked.has(p.id) || lp.skipped.has(p.id)) {
              continue;
            }
            const box = calcLabelBBox(
              map,
              {
                ...vp,
                zoom: z,
              },
              p,
            );
            const picked = !lp.tree.collides(box);
            // Propagate to all further zoom levels. This is simpler to keep in sync and get correct than trying to pull from previous zoom levels.
            for (let zn = z; zn < labelledPoints.length; zn++) {
              const lpn = labelledPoints[zn];
              if (picked) {
                // We can't just cache and reuse the previous zoom's BBox values for points, because each zoom has different margin pt. sizes.
                lpn.tree.insert(
                  calcLabelBBox(
                    map,
                    {
                      ...vp,
                      zoom: zn,
                    },
                    p,
                  ),
                );
                lpn.picked.add(p.id);
              } else {
                // Avoid recalculating over and over again.
                lpn.skipped.add(p.id);
              }
            }
          }
        }
        if (req == latestPickLabelledPointsReqId) {
          sendUpdate(vp.zoom);
          break;
        }
      }
      pickingLabelledPoints = false;
    })();
  };

  return {
    calculate: (vp: ViewportState) => {
      curViewport = vp;
      // Send the current state so that the canvas can be updated (and feels responsive) while we're calculating, in case the points have changed due to propagation from a previous zoom level.
      sendUpdate(vp.zoom);
      pickLabelledPoints();
    },
  };
};

let pointLabelsPicker: ReturnType<typeof createPointLabelsPicker> | undefined;

addEventListener("message", (e) => {
  const msg = vPointLabelsMessageToWorker.parseRoot(e.data);
  if (msg.$type === "calculate") {
    pointLabelsPicker!.calculate(msg.viewport);
  } else if (msg.$type === "init") {
    pointLabelsPicker = createPointLabelsPicker(msg);
  } else {
    throw new UnreachableError();
  }
});
