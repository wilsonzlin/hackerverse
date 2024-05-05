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
  DEBUG_BBOX,
  MAP_DATASET,
  MapState,
  ZOOM_PER_LOD,
  cachedFetchTile,
  calcCityLabelBBox,
  calcPointLabelBBox,
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
      bboxes: DEBUG_BBOX ? labelledPoints[z].tree.all() : undefined,
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
              lp.cities.push(city);
              lp.tree.insert(
                calcCityLabelBBox(
                  map,
                  {
                    ...curViewport!,
                    zoom: z,
                  },
                  city,
                ),
              );
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
            const box = calcPointLabelBBox(
              map,
              {
                ...vp,
                zoom: z,
              },
              p,
            );
            const picked = !lp.tree.collides(box);
            if (!picked) {
              // Avoid recalculating over and over again.
              // WARNING: Only mark as skipped for current zoom level (i.e. don't propagate), as it may be picked at a higher zoom level.
              lp.skipped.add(p.id);
            } else {
              // Propagate to all further zoom levels. This is simpler to keep in sync and get correct than trying to pull from previous zoom levels.
              for (let zn = z; zn < labelledPoints.length; zn++) {
                const lpn = labelledPoints[zn];
                // We can't just cache and reuse the previous zoom's BBox values for points, because each zoom has different margin pt. sizes.
                lpn.tree.insert(
                  calcPointLabelBBox(
                    map,
                    {
                      ...vp,
                      zoom: zn,
                    },
                    p,
                  ),
                );
                lpn.picked.add(p.id);
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
