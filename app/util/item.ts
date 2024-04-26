import { fetchHnItem, vItem } from "@wzlin/crawler-toolkit-hn";
import mapExists from "@xtjs/lib/mapExists";

export const fetchItem = async (id: number) => {
  const cacheKey = `hndr:hn-item:${id}`;
  const cached = mapExists(localStorage.getItem(cacheKey), (raw) =>
    vItem.parseRoot(JSON.parse(raw)),
  );
  if (cached) {
    return cached;
  }
  const item = await fetchHnItem(id);
  localStorage.setItem(cacheKey, JSON.stringify(item));
  return item;
};
