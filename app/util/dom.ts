import assertExists from "@xtjs/lib/assertExists";
import { useEffect, useState } from "react";

export const useMeasure = (elem: HTMLElement | null | undefined) => {
  const [rect, setRect] = useState<DOMRectReadOnly>();
  useEffect(() => {
    if (!elem) {
      setRect(undefined);
      return;
    }
    const handler = () => setRect(elem.getBoundingClientRect());
    const observer = new ResizeObserver(handler);
    observer.observe(elem);
    handler();
    return () => observer.disconnect();
  }, [elem]);
  return rect;
};

const canvas = document.createElement("canvas");
const ctx = assertExists(canvas.getContext("2d"));
export const measureText = ({ font, text }: { font: string; text: string }) => {
  ctx.font = font;
  return ctx.measureText(text);
};

export const useBrowserDimensions = () => {
  const [dimensions, setDimensions] = useState({
    width: window.innerWidth,
    height: window.innerHeight,
  });
  useEffect(() => {
    const handler = () =>
      setDimensions({ width: window.innerWidth, height: window.innerHeight });
    window.addEventListener("resize", handler);
    return () => window.removeEventListener("resize", handler);
  }, []);
  return dimensions;
};
