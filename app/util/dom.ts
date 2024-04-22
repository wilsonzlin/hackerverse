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
