import mapExists from "@xtjs/lib/mapExists";
import "./Ico.css";

export const Ico = ({ i, size }: { i: string; size?: number }) => (
  <span
    className="Ico"
    style={{
      fontSize: mapExists(size, (s) => `${s}px`),
    }}
  >
    {i}
  </span>
);
