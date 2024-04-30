import mapExists from "@xtjs/lib/mapExists";
import "./Ico.css";

export const Ico = ({
  i,
  size,
  inline,
}: {
  i: string;
  size?: number;
  inline?: boolean;
}) => (
  <span
    className="Ico"
    style={{
      fontSize: mapExists(size, (s) => `${s}px`),
      // We rarely want this to be inline as it causes weird line height issues, but it's exposed as a configurable prop if needed.
      display: inline ? undefined : "block",
    }}
  >
    {i}
  </span>
);
