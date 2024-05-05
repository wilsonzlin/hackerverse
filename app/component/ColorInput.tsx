import hexToRgb from "@xtjs/lib/hexToRgb";
import rgbToHex from "@xtjs/lib/rgbToHex";
import "./ColorInput.css";

export const ColorInput = ({
  color,
  onChange,
  size,
}: {
  color: [number, number, number];
  onChange: (color: [number, number, number]) => void;
  size: number;
}) => {
  return (
    <label className="ColorInput">
      <input
        hidden
        type="color"
        value={rgbToHex(...color)}
        onChange={(e) => onChange(hexToRgb(e.currentTarget.value))}
      />
      <div
        style={{
          backgroundColor: `rgb(${color.join(",")})`,
          height: `${size}px`,
          width: `${size}px`,
        }}
      />
    </label>
  );
};
