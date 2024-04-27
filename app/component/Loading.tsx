import "./Loading.css";

export const Loading = ({
  size = 32,
  thickness,
}: {
  size?: number;
  thickness?: number;
}) => (
  <div
    className="Loading"
    style={{
      fontSize: `${thickness ?? size * 0.1}px`,
      height: `${size}px`,
      width: `${size}px`,
    }}
  />
);
