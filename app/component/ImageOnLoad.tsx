import { useState } from "react";
import "./ImageOnLoad.css";

export const ImageOnLoad = ({
  className,
  src,
}: {
  className?: string;
  src: string;
}) => {
  const [loaded, setLoaded] = useState(false);
  return (
    <img
      className={className}
      data-loaded={loaded}
      loading="lazy"
      onLoad={() => setLoaded(true)}
      referrerPolicy="no-referrer"
      src={src}
    />
  );
};
