import { useState } from "react";
import { Ico } from "./Ico";
import "./PageSwitcher.css";
import { RouteLink } from "./RouteLink";

export const PageSwitcher = () => {
  const [show, setShow] = useState(false);

  const ICO_SIZE = 24;

  return (
    <div className="PageSwitcher">
      <button onClick={() => setShow(!show)}>
        <Ico i="apps" size={24} />
      </button>
      {show && (
        <div className="menu">
          <RouteLink href="/">
            <span>Search</span>
            <Ico i="public" size={ICO_SIZE} />
          </RouteLink>
          <RouteLink href="/journey">
            <span>Journey</span>
            <Ico i="travel" size={ICO_SIZE} />
          </RouteLink>
          <RouteLink href="/recommend">
            <span>Recommend</span>
            <Ico i="lightbulb" size={ICO_SIZE} />
          </RouteLink>
          <RouteLink href="/analysis">
            <span>Analysis</span>
            <Ico i="monitoring" size={ICO_SIZE} />
          </RouteLink>
        </div>
      )}
    </div>
  );
};
