import { ReactNode } from "react";
import { router } from "../util/router";

export const RouteLink = ({
  href,
  children,
}: {
  href: string;
  children: ReactNode | ReactNode[];
}) => (
  <a
    href={href}
    onClick={(e) => {
      e.preventDefault();
      router.change(href);
    }}
  >
    {children}
  </a>
);
