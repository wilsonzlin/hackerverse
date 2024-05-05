import { ReactNode } from "react";
import { router } from "../util/router";

export const RouteLink = ({
  className,
  href,
  children,
}: {
  className?: string;
  href: string;
  children: ReactNode | ReactNode[];
}) => (
  <a
    className={className}
    href={href}
    onClick={(e) => {
      e.preventDefault();
      router.change(href);
    }}
  >
    {children}
  </a>
);
