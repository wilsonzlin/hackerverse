import gif from "./Loading.gif";

export const Loading = ({ size }: { size?: number }) => (
  <img className="Loading" src={gif} alt="Loading&hellip;" width={size} />
);
