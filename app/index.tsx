import {
  BarElement,
  Chart,
  LineElement,
  LinearScale,
  PointElement,
  TimeScale,
  Tooltip,
} from "chart.js";
import "chartjs-adapter-luxon";
import { createRoot } from "react-dom/client";
import { App } from "./component/App";
import "./index.css";

Chart.register(
  BarElement,
  LineElement,
  PointElement,
  LinearScale,
  TimeScale,
  Tooltip,
);

createRoot(document.getElementById("root")!).render(<App />);
