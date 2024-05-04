from common.util import Number
from io import BytesIO
from PIL import Image
from scipy.ndimage import gaussian_filter
from typing import Tuple
import numpy as np
import numpy.typing as npt
import pandas as pd


def render_heatmap_from_grid(
    grid_xs: npt.NDArray[np.uint32],
    grid_ys: npt.NDArray[np.uint32],
    grid_weights: npt.NDArray[np.float32],
    *,
    grid_height: int,
    grid_width: int,
    # RGB, [0, 255].
    color: Tuple[int, int, int],
    alpha_scale: float = 1.0,
    sigma: int = 1,
    upscale: int = 1,
):
    alpha_grid = np.zeros((grid_height, grid_width), dtype=np.float32)
    alpha_grid[grid_ys, grid_xs] = grid_weights
    # Upscale before blurring. If we do it after, the "edges" get "rough" because we are just duplicating the pixels.
    alpha_grid = alpha_grid.repeat(upscale, axis=0).repeat(upscale, axis=1)
    # Technically we should multiply sigma by upscale for consistency, but we leave this to the caller, as this way it's possible to have a "fractional" sigma by increasing the upscale without the sigma. (The sigma cannot normally be a non-integer.)
    blur = gaussian_filter(alpha_grid, sigma=sigma)
    blur = (blur * alpha_scale).clip(min=0, max=1)

    img = np.full(
        (grid_height * upscale, grid_width * upscale, 4),
        (*color, 0),
        dtype=np.uint8,
    )
    img[:, :, 3] = (blur * 255).astype(np.uint8)

    webp_out = BytesIO()
    Image.fromarray(img, "RGBA").save(webp_out, format="webp")
    return webp_out.getvalue()


def render_heatmap(
    xs: npt.NDArray[np.float32],
    ys: npt.NDArray[np.float32],
    weights: npt.NDArray[np.float32],
    *,
    # How to combine weights of points that map to the same grid cell.
    agg: str = "sum",
    x_range: Tuple[Number, Number],
    y_range: Tuple[Number, Number],
    density: Number,
    # RGB, [0, 255].
    color: Tuple[int, int, int],
    alpha_scale: Number = 1.0,
    sigma: int = 1,
    upscale: int = 1,
):
    x_min, x_max = x_range
    y_min, y_max = y_range
    grid_width = int((x_max - x_min) * density)
    grid_height = int((y_max - y_min) * density)

    gv = (
        pd.DataFrame(
            {
                "x": ((xs - x_min) * density).clip(0, grid_width - 1).astype("int32"),
                "y": ((ys - y_min) * density).clip(0, grid_height - 1).astype("int32"),
                "weight": weights,
            }
        )
        .groupby(["x", "y"])
        .agg(agg)
        .reset_index()
    )

    return render_heatmap_from_grid(
        grid_xs=gv["x"].to_numpy(),
        grid_ys=gv["y"].to_numpy(),
        grid_weights=gv["weight"].to_numpy(),
        grid_height=grid_height,
        grid_width=grid_width,
        color=color,
        alpha_scale=alpha_scale,
        sigma=sigma,
        upscale=upscale,
    )
