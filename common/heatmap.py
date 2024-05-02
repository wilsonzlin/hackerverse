from io import BytesIO
from PIL import Image
from scipy.ndimage import gaussian_filter
from typing import Tuple
import numpy as np
import numpy.typing as npt


def render_heatmap(
    xs: npt.NDArray[np.float32],
    ys: npt.NDArray[np.float32],
    weights: npt.NDArray[np.float32],
    *,
    x_range: Tuple[float, float],
    y_range: Tuple[float, float],
    density: float,
    # RGB, [0, 255].
    color: Tuple[int, int, int],
    alpha_scale: float = 1.0,
    sigma: int = 1,
    upscale: int = 1,
):
    x_min, x_max = x_range
    y_min, y_max = y_range
    grid_width = int((x_max - x_min) * density)
    grid_height = int((y_max - y_min) * density)

    grid_xs = ((xs - x_min) * density).clip(0, grid_width - 1).astype(int)
    grid_ys = ((ys - y_min) * density).clip(0, grid_height - 1).astype(int)

    alpha_grid = np.zeros((grid_height, grid_width), dtype=np.float32)
    alpha_grid[grid_ys, grid_xs] = weights
    alpha_grid = alpha_grid.repeat(upscale, axis=0).repeat(upscale, axis=1)
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
