from io import BytesIO
from PIL import Image
from scipy.ndimage import gaussian_filter
import numpy as np
import numpy.typing as npt
import pandas as pd


def render_terrain(
    xs: npt.NDArray[np.float32],
    ys: npt.NDArray[np.float32],
    *,
    # Increasing this increases grid size and therefore distance between points, making blur less effective. More blank space will be incorporated into the blur kernel, even if sigma is increased.
    dpi=32,
    # Render at higher resolution.
    upscale: int = 1,
    # Gaussian blur parameter. Higher = more blur. Must be a positive integer. Set to zero to disable altogether.
    sigma: int = 1,
    # This is what Google Maps does with terrain; it's not a smooth gradient. Set to zero to disable.
    contours: int = 4,
    color_land=(144, 224, 190),
    color_water=(108, 210, 231),
):
    x_min, x_max = (xs.min(), xs.max())
    y_min, y_max = (ys.min(), ys.max())
    grid_width = int((x_max - x_min) * dpi)
    grid_height = int((y_max - y_min) * dpi)

    USE_LOG_SCALE = True

    gv = pd.DataFrame(
        {
            "x": ((xs - x_min) * dpi).clip(0, grid_width - 1).astype("int32"),
            "y": ((ys - y_min) * dpi).clip(0, grid_height - 1).astype("int32"),
        }
    )
    gv = gv.groupby(["x", "y"]).value_counts().reset_index(name="density")
    if USE_LOG_SCALE:
        gv["density"] = np.log(gv["density"] + 1)

    grid = np.zeros((grid_height, grid_width), dtype=np.float32)
    grid[gv["y"], gv["x"]] = gv["density"]
    # Upscale before blurring. If we do it after, the smooth blurred "edges" get "rough" because we are just duplicating the pixels.
    grid = grid.repeat(upscale, axis=0).repeat(upscale, axis=1)
    if sigma:
        grid = gaussian_filter(grid, sigma=sigma * upscale)

    if contours:
        g_min, g_max = grid.min(), grid.max()
        buckets = contours
        bucket_size = (g_max - g_min) / buckets
        # Values fall into [0, buckets - 1].
        # Yes, this means that some points will fall onto a grid cell with value 0 i.e. some will be on water. This looks nicer than trying to force land onto every point (i.e. bucket minimum value of 1).
        grid = (grid - g_min) // bucket_size
        # Some values may lie exactly on the max and will end up with a bucket of `bucket`.
        grid = np.clip(grid, 0, buckets)
        # Divide by (buckets - 1) to get [0, 1] as otherwise nothing is full alpha.
        grid = grid / (buckets - 1)

    img = np.full(
        (grid_height * upscale, grid_width * upscale, 4),
        (*color_land, 0),
        dtype=np.uint8,
    )
    img[:, :, 3] = (grid * 255).astype(np.uint8)
    webp_out = BytesIO()
    Image.fromarray(img, "RGBA").save(webp_out, format="webp", lossless=True)
    land = webp_out.getvalue()

    img = np.full(
        (grid_height * upscale, grid_width * upscale, 4),
        (*color_water, 0),
        dtype=np.uint8,
    )
    img[:, :, 3] = (grid == 0).astype(np.uint8) * 255
    webp_out = BytesIO()
    Image.fromarray(img, "RGBA").save(webp_out, format="webp", lossless=True)
    water = webp_out.getvalue()

    return {
        "land": land,
        "water": water,
    }
