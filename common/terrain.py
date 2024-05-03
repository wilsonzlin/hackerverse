from scipy.ndimage import gaussian_filter
from typing import Dict
from typing import List
import cv2
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
    # This is what Google Maps does with terrain; it's not a smooth gradient. Must be at least one.
    contours: int = 4,
    use_log_scale=True,
):
    x_min, x_max = (xs.min(), xs.max())
    y_min, y_max = (ys.min(), ys.max())
    grid_width = int((x_max - x_min) * dpi)
    grid_height = int((y_max - y_min) * dpi)

    gv = pd.DataFrame(
        {
            "x": ((xs - x_min) * dpi).clip(0, grid_width - 1).astype("int32"),
            "y": ((ys - y_min) * dpi).clip(0, grid_height - 1).astype("int32"),
        }
    )
    gv = gv.groupby(["x", "y"]).value_counts().reset_index(name="density")
    if use_log_scale:
        gv["density"] = np.log(gv["density"] + 1)

    grid = np.zeros((grid_height, grid_width), dtype=np.float32)
    grid[gv["y"], gv["x"]] = gv["density"]
    # Upscale before blurring. If we do it after, the smooth blurred "edges" get "rough" because we are just duplicating the pixels.
    grid = grid.repeat(upscale, axis=0).repeat(upscale, axis=1)
    if sigma:
        grid = gaussian_filter(grid, sigma=sigma * upscale)

    g_min, g_max = grid.min(), grid.max()
    buckets = contours
    bucket_size = (g_max - g_min) / buckets
    # Values fall into [0, buckets - 1].
    # Yes, this means that some points will fall onto a grid cell with value 0 i.e. some will be on water. This looks nicer than trying to force land onto every point (i.e. bucket minimum value of 1), because it creates too many sparse random-looking dull blotches.
    grid = (grid - g_min) // bucket_size
    # Some values may lie exactly on the max and will end up with a bucket of `buckets`.
    grid = np.clip(grid, 0, buckets - 1)

    # Map from level to list of paths, where a path is a NumPy matrix of (x, y) points.
    shapes: Dict[int, List[npt.NDArray[np.float32]]] = {}
    for bucket in range(buckets):
        shapes[bucket] = []
        num_shapes, labelled_image = cv2.connectedComponents(
            (grid == bucket).astype(np.uint8)
        )
        # Ignore label 0 as it's the background.
        for shape_no in range(1, num_shapes):
            shape_mask = labelled_image == shape_no
            # Use RETR_EXTERNAL as we only want the outer edges, and don't care about inner holes since they'll be represented by other larger-bucket shapes.
            shape_contours, _ = cv2.findContours(
                shape_mask.astype(np.uint8), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
            )
            for shape_border_points in shape_contours:
                # The resulting shape is (N, 1, 2), where N is the number of points. Remove unnecessary second dimension.
                shape_border_points = shape_border_points.squeeze(1)
                if shape_border_points.shape[0] < 4:
                    # Not a polygon.
                    continue
                # We want bucket 0 only when it cuts out an inner hole in a larger bucket.
                if bucket == 0 and (0, 0) in shape_border_points:
                    continue

                # Convert back to original scale.
                shape_border_points = shape_border_points / upscale
                shape_border_points[:, 0] = shape_border_points[:, 0] / dpi + x_min
                shape_border_points[:, 1] = shape_border_points[:, 1] / dpi + y_min
                shapes[bucket].append(shape_border_points.astype(np.float32))

    return shapes
