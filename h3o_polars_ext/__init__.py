from .h3o_polars_ext import cell_to_parent, grid_ring
import polars as pl

def register_namespace():
    @pl.api.register_series_namespace("h3")
    class H3Shortcuts:
        def __init__(self, s: pl.Series):
            self._s = s

        def cell_to_parent(self, res: int) -> pl.Series:
            return cell_to_parent(self._s, res)
        
        def grid_ring(self, k: int) -> pl.Series:
            return grid_ring(self._s, k)
        
register_namespace()