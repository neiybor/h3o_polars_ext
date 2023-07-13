from .h3o_polars_ext import cell_to_parent
import polars as pl

def register_namespace():
    @pl.api.register_series_namespace("h3")
    class H3Shortcuts:
        def __init__(self, s: pl.Series):
            self._s = s

        def cell_to_parent(self, res: int) -> pl.Series:
            return cell_to_parent(self._s, res)
        
register_namespace()