from functools import partial
from h3o_polars_ext import cell_to_parent

import h3.api.basic_int as h3
import numpy as np
import polars as pl
import time

def hexadecimal_to_int64(s: pl.Series) -> pl.Series:
    first   = np.left_shift(s.str.slice(0,  5).str.parse_int(16).cast(pl.UInt64), 40)
    second  = np.left_shift(s.str.slice(5,  5).str.parse_int(16).cast(pl.UInt64), 20)
    third  =  s.str.slice(10).str.parse_int(16).cast(pl.UInt64)
    return first + second + third

parent_f = partial(h3.cell_to_parent, res=5)
NUM_VALS = 50_000_000
print('Get starting series')
start = hexadecimal_to_int64(pl.Series(['85283473fffffff','8526c38bfffffff','8526e387fffffff'] * NUM_VALS))

print('Begin first test')
start_time = time.time()
t1 = start.apply(parent_f)
end_time = time.time()
print(f"Duration of series apply: {end_time - start_time}")

start_time = time.time()
t2 = cell_to_parent(start, 5)
end_time = time.time()
print(f"Duration of cell_to_parent: {end_time - start_time}")