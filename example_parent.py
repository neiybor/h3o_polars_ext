from functools import partial
import h3o_polars_ext as h3e

# import h3.api.basic_int as h3
import h3
import numpy as np
import polars as pl
import time

def hexadecimal_to_int64(s: pl.Series) -> pl.Series:
    first   = np.left_shift(s.str.slice(0,  5).str.parse_int(16).cast(pl.UInt64), 40)
    second  = np.left_shift(s.str.slice(5,  5).str.parse_int(16).cast(pl.UInt64), 20)
    third  =  s.str.slice(10).str.parse_int(16).cast(pl.UInt64)
    return first + second + third

parent_f = partial(h3.cell_to_parent, res=5)
NUM_VALS = 10_000_000
print('Get starting series')
# start = hexadecimal_to_int64(pl.Series(['85283473fffffff','8526c38bfffffff','8526e387fffffff'] * NUM_VALS))
start = pl.concat([
    pl.select(pl.repeat(0x85283473fffffff, n=NUM_VALS, dtype=pl.UInt64)), 
    pl.select(pl.repeat(0x8526c38bfffffff, n=NUM_VALS, dtype=pl.UInt64)), 
    pl.select(pl.repeat(0x8526e387fffffff, n=NUM_VALS, dtype=pl.UInt64))
]).to_series(0)

start_utf8 = pl.concat([
    pl.select(pl.repeat('85283473fffffff', n=NUM_VALS, dtype=pl.Utf8)), 
    pl.select(pl.repeat('8526c38bfffffff', n=NUM_VALS, dtype=pl.Utf8)), 
    pl.select(pl.repeat('8526e387fffffff', n=NUM_VALS, dtype=pl.Utf8))
]).to_series(0)

print('Begin first test')
start_time = time.time()
t1 = start_utf8.apply(parent_f)
end_time = time.time()
print(f"Duration of series apply: {end_time - start_time}")

start_time = time.time()
t2 = h3e.cell_to_parent(start, 5)
end_time = time.time()
print(f"Duration of cell_to_parent: {end_time - start_time}")

start_time = time.time()
t3 = start.h3.cell_to_parent(5)
end_time = time.time()
print(f"Test series extension of cell_to_parent: {end_time - start_time}")