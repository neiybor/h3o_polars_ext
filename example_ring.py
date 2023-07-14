from functools import partial
import h3o_polars_ext as h3e

# import h3
import h3.api.basic_int as h3
import polars as pl
import time

def list_ring(x, k=1):
    return list(h3.grid_ring(x, k))

ring_f = partial(list_ring, k=2)
NUM_VALS = 1_000_000

print('Get starting series')
start = pl.concat([
    pl.select(pl.repeat(0x85283473fffffff, n=NUM_VALS, dtype=pl.UInt64)), 
    pl.select(pl.repeat(0x8526c38bfffffff, n=NUM_VALS, dtype=pl.UInt64)), 
    pl.select(pl.repeat(0x8526e387fffffff, n=NUM_VALS, dtype=pl.UInt64))
]).to_series(0)

print('Begin first test')
start_time = time.time()
t1 = start.apply(ring_f)
end_time = time.time()
print(f"Duration of series apply: {end_time - start_time}")

start_time = time.time()
t2 = h3e.grid_ring(start, 2)
end_time = time.time()
print(f"Duration of grid_ring: {end_time - start_time}")

start_time = time.time()
t3 = start.h3.grid_ring(2)
end_time = time.time()
print(f"Test series extension of grid_ring: {end_time - start_time}")