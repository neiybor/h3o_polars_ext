use h3o;
use polars::prelude::*;
use rayon::prelude::*;

pub(super) fn cell_to_parent(srs: Series, res: u8) -> PolarsResult<Series> {
    let resolution = h3o::Resolution::try_from(res).unwrap();
    let children: Vec<Option<u64>> = srs.u64().unwrap().to_vec();
    let out: ChunkedArray<UInt64Type> = ChunkedArray::from_vec("parent", 
        children
        .par_iter()
        .map(|c| h3o::CellIndex::try_from(c.unwrap()).unwrap())
        .map(|i| i.parent(resolution).unwrap())
        .map(|v| u64::from(v))
        .collect());
    Ok(out.into_series())
}

pub(super) fn grid_ring(srs: Series, k: u32) -> PolarsResult<Series> {
    let center_cells: Vec<Option<u64>> = srs.u64().unwrap().to_vec();
    let v: Vec<UInt64Chunked> = center_cells
        .par_iter()
        .map(|c| UInt64Chunked::from_vec("ring_cells", 
            h3o::CellIndex::try_from(c.unwrap())
            .unwrap()
            .grid_ring_fast(k)
            .collect::<Option<Vec<_>>>()
            .unwrap()
            .par_iter()
            .map(|&i| u64::from(i)
        ).collect()))
        .collect();
    Ok(v.into_iter().map(|inner_chunk| inner_chunk.into_series()).collect::<ListChunked>().into_series())
}