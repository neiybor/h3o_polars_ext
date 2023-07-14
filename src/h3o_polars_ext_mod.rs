use h3o;
use polars::prelude::*;
use rayon::prelude::*;

pub(super) fn cell_to_parent(srs: Series, res: u8) -> PolarsResult<Series> {
    let resolution = h3o::Resolution::try_from(res).unwrap();
    let children: Vec<Option<u64>> = srs.u64().unwrap().into_iter().collect();
    let out: ChunkedArray<UInt64Type> = ChunkedArray::from_vec(srs.name(), 
        children
        .par_iter()
        .map(|c| h3o::CellIndex::try_from(c.unwrap()).unwrap())
        .map(|i| i.parent(resolution).unwrap())
        .map(|v| u64::from(v))
        .collect());
    Ok(out.into_series())
}