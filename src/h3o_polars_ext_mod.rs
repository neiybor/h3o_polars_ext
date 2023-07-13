use h3o;
use polars::prelude::*;

pub(super) fn cell_to_parent(srs: Series, res: u8) -> PolarsResult<Series> {
    let resolution = h3o::Resolution::try_from(res).unwrap();
    let chunked_array = srs.u64().unwrap();
    let out = chunked_array.apply(|c| u64::from(h3o::CellIndex::try_from(c).unwrap().parent(resolution).unwrap()));
    Ok(out.into_series())
}